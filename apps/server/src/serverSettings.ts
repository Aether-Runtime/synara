/**
 * ServerSettings - Server-authoritative settings persistence.
 *
 * Owns settings that affect server-side behavior. The web app can continue to
 * keep UI-only preferences in local storage while these values become durable
 * and process-authoritative on the server.
 */
import {
  DEFAULT_DROID_GIT_TEXT_GENERATION_MODEL,
  DEFAULT_GIT_TEXT_GENERATION_MODEL,
  DEFAULT_MODEL_BY_PROVIDER,
  DEFAULT_SERVER_SETTINGS,
  type ModelSelection,
  ServerSettings,
  ServerSettingsError,
  type ServerSettingsPatch,
  type ServerSettingsView,
} from "@synara/contracts";
import { deepMerge, type DeepPartial } from "@synara/shared/Struct";
import { applyServerSettingsPatch } from "@synara/shared/serverSettings";
import {
  Cause,
  Deferred,
  Effect,
  FileSystem,
  Layer,
  Path,
  PubSub,
  Ref,
  Schema,
  SchemaIssue,
  ServiceMap,
  Stream,
} from "effect";
import * as Semaphore from "effect/Semaphore";
import { writeFileStringAtomically } from "./atomicWrite";
import { ServerConfig } from "./config";
import {
  GIT_TEXT_GENERATION_PROVIDER_ORDER,
  hasDedicatedTextGenerationProvider,
} from "./git/textGenerationSelection";
import {
  PROVIDER_SECRET_PROVIDERS,
  PROVIDER_SECRETS,
  ProviderCredentials,
  ProviderCredentialsLive,
  type ProviderSecretProvider,
} from "./providerCredentials";

export interface ServerSettingsShape {
  readonly start: Effect.Effect<void, ServerSettingsError>;
  readonly ready: Effect.Effect<void, ServerSettingsError>;
  readonly getSettings: Effect.Effect<ServerSettings, ServerSettingsError>;
  readonly getSettingsView: Effect.Effect<ServerSettingsView, ServerSettingsError>;
  readonly getSnapshot: Effect.Effect<ServerSettingsSnapshot, ServerSettingsError>;
  readonly updateSettings: (
    patch: ServerSettingsPatch,
  ) => Effect.Effect<ServerSettings, ServerSettingsError>;
  readonly updateSettingsView: (
    patch: ServerSettingsPatch,
  ) => Effect.Effect<ServerSettingsView, ServerSettingsError>;
  readonly streamChanges: Stream.Stream<ServerSettings>;
  readonly streamViews: Stream.Stream<ServerSettingsView>;
}

export interface ServerSettingsSnapshot {
  readonly revision: number;
  readonly migrationVersion: number;
  readonly settings: ServerSettings;
}

const SERVER_SETTINGS_MIGRATION_VERSION = 3;
const PREVIOUS_GIT_TEXT_GENERATION_MODEL = "gpt-5.4-mini";
const PREVIOUS_LUNA_GIT_TEXT_GENERATION_MODEL = "gpt-5.6-luna";

function migrateSettings(settings: ServerSettings, migrationVersion: number): ServerSettings {
  const selection = settings.textGenerationModelSelection;
  if (
    selection.provider !== "codex" ||
    !(
      (migrationVersion < 2 && selection.model === PREVIOUS_GIT_TEXT_GENERATION_MODEL) ||
      (migrationVersion < 3 && selection.model === PREVIOUS_LUNA_GIT_TEXT_GENERATION_MODEL)
    )
  ) {
    return settings;
  }

  return {
    ...settings,
    textGenerationModelSelection: {
      ...selection,
      model: DEFAULT_GIT_TEXT_GENERATION_MODEL,
    },
  };
}

export function toServerSettingsView(settings: ServerSettings): ServerSettingsView {
  return settings;
}

export class ServerSettingsService extends ServiceMap.Service<
  ServerSettingsService,
  ServerSettingsShape
>()("synara/serverSettings/ServerSettingsService") {
  static readonly layerTest = (overrides: DeepPartial<ServerSettings> = {}) =>
    Layer.effect(
      ServerSettingsService,
      Effect.gen(function* () {
        const currentSettingsRef = yield* Ref.make<ServerSettings>(
          deepMerge(DEFAULT_SERVER_SETTINGS, overrides),
        );
        const changesPubSub = yield* PubSub.unbounded<ServerSettings>();
        const revisionRef = yield* Ref.make(0);
        const emitChange = (settings: ServerSettings) =>
          PubSub.publish(changesPubSub, settings).pipe(Effect.asVoid);
        const getSettings = Ref.get(currentSettingsRef).pipe(
          Effect.map(resolveTextGenerationProvider),
        );
        const updateSettings = (patch: ServerSettingsPatch) =>
          Ref.get(currentSettingsRef).pipe(
            Effect.flatMap((currentSettings) =>
              normalizeSettings("<memory>", currentSettings, patch),
            ),
            Effect.tap((nextSettings) => Ref.set(currentSettingsRef, nextSettings)),
            Effect.tap(() => Ref.update(revisionRef, (revision) => revision + 1)),
            Effect.tap(emitChange),
            Effect.map(resolveTextGenerationProvider),
          );

        return {
          start: Effect.void,
          ready: Effect.void,
          getSettings,
          getSettingsView: getSettings.pipe(Effect.map(toServerSettingsView)),
          getSnapshot: Effect.all({
            revision: Ref.get(revisionRef),
            settings: getSettings,
          }).pipe(
            Effect.map(({ revision, settings }) => ({
              revision,
              migrationVersion: SERVER_SETTINGS_MIGRATION_VERSION,
              settings,
            })),
          ),
          updateSettings,
          updateSettingsView: (patch) =>
            updateSettings(patch).pipe(Effect.map(toServerSettingsView)),
          get streamChanges() {
            return Stream.fromPubSub(changesPubSub).pipe(Stream.map(resolveTextGenerationProvider));
          },
          get streamViews() {
            return Stream.fromPubSub(changesPubSub).pipe(
              Stream.map(resolveTextGenerationProvider),
              Stream.map(toServerSettingsView),
            );
          },
        } satisfies ServerSettingsShape;
      }),
    );
}

function resolveTextGenerationProvider(settings: ServerSettings): ServerSettings {
  const selection = settings.textGenerationModelSelection;
  if (
    hasDedicatedTextGenerationProvider(selection.provider) &&
    settings.providers[selection.provider].enabled
  ) {
    return settings;
  }

  const fallback = GIT_TEXT_GENERATION_PROVIDER_ORDER.find(
    (provider) => settings.providers[provider].enabled,
  );
  if (!fallback) {
    return settings;
  }

  return {
    ...settings,
    textGenerationModelSelection: {
      provider: fallback,
      model:
        fallback === "droid"
          ? DEFAULT_DROID_GIT_TEXT_GENERATION_MODEL
          : DEFAULT_MODEL_BY_PROVIDER[fallback],
    } as ModelSelection,
  };
}

function normalizeSettings(
  settingsPath: string,
  current: ServerSettings,
  patch: ServerSettingsPatch,
): Effect.Effect<ServerSettings, ServerSettingsError> {
  return Schema.decodeUnknownEffect(ServerSettings)(applyServerSettingsPatch(current, patch)).pipe(
    Effect.mapError(
      (cause) =>
        new ServerSettingsError({
          settingsPath,
          detail: `failed to normalize server settings: ${SchemaIssue.makeFormatterDefault()(cause.issue)}`,
          cause,
        }),
    ),
  );
}

function readProviderSecretField(
  providers: unknown,
  provider: ProviderSecretProvider,
): string | undefined {
  if (providers === null || typeof providers !== "object") return undefined;
  const providerRecord = (providers as Record<string, unknown>)[provider];
  if (providerRecord === null || typeof providerRecord !== "object") return undefined;
  const value = (providerRecord as Record<string, unknown>)[PROVIDER_SECRETS[provider].patchKey];
  return typeof value === "string" ? value : undefined;
}

// Secrets written to settings.json by older builds (or by hand) move into the secret
// store; the rewritten file no longer carries them because ServerSettings omits the field.
function readLegacyProviderSecrets(raw: string): ReadonlyMap<ProviderSecretProvider, string> {
  try {
    const parsed = JSON.parse(raw) as { providers?: unknown };
    const secrets = new Map<ProviderSecretProvider, string>();
    for (const provider of PROVIDER_SECRET_PROVIDERS) {
      const value = readProviderSecretField(parsed.providers, provider)?.trim();
      if (value) {
        secrets.set(provider, value);
      }
    }
    return secrets;
  } catch {
    return new Map();
  }
}

function omitProviderSecrets(patch: ServerSettingsPatch): ServerSettingsPatch {
  if (!patch.providers) return patch;
  const providers: Record<string, unknown> = { ...patch.providers };
  for (const provider of PROVIDER_SECRET_PROVIDERS) {
    const providerPatch = patch.providers[provider];
    if (!providerPatch) continue;
    const { [PROVIDER_SECRETS[provider].patchKey]: _secret, ...rest } = providerPatch as Record<
      string,
      unknown
    >;
    providers[provider] = rest;
  }
  return { ...patch, providers: providers as NonNullable<ServerSettingsPatch["providers"]> };
}

// Migrate only portable Kilo state. Its model/options shape and enabled flag
// remain meaningful, but Kilo binary paths, endpoints, and credentials are not
// compatible with the OpenCode process protocol and must not be copied.
function migrateRemovedKiloSettings(settings: unknown): unknown {
  if (settings === null || typeof settings !== "object" || Array.isArray(settings)) {
    return settings;
  }
  const record = settings as Record<string, unknown>;
  let migrated = record;
  const selection = record.textGenerationModelSelection;
  if (selection !== null && typeof selection === "object" && !Array.isArray(selection)) {
    const selectionRecord = selection as Record<string, unknown>;
    if (selectionRecord.provider === "kilo") {
      const options = selectionRecord.options;
      const migratedOptions =
        options !== null &&
        typeof options === "object" &&
        !Array.isArray(options) &&
        "kilo" in options
          ? (options as Record<string, unknown>).kilo
          : options;
      migrated = {
        ...migrated,
        textGenerationModelSelection: {
          ...selectionRecord,
          provider: "opencode",
          ...(migratedOptions === undefined ? {} : { options: migratedOptions }),
        },
      };
    }
  }

  const providers = record.providers;
  if (providers !== null && typeof providers === "object" && !Array.isArray(providers)) {
    const providerRecord = providers as Record<string, unknown>;
    const kilo = providerRecord.kilo;
    if (kilo !== null && typeof kilo === "object" && !Array.isArray(kilo)) {
      const kiloRecord = kilo as Record<string, unknown>;
      const existingOpenCode =
        providerRecord.opencode !== null &&
        typeof providerRecord.opencode === "object" &&
        !Array.isArray(providerRecord.opencode)
          ? (providerRecord.opencode as Record<string, unknown>)
          : {};
      const portableCustomModels = [
        ...(Array.isArray(existingOpenCode.customModels) ? existingOpenCode.customModels : []),
        ...(Array.isArray(kiloRecord.customModels) ? kiloRecord.customModels : []),
      ].filter(
        (value, index, values) => typeof value === "string" && values.indexOf(value) === index,
      );
      const { kilo: _removedKilo, ...remainingProviders } = providerRecord;
      migrated = {
        ...migrated,
        providers: {
          ...remainingProviders,
          opencode: {
            ...existingOpenCode,
            ...(kiloRecord.enabled === true ? { enabled: true } : {}),
            ...(portableCustomModels.length > 0 ? { customModels: portableCustomModels } : {}),
          },
        },
      };
    }
  }
  return migrated;
}

function decodeSettingsFromJson(settingsPath: string, raw: string) {
  try {
    const parsed = JSON.parse(raw) as unknown;
    const envelope =
      parsed !== null && typeof parsed === "object" && "settings" in parsed
        ? (parsed as { revision?: unknown; migrationVersion?: unknown; settings: unknown })
        : null;
    const decoded = Schema.decodeUnknownExit(ServerSettings)(
      migrateRemovedKiloSettings(envelope?.settings ?? parsed),
    );
    if (decoded._tag === "Failure") {
      return { _tag: "Failure" as const, error: Cause.pretty(decoded.cause) };
    }
    return {
      _tag: "Success" as const,
      value: decoded.value,
      revision:
        envelope && Number.isSafeInteger(envelope.revision) && Number(envelope.revision) >= 0
          ? Number(envelope.revision)
          : 0,
      migrationVersion:
        envelope && Number.isSafeInteger(envelope.migrationVersion)
          ? Number(envelope.migrationVersion)
          : 0,
      legacyFormat: envelope === null,
    };
  } catch (cause) {
    const error = new ServerSettingsError({
      settingsPath,
      detail: "failed to parse settings JSON",
      cause,
    });
    return { _tag: "Failure" as const, error: error.message };
  }
}

const makeServerSettings = Effect.gen(function* () {
  const { settingsPath } = yield* ServerConfig;
  const providerCredentials = yield* ProviderCredentials;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const writeSemaphore = yield* Semaphore.make(1);
  const changesPubSub = yield* PubSub.unbounded<ServerSettings>();
  const settingsRef = yield* Ref.make<ServerSettings>(DEFAULT_SERVER_SETTINGS);
  const revisionRef = yield* Ref.make(0);
  const startedRef = yield* Ref.make(false);
  const startedDeferred = yield* Deferred.make<void, ServerSettingsError>();

  const emitChange = (settings: ServerSettings) =>
    PubSub.publish(changesPubSub, settings).pipe(Effect.asVoid);

  const withCredentialState = (settings: ServerSettings) =>
    Effect.forEach(PROVIDER_SECRET_PROVIDERS, (provider) =>
      providerCredentials
        .isSecretConfigured(provider)
        .pipe(Effect.map((configured) => [provider, configured] as const)),
    ).pipe(
      Effect.map((entries): ServerSettings => {
        const providers: Record<string, unknown> = { ...settings.providers };
        for (const [provider, configured] of entries) {
          providers[provider] = {
            ...settings.providers[provider],
            [PROVIDER_SECRETS[provider].configuredKey]: configured,
          };
        }
        return { ...settings, providers: providers as ServerSettings["providers"] };
      }),
      Effect.mapError(
        (cause) =>
          new ServerSettingsError({
            settingsPath,
            detail: "failed to read provider credential state",
            cause,
          }),
      ),
    );

  const loadSettingsFromDisk = Effect.gen(function* () {
    const exists = yield* fs.exists(settingsPath).pipe(
      Effect.mapError(
        (cause) =>
          new ServerSettingsError({
            settingsPath,
            detail: "failed to check settings file existence",
            cause,
          }),
      ),
    );
    if (!exists) {
      return {
        settings: yield* withCredentialState(DEFAULT_SERVER_SETTINGS),
        revision: 0,
        migrated: false,
      };
    }

    const raw = yield* fs.readFileString(settingsPath).pipe(
      Effect.mapError(
        (cause) =>
          new ServerSettingsError({
            settingsPath,
            detail: "failed to read settings file",
            cause,
          }),
      ),
    );
    const decoded = decodeSettingsFromJson(settingsPath, raw);
    if (decoded._tag === "Failure") {
      const quarantinePath = `${settingsPath}.invalid-${Date.now()}`;
      yield* fs.rename(settingsPath, quarantinePath).pipe(Effect.catch(() => Effect.void));
      yield* Effect.logWarning("quarantined invalid settings.json, using defaults", {
        path: settingsPath,
        quarantinePath,
        error: decoded.error,
      });
      return {
        settings: yield* withCredentialState(DEFAULT_SERVER_SETTINGS),
        revision: 0,
        migrated: false,
      };
    }
    const legacySecrets = readLegacyProviderSecrets(raw);
    yield* Effect.forEach(
      legacySecrets,
      ([provider, secret]) => providerCredentials.replaceSecret(provider, secret),
      { discard: true },
    ).pipe(
      Effect.mapError(
        (cause) =>
          new ServerSettingsError({
            settingsPath,
            detail: "failed to migrate provider credentials",
            cause,
          }),
      ),
    );
    return {
      settings: yield* withCredentialState(
        migrateSettings(decoded.value, decoded.migrationVersion),
      ),
      revision: decoded.revision,
      migrated:
        legacySecrets.size > 0 ||
        decoded.legacyFormat ||
        decoded.migrationVersion !== SERVER_SETTINGS_MIGRATION_VERSION,
    };
  });

  const writeSettingsAtomically = (snapshot: ServerSettingsSnapshot) => {
    return writeFileStringAtomically({
      filePath: settingsPath,
      contents: `${JSON.stringify(snapshot, null, 2)}\n`,
    }).pipe(
      Effect.mapError(
        (cause) =>
          new ServerSettingsError({
            settingsPath,
            detail: "failed to write settings file",
            cause,
          }),
      ),
    );
  };

  const start = Effect.gen(function* () {
    const shouldStart = yield* Ref.modify(startedRef, (started) => [!started, true]);
    if (!shouldStart) {
      return yield* Deferred.await(startedDeferred);
    }

    const startup = Effect.gen(function* () {
      yield* fs.makeDirectory(path.dirname(settingsPath), { recursive: true }).pipe(
        Effect.mapError(
          (cause) =>
            new ServerSettingsError({
              settingsPath,
              detail: "failed to prepare settings directory",
              cause,
            }),
        ),
      );
      const loaded = yield* loadSettingsFromDisk;
      if (loaded.migrated) {
        loaded.revision += 1;
        yield* writeSettingsAtomically({
          revision: loaded.revision,
          migrationVersion: SERVER_SETTINGS_MIGRATION_VERSION,
          settings: loaded.settings,
        });
      }
      yield* Ref.set(settingsRef, loaded.settings);
      yield* Ref.set(revisionRef, loaded.revision);
    });

    const startupExit = yield* Effect.exit(startup);
    if (startupExit._tag === "Failure") {
      yield* Deferred.failCause(startedDeferred, startupExit.cause).pipe(Effect.orDie);
      return yield* Effect.failCause(startupExit.cause);
    }

    yield* Deferred.succeed(startedDeferred, undefined).pipe(Effect.orDie);
  });

  const getSettings = Ref.get(settingsRef).pipe(Effect.map(resolveTextGenerationProvider));
  const updateSettings = (patch: ServerSettingsPatch) =>
    writeSemaphore.withPermits(1)(
      Effect.gen(function* () {
        const disk = yield* loadSettingsFromDisk;
        const current = disk.settings;
        for (const provider of PROVIDER_SECRET_PROVIDERS) {
          const secret = readProviderSecretField(patch.providers, provider);
          if (secret !== undefined) {
            yield* providerCredentials.replaceSecret(provider, secret).pipe(
              Effect.mapError(
                (cause) =>
                  new ServerSettingsError({
                    settingsPath,
                    detail: `failed to update ${provider} ${PROVIDER_SECRETS[provider].label}`,
                    cause,
                  }),
              ),
            );
          }
        }
        const normalized = yield* normalizeSettings(
          settingsPath,
          current,
          omitProviderSecrets(patch),
        );
        const next = yield* withCredentialState(normalized);
        const nextRevision = Math.max(disk.revision, yield* Ref.get(revisionRef)) + 1;
        yield* writeSettingsAtomically({
          revision: nextRevision,
          migrationVersion: SERVER_SETTINGS_MIGRATION_VERSION,
          settings: next,
        });
        yield* Ref.set(settingsRef, next);
        yield* Ref.set(revisionRef, nextRevision);
        yield* emitChange(next);
        return resolveTextGenerationProvider(next);
      }),
    );

  return {
    start,
    ready: Deferred.await(startedDeferred),
    getSettings,
    getSettingsView: getSettings.pipe(Effect.map(toServerSettingsView)),
    getSnapshot: Effect.all({ revision: Ref.get(revisionRef), settings: getSettings }).pipe(
      Effect.map(({ revision, settings }) => ({
        revision,
        migrationVersion: SERVER_SETTINGS_MIGRATION_VERSION,
        settings,
      })),
    ),
    updateSettings,
    updateSettingsView: (patch) => updateSettings(patch).pipe(Effect.map(toServerSettingsView)),
    get streamChanges() {
      return Stream.fromPubSub(changesPubSub).pipe(Stream.map(resolveTextGenerationProvider));
    },
    get streamViews() {
      return Stream.fromPubSub(changesPubSub).pipe(
        Stream.map(resolveTextGenerationProvider),
        Stream.map(toServerSettingsView),
      );
    },
  } satisfies ServerSettingsShape;
});

export const ServerSettingsLive = Layer.effect(ServerSettingsService, makeServerSettings).pipe(
  Layer.provide(ProviderCredentialsLive),
);

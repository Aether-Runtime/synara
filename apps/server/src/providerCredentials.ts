// FILE: providerCredentials.ts
// Purpose: Owns server-only credentials used to connect to external provider servers and APIs.
// Layer: Server provider security boundary

import type { ProviderKind, ServerSettings, ServerSettingsPatch } from "@synara/contracts";
import { Effect, Layer, ServiceMap } from "effect";

import { ServerSecretStoreLive } from "./auth/Layers/ServerSecretStore";
import { ServerSecretStore, type SecretStoreError } from "./auth/Services/ServerSecretStore";

/**
 * Provider secrets kept in the server secret store. Each one is written through a
 * patch-only settings field (`patchKey`) and reaches clients only as a boolean
 * (`configuredKey`); the value itself never leaves the server.
 */
export const PROVIDER_SECRETS = {
  opencode: {
    secretName: "provider-opencode-server-password",
    patchKey: "serverPassword",
    configuredKey: "serverPasswordConfigured",
    label: "server password",
  },
  aether: {
    secretName: "provider-aether-api-key",
    patchKey: "apiKey",
    configuredKey: "apiKeyConfigured",
    label: "API key",
  },
} as const satisfies { readonly [P in ProviderKind]?: ProviderSecretField<P> };

// Pins each entry's field names to the settings contract so a rename cannot drift.
interface ProviderSecretField<P extends ProviderKind> {
  readonly secretName: string;
  readonly patchKey: keyof NonNullable<NonNullable<ServerSettingsPatch["providers"]>[P]>;
  readonly configuredKey: keyof ServerSettings["providers"][P];
  readonly label: string;
}

export type ProviderSecretProvider = keyof typeof PROVIDER_SECRETS;

export const PROVIDER_SECRET_PROVIDERS = Object.keys(
  PROVIDER_SECRETS,
) as ReadonlyArray<ProviderSecretProvider>;

export interface ProviderCredentialsShape {
  readonly getSecret: (
    provider: ProviderSecretProvider,
  ) => Effect.Effect<string | null, SecretStoreError>;
  readonly replaceSecret: (
    provider: ProviderSecretProvider,
    value: string | null,
  ) => Effect.Effect<void, SecretStoreError>;
  readonly isSecretConfigured: (
    provider: ProviderSecretProvider,
  ) => Effect.Effect<boolean, SecretStoreError>;
}

export class ProviderCredentials extends ServiceMap.Service<
  ProviderCredentials,
  ProviderCredentialsShape
>()("synara/providerCredentials/ProviderCredentials") {}

export const resolveProviderSecret = (provider: ProviderSecretProvider) =>
  Effect.gen(function* () {
    const credentials = yield* ProviderCredentials;
    return (yield* credentials.getSecret(provider)) ?? undefined;
  }).pipe(Effect.orDie);

export const makeProviderSecretResolver =
  (credentials: ProviderCredentialsShape) =>
  (provider: ProviderSecretProvider): Effect.Effect<string | undefined> =>
    credentials.getSecret(provider).pipe(
      Effect.map((value) => value ?? undefined),
      Effect.orDie,
    );

const makeProviderCredentials = Effect.gen(function* () {
  const secrets = yield* ServerSecretStore;
  const encoder = new TextEncoder();
  const decoder = new TextDecoder("utf-8", { fatal: true });

  const getSecret: ProviderCredentialsShape["getSecret"] = (provider) =>
    secrets.get(PROVIDER_SECRETS[provider].secretName).pipe(
      Effect.map((value) => {
        if (!value || value.byteLength === 0) return null;
        const decoded = decoder.decode(value);
        return decoded.length > 0 ? decoded : null;
      }),
    );

  const replaceSecret: ProviderCredentialsShape["replaceSecret"] = (provider, value) => {
    const normalized = value?.trim() ?? "";
    const { secretName } = PROVIDER_SECRETS[provider];
    return normalized.length > 0
      ? secrets.set(secretName, encoder.encode(normalized))
      : secrets.remove(secretName);
  };

  const isSecretConfigured: ProviderCredentialsShape["isSecretConfigured"] = (provider) =>
    getSecret(provider).pipe(Effect.map((value) => value !== null));

  return {
    getSecret,
    replaceSecret,
    isSecretConfigured,
  } satisfies ProviderCredentialsShape;
});

export const ProviderCredentialsLive = Layer.effect(
  ProviderCredentials,
  makeProviderCredentials,
).pipe(Layer.provide(ServerSecretStoreLive));

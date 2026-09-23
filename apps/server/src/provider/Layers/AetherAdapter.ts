/**
 * AetherAdapterLive - Skeleton adapter for the Aether cloud provider.
 *
 * Aether is registered end to end (settings, health, model picker) before its
 * sessions exist; see plans/aether-cloud-provider.md, steps P2–P6. Until then every
 * operation a user action reaches fails with a typed request error, while the
 * lifecycle hooks Synara calls on its own (shutdown, idle reaper, session lookup,
 * the runtime event pump) stay safe: this adapter owns no sessions.
 *
 * @module AetherAdapterLive
 */
import { Effect, Layer, Stream } from "effect";

import { ProviderAdapterRequestError } from "../Errors.ts";
import { AetherAdapter, type AetherAdapterShape } from "../Services/AetherAdapter.ts";

const PROVIDER = "aether" as const;

export const AETHER_NOT_IMPLEMENTED_DETAIL = "The Aether provider is not implemented yet";

const notImplemented = (method: string) =>
  Effect.fail(
    new ProviderAdapterRequestError({
      provider: PROVIDER,
      method,
      detail: AETHER_NOT_IMPLEMENTED_DETAIL,
    }),
  );

export function makeAetherAdapter(): AetherAdapterShape {
  return {
    provider: PROVIDER,
    capabilities: {
      sessionModelSwitch: "unsupported",
      conversationRollback: "restart-session",
      supportsSkillMentions: false,
      supportsSkillDiscovery: false,
      supportsNativeSlashCommandDiscovery: false,
      supportsPluginMentions: false,
      supportsPluginDiscovery: false,
      supportsRuntimeModelList: false,
      supportsTurnSteering: false,
      supportsLiveTurnDiffPatch: false,
    },
    startSession: () => notImplemented("startSession"),
    sendTurn: () => notImplemented("sendTurn"),
    interruptTurn: () => notImplemented("interruptTurn"),
    respondToRequest: () => notImplemented("respondToRequest"),
    respondToUserInput: () => notImplemented("respondToUserInput"),
    readThread: () => notImplemented("readThread"),
    rollbackThread: () => notImplemented("rollbackThread"),
    stopSession: () => Effect.void,
    stopAll: () => Effect.void,
    listSessions: () => Effect.succeed([]),
    hasSession: () => Effect.succeed(false),
    // No events, and no completion either: the runtime event pump treats a
    // finished adapter stream as a failure and restarts it with backoff.
    streamEvents: Stream.never,
  };
}

export const AetherAdapterLive = Layer.succeed(AetherAdapter, makeAetherAdapter());

import { ApprovalRequestId, ThreadId, TurnId } from "@synara/contracts";
import { Effect, Exit, Fiber, Stream } from "effect";
import { describe, expect, it } from "vitest";

import { ProviderAdapterRequestError } from "../Errors.ts";
import {
  assertProviderAdapterConformance,
  providerAdapterConformanceIssues,
} from "../providerAdapterConformance.ts";
import { runProviderRuntimeEventPump } from "../providerRuntimeEventPump.ts";
import type { ProviderRuntimeEventPumpHealth } from "../Services/ProviderService.ts";
import { AETHER_NOT_IMPLEMENTED_DETAIL, makeAetherAdapter } from "./AetherAdapter.ts";

const threadId = ThreadId.makeUnsafe("aether-thread");
const requestId = ApprovalRequestId.makeUnsafe("aether-request");

describe("AetherAdapter skeleton", () => {
  const adapter = makeAetherAdapter();

  it("passes provider adapter conformance and declares the cloud limits", () => {
    expect(providerAdapterConformanceIssues(adapter)).toEqual([]);
    expect(() => assertProviderAdapterConformance(adapter)).not.toThrow();
    expect(adapter.capabilities).toEqual({
      sessionModelSwitch: "unsupported",
      conversationRollback: "unsupported",
      reachesAgentGateway: false,
      managesWorktreeBranch: true,
      supportsSkillMentions: false,
      supportsSkillDiscovery: false,
      supportsNativeSlashCommandDiscovery: false,
      supportsPluginMentions: false,
      supportsPluginDiscovery: false,
      supportsRuntimeModelList: false,
      supportsTurnSteering: false,
      supportsLiveTurnDiffPatch: false,
    });
  });

  it.each([
    [
      "startSession",
      () => adapter.startSession({ threadId, provider: "aether", runtimeMode: "full-access" }),
    ],
    ["sendTurn", () => adapter.sendTurn({ threadId, input: "hello" })],
    ["interruptTurn", () => adapter.interruptTurn(threadId, TurnId.makeUnsafe("turn-1"))],
    ["respondToRequest", () => adapter.respondToRequest(threadId, requestId, "accept")],
    ["respondToUserInput", () => adapter.respondToUserInput(threadId, requestId, {})],
    ["readThread", () => adapter.readThread(threadId)],
    ["rollbackThread", () => adapter.rollbackThread(threadId, 1)],
  ] as const)("fails %s with a typed not-implemented error", async (method, call) => {
    const exit = await Effect.runPromiseExit(call() as Effect.Effect<unknown, unknown>);

    expect(exit).toEqual(
      Exit.fail(
        new ProviderAdapterRequestError({
          provider: "aether",
          method,
          detail: AETHER_NOT_IMPLEMENTED_DETAIL,
        }),
      ),
    );
    expect(Exit.isFailure(exit) && String(exit.cause)).toContain(
      "The Aether provider is not implemented yet",
    );
  });

  it("keeps the lifecycle paths Synara calls on its own safe", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        // Idempotent: stopping an unknown session twice, then everything, succeeds.
        yield* adapter.stopSession(threadId);
        yield* adapter.stopSession(threadId);
        yield* adapter.stopAll();
        expect(yield* adapter.listSessions()).toEqual([]);
        expect(yield* adapter.hasSession(threadId)).toBe(false);
      }),
    );
  });

  it("streams no events and never ends, so the runtime event pump stays healthy", async () => {
    const healthUpdates: ProviderRuntimeEventPumpHealth[] = [];
    const processed: unknown[] = [];

    await Effect.runPromise(
      Effect.gen(function* () {
        const pump = yield* runProviderRuntimeEventPump({
          provider: "aether",
          stream: adapter.streamEvents,
          processEvent: (event) => Effect.sync(() => processed.push(event)),
          updateHealth: (health) => healthUpdates.push(health),
          retryBaseDelayMs: 1,
          retryMaxDelayMs: 1,
        }).pipe(Effect.forkChild);
        yield* Effect.sleep("50 millis");
        yield* Fiber.interrupt(pump);

        const firstEvent = yield* adapter.streamEvents.pipe(
          Stream.take(1),
          Stream.runCollect,
          Effect.timeoutOption("20 millis"),
        );
        expect(firstEvent._tag).toBe("None");
      }),
    );

    expect(processed).toEqual([]);
    expect(healthUpdates.map((health) => health.status)).toEqual(["healthy"]);
  });
});

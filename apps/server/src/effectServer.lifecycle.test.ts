import { Effect, Scope } from "effect";
import { describe, expect, it } from "vitest";

import { closeServerRuntimePipeline, reconcileProviderTurnsAtStartup } from "./effectServer.ts";

describe("server runtime pipeline shutdown", () => {
  it("persists accepted provider terminal work before the engine stops", async () => {
    const order: string[] = [];
    let terminalAccepted = false;
    let terminalPersisted = false;
    let attachmentsDrained = false;
    const subscriptionsScope = await Effect.runPromise(Scope.make("sequential"));
    await Effect.runPromise(
      Scope.addFinalizer(
        subscriptionsScope,
        Effect.sync(() => {
          expect(terminalAccepted).toBe(true);
          terminalPersisted = true;
          order.push("reactors-drained-and-persisted");
        }),
      ),
    );

    await Effect.runPromise(
      closeServerRuntimePipeline({
        orchestrationEngine: {
          quiesce: Effect.sync(() => order.push("engine-quiesced")),
          drain: Effect.sync(() => order.push("admitted-commands-drained")),
          stop: Effect.sync(() => {
            expect(terminalPersisted).toBe(true);
            expect(attachmentsDrained).toBe(true);
            order.push("engine-stopped");
          }),
        },
        providerService: {
          closeRuntimeEvents: Effect.sync(() => {
            terminalAccepted = true;
            order.push("provider-terminal-events-fenced");
          }),
        },
        managedAttachmentCleanup: {
          drain: Effect.sync(() => {
            expect(terminalPersisted).toBe(true);
            attachmentsDrained = true;
            order.push("managed-attachments-drained");
          }),
        },
        subscriptionsScope,
      }),
    );

    expect(order).toEqual([
      "engine-quiesced",
      "admitted-commands-drained",
      "provider-terminal-events-fenced",
      "reactors-drained-and-persisted",
      "managed-attachments-drained",
      "engine-stopped",
    ]);
  });
});

describe("server startup reconciliation", () => {
  it("starts the live runtime reconciler only after restart reconciliation", async () => {
    const order: string[] = [];
    await Effect.runPromise(
      reconcileProviderTurnsAtStartup({
        reconcileRestartStuckTurns: Effect.sync(() => void order.push("restart-stuck-turns")),
        reconcileSettledOpenTurns: Effect.sync(() => void order.push("settled-open-turns")),
        startRuntimeReconciler: Effect.sync(() => void order.push("runtime-reconciler")),
      }),
    );
    // Its first pass must see the turns restart reconciliation reattached.
    expect(order).toEqual(["restart-stuck-turns", "settled-open-turns", "runtime-reconciler"]);
  });
});

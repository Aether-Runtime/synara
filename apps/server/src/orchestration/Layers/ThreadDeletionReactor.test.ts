import { execFileSync } from "node:child_process";
import { existsSync } from "node:fs";
import * as fs from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { CommandId, EventId, ThreadId, type OrchestrationEvent } from "@synara/contracts";
import { Cause, Effect, Exit, Layer, ManagedRuntime, Option, PubSub, Scope, Stream } from "effect";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  cleanupSucceededUnlessInterrupted,
  closeThreadTerminalScopes,
  detachThreadDevice,
  isThreadCurrentlyArchived,
  isThreadLifecycleCleanupEvent,
  ThreadDeletionReactorLive,
} from "./ThreadDeletionReactor";
import { ServerConfig, type ServerConfigShape } from "../../config";
import {
  DeletedThreadWorktreeRemovalFailures,
  DeletedThreadWorktreeRemovalFailuresLive,
} from "../../deletedThreadWorktreeRemovalFailures";
import { DeviceService, type DeviceServiceShape } from "../../device/Services/DeviceService";
import { DeviceManager } from "../../device/DeviceManager";
import { FakeDeviceBackend } from "../../device/FakeDeviceBackend";
import { GitCoreLive } from "../../git/Layers/GitCore";
import { GitCore, type WorktreeOwner } from "../../git/Services/GitCore";
import {
  GitStatusBroadcaster,
  type GitStatusBroadcasterShape,
} from "../../git/Services/GitStatusBroadcaster";
import { ProfileStatsArchive, type ProfileStatsArchiveShape } from "../../profileStatsArchive";
import {
  ProviderService,
  type ProviderServiceShape,
} from "../../provider/Services/ProviderService";
import {
  TerminalError,
  TerminalManager,
  type TerminalManagerShape,
} from "../../terminal/Services/Manager";
import {
  OrchestrationEngineService,
  type OrchestrationEngineShape,
} from "../Services/OrchestrationEngine";
import {
  ProjectionSnapshotQuery,
  type ProjectionSnapshotQueryShape,
} from "../Services/ProjectionSnapshotQuery";
import { ThreadDeletionReactor } from "../Services/ThreadDeletionReactor";

describe("terminal scope cleanup", () => {
  it("closes dock terminals even when the host terminal close fails", async () => {
    const close = vi.fn(() => Effect.void as Effect.Effect<void, TerminalError>);
    close.mockReturnValueOnce(Effect.fail(new TerminalError({ message: "host close failed" })));
    const result = await Effect.runPromise(
      closeThreadTerminalScopes(
        { close, closeSessionsOpenedAtOrBefore: () => Effect.void },
        ThreadId.makeUnsafe("host"),
        true,
      ),
    );
    expect(result).toBe(false);
    expect(close.mock.calls).toEqual([
      [{ threadId: "host", deleteHistory: true }],
      [{ threadId: "dock-terminal:host", deleteHistory: true }],
    ]);
  });

  it("preserves the archive generation fence for both terminal scopes", async () => {
    const close = vi.fn(() => Effect.void);
    const closeSessionsOpenedAtOrBefore = vi.fn(() => Effect.void);
    const archivedAt = "2026-09-09T12:00:00.000Z";
    const result = await Effect.runPromise(
      closeThreadTerminalScopes(
        { close, closeSessionsOpenedAtOrBefore },
        ThreadId.makeUnsafe("host"),
        false,
        archivedAt,
      ),
    );
    expect(result).toBe(true);
    expect(close).not.toHaveBeenCalled();
    expect(closeSessionsOpenedAtOrBefore.mock.calls).toEqual([
      [{ threadId: "host", openedAtOrBefore: archivedAt }],
      [{ threadId: "dock-terminal:host", openedAtOrBefore: archivedAt }],
    ]);
  });
});

function lifecycleEvent(type: "thread.archived" | "thread.deleted"): OrchestrationEvent {
  const threadId = ThreadId.makeUnsafe(`thread-${type}`);
  const now = "2026-07-23T20:00:00.000Z";
  return {
    sequence: 1,
    eventId: EventId.makeUnsafe(`event-${type}`),
    aggregateKind: "thread",
    aggregateId: threadId,
    type,
    occurredAt: now,
    payload:
      type === "thread.deleted"
        ? { threadId, deletedAt: now }
        : { threadId, archivedAt: now, updatedAt: now },
  } as OrchestrationEvent;
}

describe("isThreadLifecycleCleanupEvent", () => {
  it("routes both archive and delete through server-owned cleanup", () => {
    expect(isThreadLifecycleCleanupEvent(lifecycleEvent("thread.archived"))).toBe(true);
    expect(isThreadLifecycleCleanupEvent(lifecycleEvent("thread.deleted"))).toBe(true);
  });
});

describe("isThreadCurrentlyArchived", () => {
  it("rejects stale archive cleanup after an undo has cleared archivedAt", () => {
    expect(isThreadCurrentlyArchived({ archivedAt: null })).toBe(false);
    expect(isThreadCurrentlyArchived(undefined)).toBe(false);
    expect(isThreadCurrentlyArchived({ archivedAt: "2026-07-23T20:00:00.000Z" })).toBe(true);
  });
});

describe("cleanupSucceededUnlessInterrupted", () => {
  const threadId = ThreadId.makeUnsafe("thread-deletion-reactor-test");

  it("returns true for successful cleanup", async () => {
    const result = await Effect.runPromise(
      cleanupSucceededUnlessInterrupted({
        effect: Effect.void,
        message: "thread deletion cleanup skipped provider session stop",
        threadId,
      }),
    );

    expect(result).toBe(true);
  });

  it("returns false for ordinary cleanup failures", async () => {
    const result = await Effect.runPromise(
      cleanupSucceededUnlessInterrupted({
        effect: Effect.fail("cleanup failed"),
        message: "thread deletion cleanup skipped provider session stop",
        threadId,
      }),
    );

    expect(result).toBe(false);
  });

  it("preserves interrupt causes", async () => {
    const exit = await Effect.runPromiseExit(
      cleanupSucceededUnlessInterrupted({
        effect: Effect.interrupt,
        message: "thread deletion cleanup skipped provider session stop",
        threadId,
      }),
    );

    expect(Exit.isFailure(exit)).toBe(true);
    if (Exit.isFailure(exit)) {
      expect(Cause.hasInterruptsOnly(exit.cause)).toBe(true);
    }
  });
});

describe("detachThreadDevice", () => {
  it("releases the device attachment when an active thread is deleted", async () => {
    const threadId = ThreadId.makeUnsafe("thread-delete-device");
    const backend = new FakeDeviceBackend();
    const manager = new DeviceManager({ backend });
    await backend.boot("FAKE-0001");
    await manager.attach(threadId, "FAKE-0001");

    await Effect.runPromise(
      detachThreadDevice(threadId).pipe(
        Effect.provideService(DeviceService, { supported: true, manager }),
      ),
    );

    expect((await manager.getThreadState(threadId)).attachedDeviceUdid).toBeNull();
    expect(backend.hasStream("FAKE-0001")).toBe(false);
  });
});

describe("a deleted thread's worktree", () => {
  const THREAD_ID = ThreadId.makeUnsafe("thread-delete-worktree");
  const DELETED_AT = "2026-09-26T12:00:00.000Z";
  const aetherOwner: WorktreeOwner = { threadId: THREAD_ID, provider: "aether" };
  const cleanups: Array<() => Promise<void>> = [];

  afterEach(async () => {
    for (const cleanup of cleanups.splice(0).toReversed()) await cleanup();
  });

  const git = (cwd: string, args: ReadonlyArray<string>) =>
    execFileSync("git", args, { cwd, stdio: "pipe" }).toString().trim();

  /**
   * A repository with one linked worktree under the managed worktrees directory, dirty with an
   * untracked file, and the deletion reactor running against it. Only git is real.
   */
  const makeHarness = async (input: {
    readonly claimedBy?: WorktreeOwner;
    /** Whether stopping the thread's session releases its claim, as Aether's release does. */
    readonly stopReleasesClaim: boolean;
  }) => {
    const base = await fs.realpath(await fs.mkdtemp(path.join(tmpdir(), "synara-thread-delete-")));
    cleanups.push(() => fs.rm(base, { recursive: true, force: true }));
    const repo = path.join(base, "repo");
    const worktreesDir = path.join(base, "worktrees");
    const worktreePath = path.join(worktreesDir, "acme", "synara");
    await fs.mkdir(repo);
    git(repo, ["init", "--initial-branch=main"]);
    git(repo, ["config", "user.email", "test@test.com"]);
    git(repo, ["config", "user.name", "Test"]);
    await fs.writeFile(path.join(repo, "README.md"), "# test\n");
    git(repo, ["add", "."]);
    git(repo, ["commit", "-m", "initial commit"]);
    git(repo, ["worktree", "add", "-b", "feature/thread", worktreePath]);
    await fs.writeFile(path.join(worktreePath, "mirrored.txt"), "cloud\n");

    const domainEvents = Effect.runSync(PubSub.unbounded<OrchestrationEvent>());
    const stoppedSessions: ThreadId[] = [];
    const refreshedStatuses: string[] = [];
    let releaseClaim: Effect.Effect<void> = Effect.void;
    const layer = ThreadDeletionReactorLive.pipe(
      Layer.provideMerge(DeletedThreadWorktreeRemovalFailuresLive),
      Layer.provideMerge(
        GitCoreLive.pipe(
          Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "synara-delete-git-" })),
          Layer.provide(NodeServices.layer),
        ),
      ),
      Layer.provide(
        Layer.mergeAll(
          Layer.succeed(ServerConfig, {
            homeDir: base,
            worktreesDir,
          } as unknown as ServerConfigShape),
          Layer.succeed(OrchestrationEngineService, {
            streamDomainEvents: Stream.fromPubSub(domainEvents),
            refreshCommandReadModel: () => Effect.void,
          } as unknown as OrchestrationEngineShape),
          Layer.succeed(ProfileStatsArchive, {
            hasThreadPurgeFence: () => Effect.succeed(false),
            purgeThreadWithStatsSnapshot: () => Effect.succeed(true),
            purgeSoftDeletedManualThreads: () => Effect.succeed(0),
          } as unknown as ProfileStatsArchiveShape),
          Layer.succeed(ProviderService, {
            stopSession: ({ threadId }: { threadId: ThreadId }) =>
              Effect.sync(() => stoppedSessions.push(threadId)).pipe(
                Effect.andThen(input.stopReleasesClaim ? releaseClaim : Effect.void),
              ),
          } as unknown as ProviderServiceShape),
          Layer.succeed(GitStatusBroadcaster, {
            refreshStatus: (cwd: string) =>
              Effect.sync(() => refreshedStatuses.push(cwd)).pipe(Effect.as({})),
          } as unknown as GitStatusBroadcasterShape),
          Layer.succeed(TerminalManager, {
            close: () => Effect.void,
            closeSessionsOpenedAtOrBefore: () => Effect.void,
          } as unknown as TerminalManagerShape),
          Layer.succeed(ProjectionSnapshotQuery, {
            listManagedWorktreeThreads: () =>
              Effect.succeed([
                {
                  id: THREAD_ID,
                  archivedAt: null,
                  deletedAt: DELETED_AT,
                  worktreePath,
                  associatedWorktreePath: worktreePath,
                },
              ]),
          } as unknown as ProjectionSnapshotQueryShape),
          Layer.succeed(DeviceService, {
            supported: false,
            manager: { handleThreadRemoved: async () => undefined },
          } as unknown as DeviceServiceShape),
        ),
      ),
    );
    const runtime = ManagedRuntime.make(layer);
    cleanups.push(() => runtime.dispose());

    if (input.claimedBy) {
      const claimScope = await Effect.runPromise(Scope.make());
      const gitCore = await runtime.runPromise(Effect.service(GitCore));
      await runtime.runPromise(
        Scope.provide(gitCore.claimWorktree(worktreePath, input.claimedBy), claimScope),
      );
      releaseClaim = Scope.close(claimScope, Exit.void);
      cleanups.push(() => Effect.runPromise(Scope.close(claimScope, Exit.void)));
    }

    const reactor = await runtime.runPromise(Effect.service(ThreadDeletionReactor));
    const reactorScope = await Effect.runPromise(Scope.make("sequential"));
    cleanups.push(() => Effect.runPromise(Scope.close(reactorScope, Exit.void)));
    await Effect.runPromise(reactor.start().pipe(Scope.provide(reactorScope)));
    await new Promise((resolve) => setTimeout(resolve, 0));

    const deleteThread = async (removeWorktree: boolean) => {
      await Effect.runPromise(
        PubSub.publish(domainEvents, {
          sequence: 1,
          eventId: EventId.makeUnsafe("event-thread-deleted"),
          aggregateKind: "thread",
          aggregateId: THREAD_ID,
          type: "thread.deleted",
          occurredAt: DELETED_AT,
          commandId: CommandId.makeUnsafe("cmd-thread-delete"),
          causationEventId: null,
          correlationId: null,
          metadata: {},
          payload: {
            threadId: THREAD_ID,
            deletedAt: DELETED_AT,
            ...(removeWorktree
              ? { removeWorktree: { workspaceRoot: repo, path: worktreePath } }
              : {}),
          },
        }),
      );
      await runtime.runPromise(reactor.drain);
    };
    // A new subscriber, like a client that reconnects after the failure, receives it first.
    const firstReportedFailure = () =>
      runtime.runPromise(
        Effect.service(DeletedThreadWorktreeRemovalFailures).pipe(
          Effect.flatMap((failures) => Stream.runHead(failures.stream)),
        ),
      );

    return {
      repo,
      worktreePath,
      stoppedSessions,
      refreshedStatuses,
      deleteThread,
      firstReportedFailure,
    };
  };

  it("removes the worktree the user chose to delete, dirty and claimed, once the stop releases it", async () => {
    const harness = await makeHarness({ claimedBy: aetherOwner, stopReleasesClaim: true });

    await harness.deleteThread(true);

    expect(harness.stoppedSessions).toEqual([THREAD_ID]);
    expect(existsSync(harness.worktreePath)).toBe(false);
    expect(harness.refreshedStatuses).toEqual([harness.repo]);
  });

  it("keeps a dirty, claimed worktree the user did not choose to delete", async () => {
    const harness = await makeHarness({ claimedBy: aetherOwner, stopReleasesClaim: true });

    await harness.deleteThread(false);

    expect(harness.stoppedSessions).toEqual([THREAD_ID]);
    expect(existsSync(path.join(harness.worktreePath, "mirrored.txt"))).toBe(true);
  });

  it("keeps an unclaimed local thread's dirty worktree the user did not choose to delete", async () => {
    const harness = await makeHarness({ stopReleasesClaim: false });

    await harness.deleteThread(false);

    expect(harness.stoppedSessions).toEqual([THREAD_ID]);
    expect(existsSync(path.join(harness.worktreePath, "mirrored.txt"))).toBe(true);
  });

  it("reports a failed removal to connected clients and keeps the worktree", async () => {
    const harness = await makeHarness({ claimedBy: aetherOwner, stopReleasesClaim: false });

    await harness.deleteThread(true);

    expect(existsSync(path.join(harness.worktreePath, "mirrored.txt"))).toBe(true);
    expect(harness.refreshedStatuses).toEqual([]);
    expect(await harness.firstReportedFailure()).toEqual(
      Option.some({
        worktreePath: harness.worktreePath,
        error: expect.stringContaining("is managed by the aether session"),
        at: expect.any(String),
      }),
    );
  });
});

// FILE: AetherAdapter.test.ts
// Purpose: The Aether adapter's lifecycle against a fake Aether API, a fake workspace socket and a
// real Git worktree whose origin is a local bare repository: task creation, turns, settlement
// behind the mirror, retries of lost sends, questions, interrupts, restarts and ownership.
// Layer: Server provider tests (Aether).
import { execFileSync } from "node:child_process";
import { existsSync, writeFileSync } from "node:fs";
import * as fs from "node:fs/promises";
import path from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import {
  ApprovalRequestId,
  CheckpointRef,
  OrchestrationProposedPlanId,
  ProjectId,
  ThreadId,
  TurnId,
  type OrchestrationCommand,
  type OrchestrationThreadShell,
  type ProviderRuntimeEvent,
  type ProviderSendTurnInput,
} from "@synara/contracts";
import { PROVIDER_DESCRIPTOR_BY_KIND } from "@synara/shared/providerMetadata";
import {
  Duration,
  Effect,
  Exit,
  Fiber,
  FileSystem,
  Latch,
  Layer,
  Option,
  Scope,
  Stream,
} from "effect";
import { describe, expect } from "vitest";

import { CheckpointStoreLive } from "../../checkpointing/Layers/CheckpointStore.ts";
import { CheckpointStore } from "../../checkpointing/Services/CheckpointStore.ts";
import { ServerConfig } from "../../config.ts";
import { GitCoreLive } from "../../git/Layers/GitCore.ts";
import { GitCore } from "../../git/Services/GitCore.ts";
import { ProviderRuntimeEventRepositoryLive } from "../../persistence/Layers/ProviderRuntimeEvents.ts";
import {
  ProjectionThreadRepository,
  type ProjectionThread,
  type ProjectionThreadRepositoryShape,
} from "../../persistence/Services/ProjectionThreads.ts";
import type { PersistedProviderRuntimeEvent } from "../../persistence/Services/ProviderRuntimeEvents.ts";
import {
  AetherApiConflictError,
  AetherApiHttpError,
  type AetherApiError,
} from "../aether/aetherApiErrors.ts";
import { AetherToolId, type AetherWorkspaceId } from "../aether/aetherApiSchemas.ts";
import {
  makeFakeAetherTask,
  FAKE_HEAD_BRANCH,
  FAKE_REPO_URL,
  type FakeTurn,
} from "../aether/testing/aetherFakeTask.ts";
import {
  PROJECT_ID,
  pullRequestBody,
  reviewSummaryBody,
  TASK_ID,
} from "../aether/testing/aetherApiFixtures.ts";
import {
  added,
  diffOf,
  hunk,
  makeFakeWorkspace,
  modified,
  tooLarge,
} from "../aether/testing/aetherMirrorFixtures.ts";
import { classifyProviderAttemptOutcome } from "../../orchestration/Layers/ProviderCommandReactor.ts";
import { claimQuitResumeRecord, prepareQuitResume } from "../../orchestration/quitResume.ts";
import { providerAdapterConformanceIssues } from "../providerAdapterConformance.ts";
import { planProviderRuntimeReconciliation } from "../providerRuntimeReconciliation.ts";
import { AetherAdapter, type AetherAdapterShape } from "../Services/AetherAdapter.ts";
import { AetherApiClient } from "../Services/AetherApiClient.ts";
import type { ProviderRuntimeBinding } from "../Services/ProviderSessionDirectory.ts";
import { ProviderAdapterRegistry } from "../Services/ProviderAdapterRegistry.ts";
import { ProviderService } from "../Services/ProviderService.ts";
import { ProviderSessionDirectory } from "../Services/ProviderSessionDirectory.ts";
import { ProviderUnsupportedError } from "../Errors.ts";
import { ProviderSessionRuntimeRepositoryLive } from "../../persistence/Layers/ProviderSessionRuntime.ts";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import {
  AetherAdapterLive,
  keepCursorIn,
  makeAetherAdapter,
  openRunningWorkspace,
  type AetherAdapterOptions,
} from "./AetherAdapter.ts";
import { makeProviderServiceLive } from "./ProviderService.ts";
import { ProviderSessionDirectoryLive } from "./ProviderSessionDirectory.ts";

const GitLayer = GitCoreLive.pipe(
  Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "synara-aether-adapter-test-" })),
  Layer.provide(NodeServices.layer),
);
const TestLayer = Layer.mergeAll(GitLayer, CheckpointStoreLive.pipe(Layer.provide(GitLayer))).pipe(
  Layer.provideMerge(NodeServices.layer),
);

const THREAD_ID = ThreadId.makeUnsafe("thread-aether");
/** The proposed plan Synara's Implement action names on its turn. */
const SOURCE_PLAN = {
  threadId: THREAD_ID,
  planId: OrchestrationProposedPlanId.makeUnsafe("plan-1"),
};
const MODEL = "claude-opus-5";

const git = (cwd: string, args: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const gitCore = yield* GitCore;
    const result = yield* gitCore.execute({ operation: "AetherAdapter.test", cwd, args });
    return result.stdout.trim();
  });

/** Polls in real time: the adapter's poll loop runs on the live clock. */
const waitFor = <A>(what: string, read: () => A | undefined) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 500; attempt += 1) {
      const value = read();
      if (value !== undefined) return value;
      yield* Effect.sleep("20 millis");
    }
    return yield* Effect.die(new Error(`Timed out waiting for ${what}.`));
  });

const completions = (events: ReadonlyArray<ProviderRuntimeEvent>) =>
  events.filter((event) => event.type === "turn.completed");

/** A session directory in memory: what a restart keeps. */
const sessionDirectoryLayer = ProviderSessionDirectoryLive.pipe(
  Layer.provide(ProviderSessionRuntimeRepositoryLive.pipe(Layer.provide(SqlitePersistenceMemory))),
);

/** ProviderService over one Aether adapter, on the session directory it is given. */
const providerServiceOver = (adapter: AetherAdapterShape) =>
  makeProviderServiceLive().pipe(
    Layer.provide(
      Layer.succeed(ProviderAdapterRegistry, {
        getByProvider: (provider) =>
          provider === "aether"
            ? Effect.succeed(adapter)
            : Effect.fail(new ProviderUnsupportedError({ provider })),
        listProviders: () => Effect.succeed(["aether"] as const),
      }),
    ),
  );

/** One process's ProviderService over `adapter`, living as long as `scope`. */
const launchProviderService = (adapter: AetherAdapterShape, scope: Scope.Scope) =>
  Layer.buildWithScope(providerServiceOver(adapter), scope).pipe(
    Effect.flatMap((services) => Effect.service(ProviderService).pipe(Effect.provide(services))),
  );

/** The thread as the live reconciler reads it: running `turnId` since `since`. */
const reconcilableThread = (turnId: TurnId, since: string): OrchestrationThreadShell => ({
  id: THREAD_ID,
  projectId: ProjectId.makeUnsafe("project-aether"),
  title: "Aether thread",
  modelSelection: { provider: "aether", model: MODEL },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  associatedWorktreePath: null,
  associatedWorktreeBranch: null,
  associatedWorktreeRef: null,
  createBranchFlowCompleted: false,
  isPinned: false,
  parentThreadId: null,
  creationSource: null,
  sourceThreadId: null,
  sourceTurnId: null,
  gatewayOperationId: null,
  gatewayOperationIndex: null,
  subagentAgentId: null,
  subagentNickname: null,
  subagentRole: null,
  forkSourceThreadId: null,
  sidechatSourceThreadId: null,
  lastKnownPr: null,
  latestTurn: {
    turnId,
    state: "running",
    requestedAt: since,
    startedAt: since,
    completedAt: null,
    assistantMessageId: null,
  },
  createdAt: since,
  updatedAt: since,
  archivedAt: null,
  handoff: null,
  session: {
    threadId: THREAD_ID,
    status: "running",
    providerName: "aether",
    runtimeMode: "full-access",
    activeTurnId: turnId,
    lastError: null,
    updatedAt: since,
  },
});

/**
 * A repository whose origin is `FAKE_REPO_URL`, rewritten to a local bare repository, with a
 * linked worktree for the thread off `main`; a fake Aether task and workspace; and a journal that
 * records every event an adapter emits, as the runtime event pump would.
 */
const makeHarness = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const root = yield* fileSystem
    .makeTempDirectoryScoped({ prefix: "aether-adapter-" })
    .pipe(Effect.flatMap((dir) => Effect.promise(() => fs.realpath(dir))));
  const origin = path.join(root, "origin.git");
  const repo = path.join(root, "repo");
  const worktree = path.join(root, "worktree");
  yield* Effect.promise(async () => {
    await fs.mkdir(repo);
    await fs.writeFile(path.join(repo, "app.txt"), "one\ntwo\n");
  });
  yield* git(root, ["init", "--bare", "--initial-branch=main", origin]);
  yield* git(repo, ["init", "--initial-branch=main"]);
  yield* git(repo, ["config", "user.email", "test@test.com"]);
  yield* git(repo, ["config", "user.name", "Test"]);
  yield* git(repo, ["config", "core.autocrlf", "false"]);
  yield* git(repo, ["add", "-A"]);
  yield* git(repo, ["commit", "-m", "base"]);
  yield* git(repo, ["remote", "add", "origin", FAKE_REPO_URL]);
  yield* git(repo, ["config", `url.${origin}.insteadOf`, FAKE_REPO_URL]);
  yield* git(repo, ["push", "origin", "main"]);
  // Synara names a new thread's worktree branch `synara/<8 hex>`.
  yield* git(repo, ["worktree", "add", "-b", "synara/0a1b2c3d", worktree, "main"]);
  const baseCommit = yield* git(worktree, ["rev-parse", "HEAD"]);

  const aether = makeFakeAetherTask();
  const workspace = makeFakeWorkspace();
  workspace.serve(diffOf(baseCommit));
  const vm: { running: boolean; failure: AetherApiError | undefined } = {
    running: true,
    failure: undefined,
  };
  const journal: PersistedProviderRuntimeEvent[] = [];
  let sequence = 0;
  /** What the runtime event journal's retention does: drops the rows of completed turns. */
  const pruneCompletedTurns = () => {
    const completed = new Set(
      journal.flatMap(({ event }) => (event.type === "turn.completed" ? [event.turnId] : [])),
    );
    journal.splice(
      0,
      journal.length,
      ...journal.filter(({ event }) => event.turnId === undefined || !completed.has(event.turnId)),
    );
  };

  /** An adapter in its own scope, its events not yet read; closing the scope ends it. */
  const makeAdapter = (
    bindings: ReadonlyArray<ProviderRuntimeBinding> = [],
    boot: Partial<Pick<AetherAdapterOptions, "client" | "archivedBindings" | "keepCursor">> = {},
  ) =>
    Effect.gen(function* () {
      const scope = yield* Scope.make();
      const adapter = yield* makeAetherAdapter({
        client: boot.client ?? aether.client,
        openWorkspace: () =>
          vm.failure !== undefined
            ? Effect.fail(vm.failure)
            : Effect.succeed(vm.running ? Option.some(workspace.workspace) : Option.none()),
        journal: {
          getHighWaterSequence: Effect.sync(() => sequence),
          readThreadEvents: (input) =>
            Effect.sync(() =>
              journal
                .filter(
                  ({ sequence, event }) =>
                    event.threadId === input.threadId &&
                    (input.turnId === undefined || event.turnId === input.turnId) &&
                    sequence <= input.throughSequenceInclusive &&
                    sequence < (input.beforeSequenceExclusive ?? Number.POSITIVE_INFINITY),
                )
                .toReversed()
                .slice(0, input.limit),
            ),
        },
        bindings,
        archivedBindings: boot.archivedBindings ?? [],
        // A test that follows the binding passes one that writes the session directory.
        keepCursor: boot.keepCursor ?? (() => Effect.void),
      }).pipe(Scope.provide(scope));
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
      return { adapter, scope };
    });

  /**
   * Starts an adapter and records its events in the journal, as the runtime event pump would;
   * `stop` is the process going away.
   */
  const launch = (bindings: ReadonlyArray<ProviderRuntimeBinding> = []) =>
    Effect.gen(function* () {
      const { adapter, scope } = yield* makeAdapter(bindings);
      const events: ProviderRuntimeEvent[] = [];
      yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          events.push(event);
          journal.push({ sequence: (sequence += 1), event });
        }),
      ).pipe(Effect.forkIn(scope));
      return { adapter, events, stop: Scope.close(scope, Exit.void) };
    });

  const start = (adapter: AetherAdapterShape) =>
    adapter.startSession({
      threadId: THREAD_ID,
      provider: "aether",
      cwd: worktree,
      modelSelection: { provider: "aether", model: MODEL },
      runtimeMode: "full-access",
    });
  const send = (input: string, extra: Partial<ProviderSendTurnInput> = {}) =>
    ({ threadId: THREAD_ID, input, ...extra }) satisfies ProviderSendTurnInput;
  /** The VM finishes each turn at once, with a line of text. */
  const finishTurns = () => {
    aether.hooks.onTurn = (turn: FakeTurn) => {
      aether.say(turn, `text-${turn.id}`, "Done.");
      aether.settle(turn);
    };
  };

  /** The VM's clone of origin, on the task's branch from the base: it commits and pushes. */
  const vmClone = path.join(root, "vm");
  const vmPush = (files: Readonly<Record<string, string>>, message: string, rewrite = false) =>
    Effect.gen(function* () {
      if (!existsSync(vmClone)) {
        yield* git(root, ["clone", "-q", origin, vmClone]);
        yield* git(vmClone, ["config", "user.email", "agent@test.com"]);
        yield* git(vmClone, ["config", "user.name", "Agent"]);
        yield* git(vmClone, ["checkout", "-q", "-b", FAKE_HEAD_BRANCH, baseCommit]);
      }
      if (rewrite) yield* git(vmClone, ["reset", "-q", "--hard", "HEAD~1"]);
      yield* Effect.promise(async () => {
        for (const [file, content] of Object.entries(files)) {
          await fs.writeFile(path.join(vmClone, file), content);
        }
      });
      yield* git(vmClone, ["add", "-A"]);
      yield* git(vmClone, ["commit", "-q", "-m", message]);
      yield* git(vmClone, ["push", "-q", "--force", "origin", FAKE_HEAD_BRANCH]);
      return yield* git(vmClone, ["rev-parse", "HEAD"]);
    });

  /** `vmPush` at once, outside any effect: the VM moving on while Synara reads it. */
  const vmPushNow = (files: Readonly<Record<string, string>>, message: string) => {
    const run = (args: ReadonlyArray<string>) =>
      execFileSync("git", args, { cwd: vmClone, encoding: "utf8" }).trim();
    for (const [file, content] of Object.entries(files)) {
      writeFileSync(path.join(vmClone, file), content);
    }
    run(["add", "-A"]);
    run(["commit", "-q", "-m", message]);
    run(["push", "-q", "--force", "origin", FAKE_HEAD_BRANCH]);
    return run(["rev-parse", "HEAD"]);
  };

  return {
    root,
    repo,
    worktree,
    baseCommit,
    vmPush,
    vmPushNow,
    status: Effect.gen(function* () {
      const gitCore = yield* GitCore;
      const result = yield* gitCore.execute({
        operation: "AetherAdapter.test",
        cwd: worktree,
        args: ["status", "--porcelain=v1", "--untracked-files=all"],
      });
      return result.stdout
        .split("\n")
        .filter((line) => line.length > 0)
        .toSorted();
    }),
    aether,
    workspace,
    vm,
    journal,
    pruneCompletedTurns,
    makeAdapter,
    launch,
    start,
    send,
    finishTurns,
    read: (file: string) => Effect.promise(() => fs.readFile(path.join(worktree, file), "utf8")),
    write: (file: string, content: string) =>
      Effect.promise(() => fs.writeFile(path.join(worktree, file), content)),
    exists: (file: string) =>
      Effect.promise(() =>
        fs.access(path.join(worktree, file)).then(
          () => true,
          () => false,
        ),
      ),
  };
});

type Harness = Effect.Success<typeof makeHarness>;

it.layer(TestLayer, { excludeTestServices: true })("AetherAdapter", (it) => {
  describe("a turn", () => {
    it.effect("creates the task, mirrors the VM's changes, then completes the turn once", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter, events } = yield* h.launch();
        expect(providerAdapterConformanceIssues(adapter)).toEqual([]);
        expect(adapter.capabilities).toMatchObject({
          supportsTurnSteering: true,
          sessionModelSwitch: "unsupported",
        });
        // The descriptor flag locks the picker on a started thread; it must say what the
        // adapter says.
        expect(PROVIDER_DESCRIPTOR_BY_KIND.aether.supportsSessionModelSwitch).toBe(
          adapter.capabilities.sessionModelSwitch !== "unsupported",
        );
        expect(adapter.reattachTurn).toBeDefined();

        const session = yield* h.start(adapter);
        // Persisted with the session, before anything is sent.
        const cursor = session.resumeCursor as { readonly idempotencyKey: string };
        expect(session.resumeCursor).toMatchObject({
          status: "creating",
          worktreePath: h.worktree,
        });

        h.aether.hooks.onTurn = (turn) => {
          h.aether.tool(turn, "toolu_1", "bun run test");
          h.aether.say(turn, "text-1", "Added the feature.");
          h.aether.settle(turn);
        };
        h.workspace.serve(diffOf(h.baseCommit, added("feature.txt", "hello\n")));
        const checkpoints = yield* CheckpointStore;
        const before = CheckpointRef.makeUnsafe("refs/synara/checkpoints/aether/turn/0");
        const after = CheckpointRef.makeUnsafe("refs/synara/checkpoints/aether/turn/1");
        yield* checkpoints.captureCheckpoint({ cwd: h.worktree, checkpointRef: before });

        const turn = yield* adapter.sendTurn(h.send("Add the feature"));
        const completed = yield* waitFor("turn.completed", () => completions(events)[0]);
        // What the checkpoint on `turn.completed` captures: the mirrored change.
        yield* checkpoints.captureCheckpoint({ cwd: h.worktree, checkpointRef: after });
        const diff = yield* checkpoints.diffCheckpoints({
          cwd: h.worktree,
          fromCheckpointRef: before,
          toCheckpointRef: after,
          ignoreWhitespace: false,
        });
        expect(diff).toContain("feature.txt");
        expect(diff).toContain("+hello");
        expect(completed).toMatchObject({ turnId: turn.turnId, payload: { state: "completed" } });

        expect(h.aether.calls.create).toEqual([
          {
            project_id: PROJECT_ID,
            prompt: "Add the feature",
            agent_type: "claude-code",
            model: MODEL,
            interaction_mode: "default",
            idempotency_key: cursor.idempotencyKey,
            base_branch: "main",
            auto_fix_ci: false,
            auto_fix_pr_comments: false,
            auto_rebase: false,
          },
        ]);
        expect(events[0]).toMatchObject({ type: "turn.started", turnId: turn.turnId });
        expect(events.filter((event) => event.type === "item.completed")).toMatchObject([
          { turnId: turn.turnId, itemId: "row-toolu_1" },
          { turnId: turn.turnId, itemId: "text-1", payload: { detail: "Added the feature." } },
        ]);
        expect(events.at(-1)).toBe(completed);

        // Later ticks of the loop settle nothing again.
        yield* Effect.sleep("1500 millis");
        expect(completions(events)).toHaveLength(1);
        const [idle] = yield* adapter.listSessions();
        expect(idle).toMatchObject({ status: "ready", resumeCursor: { active: false } });
        expect(idle?.activeTurnId).toBeUndefined();
      }),
    );

    it.effect("refuses the project's main checkout and a worktree origin does not have", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter } = yield* h.launch();
        const local = yield* adapter
          .startSession({
            threadId: THREAD_ID,
            provider: "aether",
            cwd: h.repo,
            modelSelection: { provider: "aether", model: MODEL },
            runtimeMode: "full-access",
          })
          .pipe(Effect.flip);
        expect(local.message).toContain("main checkout (Local)");

        yield* h.start(adapter);
        yield* h.write("draft.txt", "not pushed\n");
        const dirty = yield* adapter.sendTurn(h.send("Go")).pipe(Effect.flip);
        expect(dirty.message).toContain("uncommitted changes (draft.txt)");
        yield* Effect.promise(() => fs.rm(path.join(h.worktree, "draft.txt")));
        yield* git(h.worktree, ["commit", "--allow-empty", "-m", "local only"]);
        const unpushed = yield* adapter.sendTurn(h.send("Go")).pipe(Effect.flip);
        expect(unpushed.message).toContain("is not the tip of any branch on origin");
        expect(h.aether.calls.create).toEqual([]);
      }),
    );

    it.effect("bases the task on the project's default branch when others share its tip", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        // Aether's own task branches sit at a past tip of main.
        yield* git(h.repo, ["push", "origin", "main:aether/task-one", "main:aether/task-two"]);
        const { adapter, events } = yield* h.launch();
        yield* h.start(adapter);
        h.finishTurns();
        yield* adapter.sendTurn(h.send("Go"));
        yield* waitFor("turn.completed", () => completions(events)[0]);
        expect(h.aether.calls.create[0]).toMatchObject({ base_branch: "main" });
      }),
    );

    it.effect("matches the project by host, and keeps the remote URL out of errors", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter } = yield* h.launch();
        yield* h.start(adapter);
        h.aether.projects.splice(0, 1, {
          id: PROJECT_ID,
          repo_url: "https://gitlab.com/acme/widgets",
          default_branch: "main",
        });
        const otherHost = yield* adapter.sendTurn(h.send("Go")).pipe(Effect.flip);
        expect(otherHost.message).toContain(
          "No Aether project is connected to github.com/acme/widgets",
        );

        yield* git(h.repo, ["remote", "set-url", "origin", "https://user:s3cret@example.com/solo"]);
        const unreadable = yield* adapter.sendTurn(h.send("Go")).pipe(Effect.flip);
        expect(unreadable.message).toContain("origin remote");
        expect(unreadable.message).not.toContain("s3cret");
        expect(h.aether.calls.create).toEqual([]);
      }),
    );

    it.effect("declares no conversation rollback, so a rollback is refused, not restarted", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter } = yield* h.launch();
        // "restart-session" would clear the cursor and create a second task from the mirrored
        // worktree; without the capability, ProviderService calls rollbackThread, which refuses.
        expect(adapter.capabilities.conversationRollback).toBeUndefined();
        const refused = yield* adapter.rollbackThread(THREAD_ID, 1).pipe(Effect.flip);
        expect(refused.message).toContain("Aether cannot roll back");
      }),
    );

    it.effect("refuses a 402 with Aether's own message, typed as a payment failure", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter, events } = yield* h.launch();
        yield* h.start(adapter);
        h.aether.faults.payment = true;
        const refused = yield* adapter.sendTurn(h.send("Go")).pipe(Effect.flip);
        expect(refused.message).toContain(
          "Aether requires payment (POST /tasks, workspace_runtime_not_admitted, tier free, VM small): Your plan does not include cloud workspaces.",
        );
        expect(refused.cause).toMatchObject({ _tag: "AetherApiPaymentRequiredError" });
        expect(events).toEqual([]);
      }),
    );

    it.effect("fails a turn whose task errored with Aether's own message", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter, events } = yield* h.launch();
        yield* h.start(adapter);
        const error =
          "Your inference balance is empty. Add funds in Settings › Inference to continue.";
        h.aether.hooks.onTurn = (turn) => h.aether.fail(turn, error);
        const turn = yield* adapter.sendTurn(h.send("Go"));
        const completed = yield* waitFor("turn.completed", () => completions(events)[0]);
        expect(completed).toMatchObject({
          turnId: turn.turnId,
          payload: { state: "failed", errorMessage: error },
        });
      }),
    );
  });

  describe("retries", () => {
    it.effect("retries a lost 202 with the same idempotency key and starts no second task", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter, events } = yield* h.launch();
        const session = yield* h.start(adapter);
        const { idempotencyKey } = session.resumeCursor as { readonly idempotencyKey: string };
        h.finishTurns();
        h.aether.faults.loseCreate = true;

        const lost = yield* adapter.sendTurn(h.send("Add the feature")).pipe(Effect.flip);
        expect(lost).toMatchObject({ _tag: "ProviderAdapterRequestError", method: "createTask" });
        expect(events).toEqual([]);
        // The task exists on Aether; only the response was lost.
        expect(h.aether.turns).toHaveLength(1);

        const turn = yield* adapter.sendTurn(h.send("Add the feature"));
        yield* waitFor("turn.completed", () => completions(events)[0]);
        expect(h.aether.calls.create.map((call) => call.idempotency_key)).toEqual([
          idempotencyKey,
          idempotencyKey,
        ]);
        expect(h.aether.turns).toHaveLength(1);
        expect(completions(events)).toMatchObject([{ turnId: turn.turnId }]);
      }),
    );

    it.effect("retries a lost respond with the same client_message_id and joins its turn", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter, events } = yield* h.launch();
        yield* h.start(adapter);
        h.finishTurns();
        yield* adapter.sendTurn(h.send("First"));
        yield* waitFor("the first turn", () => completions(events)[0]);

        // The message lands and its turn even finishes before the response is lost.
        h.aether.faults.loseRespond = true;
        const lost = yield* adapter.sendTurn(h.send("Second")).pipe(Effect.flip);
        expect(lost).toMatchObject({ _tag: "ProviderAdapterRequestError", method: "sendTurn" });
        yield* Effect.sleep("1500 millis");
        expect(completions(events)).toHaveLength(1);

        const retry = yield* adapter.sendTurn(h.send("Second"));
        const [first, second] = h.aether.calls.respond.map((call) => call.client_message_id);
        expect(second).toBe(first);
        expect(retry.turnId).toBe(first);
        expect(h.aether.turns).toHaveLength(2);
        yield* waitFor("the retried turn", () => completions(events)[1]);
        expect(completions(events)).toMatchObject([{}, { turnId: retry.turnId }]);
        // Not a message from outside Synara.
        expect(events.some((event) => event.type === "runtime.warning")).toBe(false);

        // The same text sent again is a new message.
        const again = yield* adapter.sendTurn(h.send("Second"));
        expect(again.turnId).not.toBe(retry.turnId);
        yield* waitFor("the third turn", () => completions(events)[2]);
        expect(h.aether.turns).toHaveLength(3);
      }),
    );
  });

  describe("lost sends", () => {
    it.effect("completes a retried send only after its run is ingested and mirrored", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter, events } = yield* h.launch();
        yield* h.start(adapter);
        h.finishTurns();
        yield* adapter.sendTurn(h.send("First"));
        yield* waitFor("the first turn", () => completions(events)[0]);
        // The loop goes idle once the task is; nothing wakes it until the retry.
        yield* Effect.sleep("1500 millis");

        // The message lands, its run settles, and the response is lost.
        h.workspace.serve(diffOf(h.baseCommit, added("feature.txt", "hello\n")));
        h.aether.faults.loseRespond = true;
        yield* adapter.sendTurn(h.send("Second")).pipe(Effect.flip);
        const landed = h.aether.turns[1]!;
        expect(landed.state).toBe("settled");

        const retry = yield* adapter.sendTurn(h.send("Second"));
        const completed = yield* waitFor("the retried turn", () => completions(events)[1]);
        // Mirrored before it completed, so the checkpoint holds the VM's changes.
        expect(yield* h.exists("feature.txt")).toBe(true);
        expect(completed).toMatchObject({ turnId: retry.turnId, payload: { state: "completed" } });
        // The run's rows belong to the retried turn, before its completion.
        const row = events.find((event) => event.itemId === `text-${landed.id}`);
        expect(row).toMatchObject({ turnId: retry.turnId });
        expect(events.indexOf(row!)).toBeLessThan(events.indexOf(completed));
        expect(events.some((event) => event.type === "runtime.warning")).toBe(false);
        yield* Effect.sleep("1500 millis");
        expect(completions(events)).toHaveLength(2);
      }),
    );
  });

  describe("boot progress", () => {
    it.effect("shows the workspace's boot status for the turn until the agent's first output", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter, events } = yield* h.launch();
        yield* h.start(adapter);
        let running: FakeTurn | undefined;
        // The VM boots: the turn runs, but the agent has said nothing yet.
        h.aether.hooks.onTurn = (turn) => {
          running = turn;
        };
        h.aether.remote.latest_status_message = "Booting workspace";
        h.aether.remote.latest_status_source = "infra";
        const turn = yield* adapter.sendTurn(h.send("Go"));
        const statuses = () =>
          events.flatMap((event) =>
            event.type === "runtime.status" ? [[event.turnId, event.payload.message]] : [],
          );
        yield* waitFor("the boot status", () => (statuses().length > 0 ? true : undefined));

        // The agent speaks; a later infra line (and the agent's own) no longer shows.
        h.aether.remote.latest_status_message = "Workspace ready";
        h.aether.say(running!, "text-1", "Starting on it.");
        yield* waitFor("the agent's text", () =>
          events.some((event) => event.itemId === "text-1") ? true : undefined,
        );
        h.aether.remote.latest_status_message = "Run command";
        h.aether.remote.latest_status_source = "agent";
        h.aether.settle(running!);
        yield* waitFor("turn.completed", () => completions(events)[0]);

        expect(statuses()).toEqual([[turn.turnId, "Booting workspace"]]);
      }),
    );
  });

  describe("the task's branch", () => {
    it.effect(
      "puts the worktree on the task's branch and follows each push before the turn completes",
      () =>
        Effect.gen(function* () {
          const h = yield* makeHarness;
          const { adapter, events } = yield* h.launch();
          yield* h.start(adapter);
          let pushed = "";
          h.aether.hooks.onTurn = (turn) => {
            h.aether.say(turn, `text-${turn.id}`, "Committed the fix and started notes.");
            h.aether.settle(turn);
          };
          h.workspace.serve(
            diffOf(
              h.baseCommit,
              modified("app.txt", hunk(1, 1, " one", "-two", "+TWO")),
              added("notes.md", "not committed yet\n"),
            ),
          );
          // The agent committed app.txt and pushed; notes.md is only in its working tree.
          pushed = yield* h.vmPush({ "app.txt": "one\nTWO\n" }, "Capitalize two");

          yield* adapter.sendTurn(h.send("Fix it and push"));
          yield* waitFor("turn.completed", () => completions(events)[0]);

          expect(yield* git(h.worktree, ["symbolic-ref", "--short", "HEAD"])).toBe(
            FAKE_HEAD_BRANCH,
          );
          expect(yield* git(h.worktree, ["rev-parse", "HEAD"])).toBe(pushed);
          expect(yield* git(h.worktree, ["rev-parse", "--abbrev-ref", "@{upstream}"])).toBe(
            `origin/${FAKE_HEAD_BRANCH}`,
          );
          // Pushed work is a commit; only what the agent has not committed is a change.
          expect(yield* h.status).toEqual(["?? notes.md"]);
          expect(events.filter((event) => event.type === "runtime.warning")).toEqual([]);
        }),
    );

    it.effect("warns and keeps the branch when origin was rewritten, and keeps mirroring", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter, events } = yield* h.launch();
        yield* h.start(adapter);
        h.finishTurns();
        const first = yield* h.vmPush({ "a.md": "a\n" }, "A");
        h.workspace.serve(diffOf(h.baseCommit, added("a.md", "a\n")));
        yield* adapter.sendTurn(h.send("One"));
        yield* waitFor("the first turn", () => completions(events)[0]);
        expect(yield* git(h.worktree, ["rev-parse", "HEAD"])).toBe(first);

        const rewritten = yield* h.vmPush({ "b.md": "b\n" }, "B instead", true);
        h.workspace.serve(diffOf(h.baseCommit, added("b.md", "b\n")));
        const second = yield* adapter.sendTurn(h.send("Two"));
        const completed = yield* waitFor("the second turn", () => completions(events)[1]);

        expect(completed).toMatchObject({ payload: { state: "completed" } });
        const warning = events.find((event) => event.type === "runtime.warning");
        expect(warning).toMatchObject({ turnId: second.turnId });
        expect(warning?.type === "runtime.warning" && warning.payload.message).toContain(
          `does not contain the local branch's tip ${first.slice(0, 12)}`,
        );
        expect(warning?.type === "runtime.warning" && warning.payload.message).toContain(
          rewritten.slice(0, 12),
        );
        expect(yield* git(h.worktree, ["rev-parse", "HEAD"])).toBe(first);
        expect(yield* h.read("b.md")).toBe("b\n");
      }),
    );

    it.effect(
      "reports the task's pull request with its CI and review, and each change to them",
      () =>
        Effect.gen(function* () {
          const h = yield* makeHarness;
          const { adapter, events } = yield* h.launch();
          yield* h.start(adapter);
          h.aether.hooks.onTurn = (turn) => {
            // The agent opened the pull request during the turn; CI and the review run after it.
            h.aether.remote.primary_pull_request = pullRequestBody({ ci_status: "pending" });
            h.aether.remote.pr_review_summary = reviewSummaryBody({ run_status: "reviewing" });
            delete (h.aether.remote.pr_review_summary as { verdict?: unknown }).verdict;
            h.aether.say(turn, `text-${turn.id}`, "Opened the PR.");
            h.aether.settle(turn);
          };
          yield* adapter.sendTurn(h.send("Open a PR"));
          yield* waitFor("turn.completed", () => completions(events)[0]);
          const reports = () =>
            events.flatMap((event) =>
              event.type === "vcs.state.changed" && event.payload.pullRequest !== undefined
                ? [event.payload.pullRequest]
                : [],
            );
          expect(reports().at(-1)).toEqual({
            number: 18,
            title: "Add docs/demo.md with checkout edge cases",
            url: "https://github.com/Aether-Runtime/acme-checkout/pull/18",
            baseBranch: "main",
            headBranch: "aether/acme-checkout-640050b7d9ed",
            state: "open",
            additions: 3,
            deletions: 0,
            checks: { status: "pending", passed: 9, failed: 0, total: 9 },
            review: { status: "reviewing", verdict: null },
          });

          // CI passes and the review approves; the next read of the task reports both.
          h.aether.remote.primary_pull_request = pullRequestBody();
          h.aether.remote.pr_review_summary = reviewSummaryBody();
          h.finishTurns();
          yield* adapter.sendTurn(h.send("Thanks"));
          yield* waitFor("the second turn", () => completions(events)[1]);
          expect(reports().at(-1)).toMatchObject({
            checks: { status: "passed", passed: 9, failed: 0, total: 9 },
            review: { status: "completed", verdict: "approve" },
          });
          // The branch the reactor reads from disk is the task's by then.
          expect(yield* git(h.worktree, ["symbolic-ref", "--short", "HEAD"])).toBe(
            FAKE_HEAD_BRANCH,
          );
        }),
    );

    it.effect("keeps following the task when its pull request data cannot be read", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter, events } = yield* h.launch();
        yield* h.start(adapter);
        // GitHub has not computed the diff stat yet, say, and a count is missing.
        const { checks_total: _missing, ...partial } = pullRequestBody({ additions: null });
        h.aether.remote.primary_pull_request = partial;
        h.finishTurns();
        yield* adapter.sendTurn(h.send("One"));
        const first = yield* waitFor("the first turn", () => completions(events)[0]);
        yield* adapter.sendTurn(h.send("Two"));
        const second = yield* waitFor("the second turn", () => completions(events)[1]);

        expect([first, second]).toMatchObject([
          { payload: { state: "completed" } },
          { payload: { state: "completed" } },
        ]);
        const warnings = events.flatMap((event) =>
          event.type === "runtime.warning" ? [event.payload.message] : [],
        );
        expect(warnings).toHaveLength(1);
        expect(warnings[0]).toContain("Aether sent pull request data Synara cannot read");
        expect(
          events.some(
            (event) =>
              event.type === "vcs.state.changed" && event.payload.pullRequest !== undefined,
          ),
        ).toBe(false);
      }),
    );

    it.effect(
      "warns once, and reports no pull request, for one in a state Synara does not know",
      () =>
        Effect.gen(function* () {
          const h = yield* makeHarness;
          const { adapter, events } = yield* h.launch();
          yield* h.start(adapter);
          h.aether.remote.primary_pull_request = pullRequestBody({ state: "draft" });
          h.finishTurns();
          yield* adapter.sendTurn(h.send("One"));
          yield* waitFor("the first turn", () => completions(events)[0]);
          yield* adapter.sendTurn(h.send("Two"));
          yield* waitFor("the second turn", () => completions(events)[1]);

          const warnings = events.filter(
            (event) =>
              event.type === "runtime.warning" && event.payload.message.includes("#18 in a state"),
          );
          expect(warnings).toHaveLength(1);
          expect(
            events.some(
              (event) =>
                event.type === "vcs.state.changed" && event.payload.pullRequest !== undefined,
            ),
          ).toBe(false);
        }),
    );

    it.effect(
      "never moves the branch past a working tree it has not mirrored, even after a reattach",
      () =>
        Effect.gen(function* () {
          const h = yield* makeHarness;
          const first = yield* h.launch();
          yield* h.start(first.adapter);
          h.finishTurns();
          h.workspace.serve(diffOf(h.baseCommit, added("notes.md", "notes\n")));
          yield* first.adapter.sendTurn(h.send("First"));
          yield* waitFor("the first turn", () => completions(first.events)[0]);
          expect(yield* h.status).toEqual(["?? notes.md"]);
          const [persisted] = yield* first.adapter.listSessions();
          yield* first.stop;

          // While Synara is closed, the agent edits app.txt, commits both files, pushes, and
          // the workspace suspends: nothing mirrors app.txt's new content.
          const pushed = yield* h.vmPush(
            { "notes.md": "notes\n", "app.txt": "one\nTWO\n" },
            "Notes and TWO",
          );
          h.workspace.serve(
            diffOf(
              h.baseCommit,
              added("notes.md", "notes\n"),
              modified("app.txt", hunk(1, 1, " one", "-two", "+TWO")),
            ),
          );
          h.vm.running = false;
          const second = yield* h.launch();
          yield* second.adapter.startSession({
            threadId: THREAD_ID,
            provider: "aether",
            cwd: h.worktree,
            resumeCursor: persisted!.resumeCursor,
            runtimeMode: "full-access",
          });
          yield* Effect.sleep("1500 millis");
          // The agent's committed app.txt never reads as a local reversion.
          expect(yield* h.status).toEqual(["?? notes.md"]);
          expect(yield* git(h.worktree, ["rev-parse", "HEAD"])).toBe(h.baseCommit);

          // A turn that settles while the workspace is still suspended defers its mirror, and
          // the branch waits with it.
          yield* second.adapter.sendTurn(h.send("Second"));
          yield* waitFor("the second turn", () => completions(second.events)[0]);
          expect(yield* h.status).toEqual(["?? notes.md"]);
          expect(yield* git(h.worktree, ["rev-parse", "HEAD"])).toBe(h.baseCommit);

          // Once a mirror writes the working tree, the branch follows: the commit is a commit.
          h.vm.running = true;
          yield* second.adapter.sendTurn(h.send("Third"));
          yield* waitFor("the third turn", () => completions(second.events)[1]);
          expect(yield* git(h.worktree, ["rev-parse", "HEAD"])).toBe(pushed);
          expect(yield* h.status).toEqual([]);
        }),
    );

    it.effect("warns at every mirror while the branch has local commits, naming both commits", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter, events } = yield* h.launch();
        yield* h.start(adapter);
        h.finishTurns();
        h.workspace.serve(diffOf(h.baseCommit, added("notes.md", "notes\n")));
        yield* adapter.sendTurn(h.send("One"));
        yield* waitFor("the first turn", () => completions(events)[0]);
        const warnings = () =>
          events.flatMap((event) =>
            event.type === "runtime.warning" ? [event.payload.message] : [],
          );
        expect(warnings()).toEqual([]);

        // The user commits an edit of their own; the agent has pushed nothing yet.
        yield* h.write("app.txt", "mine\n");
        yield* git(h.worktree, ["add", "-A"]);
        yield* git(h.worktree, ["commit", "-q", "-m", "Mine"]);
        const local = yield* git(h.worktree, ["rev-parse", "HEAD"]);
        yield* adapter.sendTurn(h.send("Two"));
        yield* waitFor("the second turn", () => completions(events)[1]);
        const unpushed = warnings().at(-1);
        expect(unpushed).toContain(local.slice(0, 12));
        expect(unpushed).toContain(h.baseCommit.slice(0, 12));
        expect(unpushed).toContain("reverted");
        // The mirror went on: the working tree is the VM's again.
        expect(yield* h.read("app.txt")).toBe("one\ntwo\n");

        // The agent pushes: the local commit is still not on origin.
        const pushed = yield* h.vmPush({ "notes.md": "notes\n" }, "Notes");
        yield* adapter.sendTurn(h.send("Three"));
        yield* waitFor("the third turn", () => completions(events)[2]);
        const both = warnings().at(-1);
        expect(both).not.toBe(unpushed);
        expect(both).toContain(local.slice(0, 12));
        expect(both).toContain(pushed.slice(0, 12));
        expect(both).toContain("reverted");
        expect(yield* git(h.worktree, ["rev-parse", "HEAD"])).toBe(local);
      }),
    );

    it.effect(
      "says once that origin deleted the merged branch, and never calls it local commits",
      () =>
        Effect.gen(function* () {
          const h = yield* makeHarness;
          const { adapter, events } = yield* h.launch();
          yield* h.start(adapter);
          h.finishTurns();
          const pushed = yield* h.vmPush({ "a.md": "a\n" }, "A");
          h.workspace.serve(diffOf(h.baseCommit, added("a.md", "a\n")));
          yield* adapter.sendTurn(h.send("One"));
          yield* waitFor("the first turn", () => completions(events)[0]);
          expect(yield* git(h.worktree, ["rev-parse", "HEAD"])).toBe(pushed);

          // The pull request merges, and GitHub deletes its branch.
          yield* git(h.root, ["--git-dir", "origin.git", "branch", "-D", FAKE_HEAD_BRANCH]);
          for (const [index, message] of ["Two", "Three"].entries()) {
            yield* adapter.sendTurn(h.send(message));
            yield* waitFor(`turn ${index + 2}`, () => completions(events)[index + 1]);
          }
          const warnings = events.flatMap((event) =>
            event.type === "runtime.warning" ? [event.payload.message] : [],
          );
          expect(warnings).toHaveLength(1);
          expect(warnings[0]).toContain("no longer exists on origin");
          expect(warnings[0]).not.toContain("commits of its own (");
          expect(yield* git(h.worktree, ["rev-parse", "HEAD"])).toBe(pushed);
          expect(yield* h.status).toEqual([]);
        }),
    );

    it.effect("moves the branch only to what origin had before the mirror read the VM", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter, events } = yield* h.launch();
        yield* h.start(adapter);
        h.finishTurns();
        const pushed = yield* h.vmPush({ "a.md": "a\n" }, "A");
        h.workspace.serve(diffOf(h.baseCommit, added("a.md", "a\n")));
        // The agent pushes again after the mirror read the VM's working tree.
        let later: string | undefined;
        h.workspace.afterNextDiff(() => {
          later = h.vmPushNow({ "b.md": "b\n" }, "B");
        });
        yield* adapter.sendTurn(h.send("Go"));
        yield* waitFor("turn.completed", () => completions(events)[0]);

        expect(later).toBeDefined();
        expect(yield* git(h.root, ["--git-dir", "origin.git", "rev-parse", FAKE_HEAD_BRANCH])).toBe(
          later,
        );
        expect(yield* git(h.worktree, ["rev-parse", "HEAD"])).toBe(pushed);
        expect(yield* h.status).toEqual([]);
      }),
    );
  });

  describe("lost sends across a restart", () => {
    it.effect("binds a retried send to the turn it landed in, and completes it at once", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const first = yield* h.launch();
        yield* h.start(first.adapter);
        h.finishTurns();
        yield* first.adapter.sendTurn(h.send("First"));
        yield* waitFor("the first turn", () => completions(first.events)[0]);
        // What Synara persisted: the cursor as of the last turn.completed.
        const [persisted] = yield* first.adapter.listSessions();

        // The message lands and its run settles, but the response is lost; then Synara restarts.
        h.aether.faults.loseRespond = true;
        yield* first.adapter.sendTurn(h.send("Second")).pipe(Effect.flip);
        yield* first.stop;

        const second = yield* h.launch();
        yield* second.adapter.startSession({
          threadId: THREAD_ID,
          provider: "aether",
          cwd: h.worktree,
          resumeCursor: persisted!.resumeCursor,
          runtimeMode: "full-access",
        });
        yield* Effect.sleep("1500 millis");
        const retry = yield* second.adapter.sendTurn(h.send("Second"));
        const completed = yield* waitFor("the retried turn", () => completions(second.events)[0]);
        expect(completed).toMatchObject({ turnId: retry.turnId, payload: { state: "completed" } });
        expect(h.aether.turns).toHaveLength(2);
        const [lost, retried] = h.aether.calls.respond.map((call) => call.client_message_id);
        expect(retried).toBe(lost);
        expect(retry.resumeCursor).toMatchObject({ active: false });
      }),
    );
  });

  describe("refusals", () => {
    const attempt = (error: unknown) =>
      classifyProviderAttemptOutcome(Exit.fail(error) as Exit.Exit<void, unknown>)._tag;

    it.effect("rejects a create Aether refused, and the next send starts with a new key", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter, events } = yield* h.launch();
        const session = yield* h.start(adapter);
        const { idempotencyKey } = session.resumeCursor as { readonly idempotencyKey: string };
        h.finishTurns();
        h.aether.faults.refuseCreate = new AetherApiHttpError({
          endpoint: "POST /tasks",
          status: 400,
          serverMessage: "Reconnect Claude Code in Settings → Connections before starting a task.",
        });
        const refused = yield* adapter.sendTurn(h.send("First try")).pipe(Effect.flip);
        // Rejected, not uncertain: the command reactor does not quarantine the thread.
        expect(attempt(refused)).toBe("rejected");
        expect(refused.message).toContain("Reconnect Claude Code in Settings");

        yield* adapter.sendTurn(h.send("Second try"));
        yield* waitFor("turn.completed", () => completions(events)[0]);
        const [first, second] = h.aether.calls.create.map((call) => call.idempotency_key);
        expect(first).toBe(idempotencyKey);
        expect(second).not.toBe(idempotencyKey);
      }),
    );

    it.effect("leaves a 5xx on create uncertain, and the retry reuses the key", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter, events } = yield* h.launch();
        yield* h.start(adapter);
        h.finishTurns();
        h.aether.faults.refuseCreate = new AetherApiHttpError({
          endpoint: "POST /tasks",
          status: 502,
        });
        const failed = yield* adapter.sendTurn(h.send("Go")).pipe(Effect.flip);
        expect(attempt(failed)).toBe("uncertain");
        yield* adapter.sendTurn(h.send("Go"));
        yield* waitFor("turn.completed", () => completions(events)[0]);
        const [first, retry] = h.aether.calls.create.map((call) => call.idempotency_key);
        expect(retry).toBe(first);
      }),
    );

    it.effect("rejects a respond Aether refused", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter, events } = yield* h.launch();
        yield* h.start(adapter);
        h.finishTurns();
        yield* adapter.sendTurn(h.send("First"));
        yield* waitFor("turn.completed", () => completions(events)[0]);
        h.aether.faults.refuseRespond = new AetherApiHttpError({
          endpoint: "POST /tasks/{taskID}/respond",
          status: 422,
          serverMessage: "The message is too long.",
        });
        const refused = yield* adapter.sendTurn(h.send("Second")).pipe(Effect.flip);
        expect(attempt(refused)).toBe("rejected");
        expect(refused.message).toContain("The message is too long.");
      }),
    );
  });

  describe("questions", () => {
    it.effect("answers with the shown question's tool id, and reads again on a stale answer", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter, events } = yield* h.launch();
        yield* h.start(adapter);
        h.aether.hooks.onTurn = (turn) => {
          h.aether.ask(turn, "toolu_ask", ["Pay now", "Place order"]);
          h.aether.hooks.onTurn = (answered) => {
            h.aether.say(answered, "text-after", "Done.");
            h.aether.settle(answered);
          };
        };
        const turn = yield* adapter.sendTurn(h.send("Build checkout"));
        const asked = yield* waitFor("the question", () =>
          events.find((event) => event.type === "user-input.requested"),
        );
        expect(asked).toMatchObject({ turnId: turn.turnId, requestId: "toolu_ask" });
        const question = ApprovalRequestId.makeUnsafe("toolu_ask");

        // A typed answer goes out as the question's custom answer; this one arrives stale.
        h.aether.faults.staleAnswer = true;
        const stale = yield* adapter
          .respondToUserInput(THREAD_ID, question, { "0": "Buy" })
          .pipe(Effect.flip);
        expect(stale.message).toContain(
          "Aether API conflict (POST /tasks/{taskID}/respond): The pending input changed.",
        );
        // Never retried blindly.
        expect(h.aether.calls.respond).toHaveLength(1);
        expect(h.aether.calls.respond[0]?.tool_response).toMatchObject({
          data: { answers: { "0": [-1] }, customAnswers: { "0": "Buy" } },
          expected_tool_id: "toolu_ask",
        });

        yield* adapter.respondToUserInput(THREAD_ID, question, { "0": "Place order" });
        expect(h.aether.calls.respond[1]).toMatchObject({
          message: JSON.stringify({ answers: { "0": [1] } }),
          tool_response: {
            tool_name: "ask_user",
            data: { answers: { "0": [1] } },
            expected_tool_id: "toolu_ask",
          },
        });
        expect(h.aether.calls.respond[1]?.tool_response).not.toHaveProperty(
          "expected_input_digest",
        );
        const completed = yield* waitFor("turn.completed", () => completions(events)[0]);
        expect(completed).toMatchObject({ turnId: turn.turnId, payload: { state: "completed" } });
        expect(events.find((event) => event.type === "user-input.resolved")).toMatchObject({
          payload: { answers: { "0": "Place order" } },
        });

        // Once answered, the question takes no more answers.
        const late = yield* adapter
          .respondToUserInput(THREAD_ID, question, { "0": "Pay now" })
          .pipe(Effect.flip);
        expect(late.message).toContain("no longer waiting for an answer");
        expect(h.aether.calls.respond).toHaveLength(2);
      }),
    );
    it.effect(
      "stamps the session's lifecycle generation on every event, and adopts a new one",
      () =>
        Effect.gen(function* () {
          const h = yield* makeHarness;
          const { adapter, events } = yield* h.launch();
          const startWith = (lifecycleGeneration: string) =>
            adapter.startSession({
              threadId: THREAD_ID,
              provider: "aether",
              lifecycleGeneration,
              cwd: h.worktree,
              modelSelection: { provider: "aether", model: MODEL },
              runtimeMode: "full-access",
            });
          yield* startWith("generation-1");
          h.aether.hooks.onTurn = (turn) => {
            h.aether.ask(turn, "exec-ask", [], {
              questions: [{ question: "Which color?", options: [{ label: "Red" }] }],
            });
            h.aether.hooks.onTurn = (answered) => {
              h.aether.say(answered, "text-after", "Done.");
              h.aether.settle(answered);
            };
          };
          yield* adapter.sendTurn(h.send("Paint it"));
          yield* waitFor("the question", () =>
            events.find((event) => event.type === "user-input.requested"),
          );
          // ProviderService routes an answer by the generation its request was raised in.
          expect(events.length).toBeGreaterThan(0);
          expect(events.every((event) => event.lifecycleGeneration === "generation-1")).toBe(true);

          // A start on the live session (ProviderService recovering it): the new generation applies.
          yield* startWith("generation-2");
          const before = events.length;
          yield* adapter.respondToUserInput(THREAD_ID, ApprovalRequestId.makeUnsafe("exec-ask"), {
            "0": "Red",
          });
          yield* waitFor("turn.completed", () => completions(events)[0]);
          const after = events.slice(before);
          expect(after.length).toBeGreaterThan(0);
          expect(after.every((event) => event.lifecycleGeneration === "generation-2")).toBe(true);
        }),
    );
    it.effect("points to Aether for a question Synara may no longer show, and reads it again", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter, events } = yield* h.launch();
        yield* h.start(adapter);
        h.aether.hooks.onTurn = (turn) => {
          h.aether.ask(turn, "toolu_ask", ["Pay now", "Place order"]);
          h.finishTurns();
        };
        yield* adapter.sendTurn(h.send("Build checkout"));
        yield* waitFor("the question", () =>
          events.find((event) => event.type === "user-input.requested"),
        );

        // The loop stops while the question waits: the turn fails, and Synara settles the
        // question as stale, so it shows it no more.
        h.aether.faults.live = new AetherApiHttpError({
          endpoint: "GET /tasks/{taskID}/conversation/live",
          status: 404,
        });
        yield* adapter
          .respondToUserInput(THREAD_ID, ApprovalRequestId.makeUnsafe("toolu_other"), {})
          .pipe(Effect.flip);
        const stopped = yield* waitFor("the failed turn", () => completions(events)[0]);
        expect(stopped).toMatchObject({ payload: { state: "failed" } });

        h.aether.faults.live = undefined;
        const gate = yield* Latch.make(false);
        h.aether.faults.liveGate = gate;
        const refused = yield* adapter.sendTurn(h.send("Ship it")).pipe(Effect.flip);
        expect(refused.message).toContain(
          "Answer it before sending a new message, in Aether if Synara no longer shows it.",
        );

        // Answered in Aether's own UI: the loop the refusal woke picks the answer up.
        yield* h.aether.client.respondToTask(TASK_ID, {
          message: JSON.stringify({ answers: { "0": [1] } }),
          client_message_id: "answered-in-aether",
          tool_response: {
            tool_name: "ask_user",
            data: { answers: { "0": [1] } },
            expected_tool_id: AetherToolId.makeUnsafe("toolu_ask"),
          },
        });
        h.aether.faults.liveGate = undefined;
        yield* gate.open;
        yield* waitFor("the answer", () =>
          events.find((event) => event.type === "user-input.resolved"),
        );
        const next = yield* adapter.sendTurn(h.send("Ship it"));
        const completed = yield* waitFor("the next turn", () => completions(events)[1]);
        expect(completed).toMatchObject({ turnId: next.turnId, payload: { state: "completed" } });
      }),
    );
    it.effect("answers Aether's question shape: options by index, free text as custom", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter, events } = yield* h.launch();
        yield* h.start(adapter);
        // A Codex task's questions, as Aether sends them: no header, no descriptions.
        h.aether.hooks.onTurn = (turn) =>
          h.aether.ask(turn, "exec-1", [], {
            questions: [
              {
                question: "Which color should I use?",
                // A field Synara does not read, as Aether may add one.
                options: [{ label: "Red", recommended: true }, { label: "Blue" }],
              },
              { question: "What should it be called?", options: [], allowOther: true },
            ],
          });
        yield* adapter.sendTurn(h.send("Paint it"));
        yield* waitFor("the question", () =>
          events.find((event) => event.type === "user-input.requested"),
        );
        yield* adapter.respondToUserInput(THREAD_ID, ApprovalRequestId.makeUnsafe("exec-1"), {
          "0": "Blue",
          "1": "Sky",
        });
        expect(h.aether.calls.respond[0]?.tool_response).toEqual({
          tool_name: "ask_user",
          data: { answers: { "0": [1], "1": [-1] }, customAnswers: { "1": "Sky" } },
          expected_tool_id: "exec-1",
        });
      }),
    );

    it.effect(
      "keeps a turn waiting on a question past the live reconciler's no-progress limit",
      () =>
        Effect.gen(function* () {
          const h = yield* makeHarness;
          const { adapter } = yield* h.makeAdapter();
          yield* Effect.gen(function* () {
            const providerService = yield* ProviderService;
            const directory = yield* ProviderSessionDirectory;
            const accepted: ProviderRuntimeEvent[] = [];
            yield* Stream.runForEach(providerService.streamEvents, (event) =>
              Effect.sync(() => void accepted.push(event)),
            ).pipe(Effect.forkScoped);
            yield* providerService.startSession(THREAD_ID, {
              threadId: THREAD_ID,
              provider: "aether",
              cwd: h.worktree,
              modelSelection: { provider: "aether", model: MODEL },
              runtimeMode: "full-access",
            });
            h.aether.hooks.onTurn = (turn) =>
              h.aether.ask(turn, "toolu_ask", ["Pay now", "Place order"]);
            const turn = yield* providerService.sendTurn({
              threadId: THREAD_ID,
              input: "Build checkout",
            });
            yield* waitFor("the question", () =>
              accepted.find((event) => event.type === "user-input.requested"),
            );

            // An hour on, nothing has moved on the thread: the question still waits.
            const quietSince = new Date(Date.now() - 60 * 60_000).toISOString();
            const bindings = yield* directory.listBindings();
            const liveSessions = yield* providerService.listSessions();
            const pumpHealth = yield* providerService.getRuntimeEventPumpHealth!();
            const reconcile = (hasPendingUserInput: boolean) =>
              planProviderRuntimeReconciliation({
                threads: [{ ...reconcilableThread(turn.turnId, quietSince), hasPendingUserInput }],
                bindings,
                liveSessions,
                pumpHealth,
                nowMs: Date.now(),
              });
            expect(reconcile(true)).toEqual([]);
            // Had it asked nothing, the same quiet turn would be settled.
            expect(reconcile(false)).toEqual([
              expect.objectContaining({
                action: "settle-interrupted",
                projectedTurnId: turn.turnId,
              }),
            ]);
          }).pipe(
            Effect.scoped,
            Effect.provide(
              Layer.mergeAll(
                providerServiceOver(adapter).pipe(Layer.provide(sessionDirectoryLayer)),
                sessionDirectoryLayer,
              ),
            ),
          );
        }),
    );
  });

  describe("plans", () => {
    it.effect("completes the planning turn, then the approving turn on the same Aether turn", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter, events } = yield* h.launch();
        yield* h.start(adapter);
        h.aether.hooks.onTurn = (turn) => {
          h.aether.propose(turn, "toolu_plan", "1. Edit\n2. Test");
          h.finishTurns();
        };
        const planning = yield* adapter.sendTurn(h.send("Plan it", { interactionMode: "plan" }));
        const proposed = yield* waitFor("the plan", () => completions(events)[0]);
        expect(proposed).toMatchObject({
          turnId: planning.turnId,
          payload: { state: "completed" },
        });
        expect(events.find((event) => event.type === "turn.proposed.completed")).toMatchObject({
          turnId: planning.turnId,
          payload: { planMarkdown: "1. Edit\n2. Test" },
        });

        // Synara's Implement action: a default-mode turn carrying the plan it implements.
        const approving = yield* adapter.sendTurn(
          h.send("PLEASE IMPLEMENT THIS PLAN:\n1. Edit\n2. Test", {
            interactionMode: "default",
            sourceProposedPlan: SOURCE_PLAN,
          }),
        );
        expect(h.aether.calls.respond).toEqual([
          {
            message: JSON.stringify({ approved: true }),
            // Approving leaves plan mode; without it Aether keeps planning.
            interaction_mode: "default",
            tool_response: {
              tool_name: "propose_plan",
              data: { approved: true },
              expected_tool_id: "toolu_plan",
            },
            client_message_id: approving.turnId,
          },
        ]);
        const implemented = yield* waitFor("the approved run", () => completions(events)[1]);
        expect(implemented).toMatchObject({
          turnId: approving.turnId,
          payload: { state: "completed" },
        });
        expect(h.aether.turns).toHaveLength(1);
        expect(
          events.find((event) => event.itemId === `text-${h.aether.turns[0]!.id}`),
        ).toMatchObject({ turnId: approving.turnId });
        yield* Effect.sleep("1500 millis");
        expect(completions(events)).toHaveLength(2);
      }),
    );
    it.effect("sends any other message while a plan waits as feedback, staying in plan mode", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter, events } = yield* h.launch();
        yield* h.start(adapter);
        h.aether.hooks.onTurn = (turn) => {
          h.aether.propose(turn, "toolu_plan", "1. Use SQLite");
          h.aether.hooks.onTurn = (revised) =>
            h.aether.propose(revised, "toolu_plan_2", "1. Use Postgres");
        };
        yield* adapter.sendTurn(h.send("Plan it", { interactionMode: "plan" }));
        yield* waitFor("the plan", () => completions(events)[0]);

        const feedback = "No. Use Postgres instead, do not implement that plan.";
        const revising = yield* adapter.sendTurn(h.send(feedback, { interactionMode: "default" }));
        const data = { approved: false, feedback };
        expect(h.aether.calls.respond).toEqual([
          {
            message: JSON.stringify(data),
            tool_response: { tool_name: "propose_plan", data, expected_tool_id: "toolu_plan" },
            client_message_id: revising.turnId,
          },
        ]);
        const revised = yield* waitFor("the revised plan", () => completions(events)[1]);
        expect(revised).toMatchObject({ turnId: revising.turnId, payload: { state: "completed" } });
        expect(events.findLast((event) => event.type === "turn.proposed.completed")).toMatchObject({
          turnId: revising.turnId,
          payload: { planMarkdown: "1. Use Postgres" },
        });
      }),
    );

    it.effect("refuses an answer to a plan that changed, and sends nothing twice", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter, events } = yield* h.launch();
        yield* h.start(adapter);
        h.aether.hooks.onTurn = (turn) => {
          h.aether.propose(turn, "toolu_plan", "1. Edit");
          h.finishTurns();
        };
        yield* adapter.sendTurn(h.send("Plan it", { interactionMode: "plan" }));
        yield* waitFor("the plan", () => completions(events)[0]);

        h.aether.faults.staleAnswer = true;
        const implement = h.send("PLEASE IMPLEMENT THIS PLAN:\n1. Edit", {
          sourceProposedPlan: SOURCE_PLAN,
        });
        const stale = yield* adapter.sendTurn(implement).pipe(Effect.flip);
        expect(stale.message).toContain("The pending input changed.");
        expect(events.filter((event) => event.type === "turn.started")).toHaveLength(1);

        const approving = yield* adapter.sendTurn(implement);
        const implemented = yield* waitFor("the approved run", () => completions(events)[1]);
        expect(implemented).toMatchObject({ turnId: approving.turnId });
        expect(h.aether.turns).toHaveLength(1);
      }),
    );
    it.effect.each(["before", "after"] as const)(
      "retries a lost Implement as the same plan answer, ingested %s the retry, completing after the mirror",
      (ingested) =>
        Effect.gen(function* () {
          const h = yield* makeHarness;
          const { adapter, events } = yield* h.launch();
          yield* h.start(adapter);
          h.aether.hooks.onTurn = (turn) => {
            h.aether.propose(turn, "toolu_plan", "1. Edit");
            h.finishTurns();
          };
          yield* adapter.sendTurn(h.send("Plan it", { interactionMode: "plan" }));
          yield* waitFor("the plan", () => completions(events)[0]);

          // The approval lands and its run settles, but the response is lost.
          h.workspace.serve(diffOf(h.baseCommit, added("feature.txt", "hello\n")));
          h.aether.faults.loseRespond = true;
          const implement = h.send("PLEASE IMPLEMENT THIS PLAN:\n1. Edit", {
            sourceProposedPlan: SOURCE_PLAN,
          });
          yield* adapter.sendTurn(implement).pipe(Effect.flip);
          const [planned] = h.aether.turns;
          expect(planned!.state).toBe("settled");
          const text = () => events.find((event) => event.itemId === `text-${planned!.id}`);
          if (ingested === "before") {
            // The loop reads the task again (as its 15 s look at a waiting plan would), and
            // settles and mirrors the run before the retry.
            yield* adapter
              .respondToUserInput(THREAD_ID, ApprovalRequestId.makeUnsafe("toolu_none"), {})
              .pipe(Effect.flip);
            // Written by the settle, which records the run as settled before it lets the retry in.
            yield* waitFor("the mirror", () =>
              existsSync(path.join(h.worktree, "feature.txt")) ? true : undefined,
            );
          }

          const retry = yield* adapter.sendTurn(implement);
          const completed = yield* waitFor("the approved run", () => completions(events)[1]);
          // The same answer again, which Aether keeps once: the agent never gets the prompt.
          const [lost, retried] = h.aether.calls.respond;
          expect(retried).toEqual(lost);
          expect(retry.turnId).toBe(lost!.client_message_id);
          expect(h.aether.turns).toHaveLength(1);
          // Mirrored before it completed, and the run's rows belong to the retried turn.
          expect(yield* h.exists("feature.txt")).toBe(true);
          expect(completed).toMatchObject({
            turnId: retry.turnId,
            payload: { state: "completed" },
          });
          expect(text()).toMatchObject({ turnId: retry.turnId });
          expect(events.indexOf(text()!)).toBeLessThan(events.indexOf(completed));
          yield* Effect.sleep("1500 millis");
          expect(completions(events)).toHaveLength(2);
        }),
    );
    it.effect.each(["an idle stop", "a restart"] as const)(
      "shows the run an Implement starts after %s while the plan waited, and its question",
      (detach) =>
        Effect.gen(function* () {
          const h = yield* makeHarness;
          const first = yield* h.launch();
          yield* h.start(first.adapter);
          h.aether.hooks.onTurn = (turn) => {
            h.aether.propose(turn, "toolu_plan", "1. Edit");
            h.aether.hooks.onTurn = (implementing) => {
              h.aether.say(implementing, "text-implementing", "Implementing.");
              h.aether.ask(implementing, "toolu_ask", ["Blue", "Red"]);
            };
          };
          yield* first.adapter.sendTurn(h.send("Plan it", { interactionMode: "plan" }));
          yield* waitFor("the plan", () => completions(first.events)[0]);
          const [session] = yield* first.adapter.listSessions();

          // The idle reaper stops the session, or Synara restarts; then the user implements.
          let second = first;
          if (detach === "an idle stop") {
            yield* first.adapter.stopSession(THREAD_ID);
          } else {
            yield* first.stop;
            second = yield* h.launch();
          }
          yield* second.adapter.startSession({
            threadId: THREAD_ID,
            provider: "aether",
            cwd: h.worktree,
            resumeCursor: session!.resumeCursor,
            runtimeMode: "full-access",
          });
          const approving = yield* second.adapter.sendTurn(
            h.send("PLEASE IMPLEMENT THIS PLAN:\n1. Edit", { sourceProposedPlan: SOURCE_PLAN }),
          );
          const asked = yield* waitFor("the question", () =>
            second.events.find((event) => event.type === "user-input.requested"),
          );
          expect(asked).toMatchObject({ turnId: approving.turnId, requestId: "toolu_ask" });
          expect(second.events.find((event) => event.itemId === "text-implementing")).toMatchObject(
            { turnId: approving.turnId },
          );
        }),
    );
  });

  describe("steering", () => {
    it.effect(
      "runs a steer into a running run as its own Aether turn, and completes after it",
      () =>
        Effect.gen(function* () {
          const h = yield* makeHarness;
          const { adapter, events } = yield* h.launch();
          yield* h.start(adapter);
          h.aether.hooks.onTurn = (turn) =>
            h.aether.say(turn, "text-live", "Working", { open: true });
          const turn = yield* adapter.sendTurn(h.send("Refactor"));
          yield* waitFor("streamed text", () =>
            events.find((event) => event.type === "content.delta"),
          );

          const steered = yield* adapter.steerTurn!(h.send("Also rename the module"));
          expect(steered.turnId).toBe(turn.turnId);
          expect(h.aether.calls.respond.at(-1)).toMatchObject({
            message: "Also rename the module",
          });
          expect(h.aether.calls.respond.at(-1)).not.toHaveProperty("queue_for_later");
          // Aether gives the steer an Aether turn of its own.
          const [original, steer] = h.aether.turns;
          expect(steer).toMatchObject({
            state: "queued",
            root: { content: "Also rename the module" },
          });

          // The agent reads the steer: the original turn settles, and the steer's turn runs.
          h.aether.hooks.onTurn = (next) => h.aether.say(next, "text-steer", "Renamed it.");
          h.aether.close("text-live");
          h.aether.settle(original!);
          const steerRow = yield* waitFor("the steer's rows", () =>
            events.find((event) => event.itemId === "text-steer"),
          );
          expect(steerRow).toMatchObject({ turnId: turn.turnId });
          yield* Effect.sleep("1500 millis");
          expect(completions(events)).toEqual([]);

          h.workspace.serve(diffOf(h.baseCommit, added("renamed.txt", "renamed\n")));
          h.aether.settle(steer!);
          const completed = yield* waitFor("turn.completed", () => completions(events)[0]);
          expect(completed).toMatchObject({ turnId: turn.turnId, payload: { state: "completed" } });
          // Mirrored when the steer's run settled, before the turn completed.
          expect(yield* h.read("renamed.txt")).toBe("renamed\n");
          expect(events.indexOf(completed)).toBeGreaterThan(events.indexOf(steerRow));
          expect(events.some((event) => event.type === "runtime.warning")).toBe(false);
          yield* Effect.sleep("1500 millis");
          expect(completions(events)).toHaveLength(1);
        }),
    );

    it.effect.each(["did not land", "landed"] as const)(
      "completes the turn with its own run when a steer into it %s and lost its response",
      (outcome) =>
        Effect.gen(function* () {
          const h = yield* makeHarness;
          const { adapter, events } = yield* h.launch();
          yield* h.start(adapter);
          h.aether.hooks.onTurn = (turn) =>
            h.aether.say(turn, "text-live", "Working", { open: true });
          const turn = yield* adapter.sendTurn(h.send("Refactor"));
          yield* waitFor("streamed text", () =>
            events.find((event) => event.type === "content.delta"),
          );

          if (outcome === "landed") {
            h.aether.faults.loseRespond = true;
          } else {
            h.aether.faults.refuseRespond = new AetherApiHttpError({
              endpoint: "POST /tasks/{taskID}/respond",
              status: 503,
            });
          }
          const lost = yield* adapter.steerTurn!(h.send("Also rename the module")).pipe(
            Effect.flip,
          );
          expect(lost).toMatchObject({ _tag: "ProviderAdapterRequestError" });

          const [original] = h.aether.turns;
          h.aether.hooks.onTurn = (steer) => {
            h.aether.say(steer, "text-steer", "Renamed it.");
            h.aether.settle(steer);
          };
          h.aether.close("text-live");
          h.aether.settle(original!);
          const completed = yield* waitFor("turn.completed", () => completions(events)[0]);
          expect(completed).toMatchObject({ turnId: turn.turnId, payload: { state: "completed" } });
          yield* Effect.sleep("1500 millis");
          expect(completions(events)).toHaveLength(1);
          // A steer that landed without Synara knowing is a turn from outside Synara.
          expect(events.some((event) => event.type === "runtime.warning")).toBe(
            outcome === "landed",
          );
        }),
    );

    it.effect(
      "steers a run the loop has not listed yet, and a stop after it waits for its settle",
      () =>
        Effect.gen(function* () {
          const h = yield* makeHarness;
          const { adapter, events } = yield* h.launch();
          yield* h.start(adapter);
          // The poll loop cannot tick: the steer comes before it lists the turn just started.
          const gate = yield* Latch.make(false);
          h.aether.faults.liveGate = gate;
          h.aether.hooks.onTurn = (turn) =>
            h.aether.say(turn, "text-live", "Working", { open: true });
          const turn = yield* adapter.sendTurn(h.send("Refactor"));
          const steered = yield* adapter.steerTurn!(h.send("Also rename the module"));
          expect(steered.turnId).toBe(turn.turnId);
          expect(h.aether.calls.respond.at(-1)).not.toHaveProperty("queue_for_later");
          expect(h.aether.turns.map((aetherTurn) => aetherTurn.state)).toEqual([
            "running",
            "queued",
          ]);

          // A stop before the agent reads the steer discards it, and waits for the running run
          // to settle and be mirrored.
          yield* gate.open;
          h.workspace.serve(diffOf(h.baseCommit, added("partial.txt", "made before stop\n")));
          yield* adapter.interruptTurn(THREAD_ID, turn.turnId);
          let mirroredFirst = false;
          const completed = yield* waitFor("turn.completed", () => {
            const first = completions(events)[0];
            if (first !== undefined) {
              mirroredFirst = existsSync(path.join(h.worktree, "partial.txt"));
            }
            return first;
          });
          expect(completed).toMatchObject({
            turnId: turn.turnId,
            payload: { state: "interrupted" },
          });
          expect(mirroredFirst).toBe(true);
          yield* Effect.sleep("1500 millis");
          expect(completions(events)).toHaveLength(1);
        }),
    );

    it.effect("runs a steer that arrives after the run settled as the same Synara turn", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter, events } = yield* h.launch();
        yield* h.start(adapter);
        let running: FakeTurn | undefined;
        h.aether.hooks.onTurn = (turn) => {
          running = turn;
          h.aether.say(turn, "text-live", "Working", { open: true });
        };
        const turn = yield* adapter.sendTurn(h.send("Refactor"));
        yield* waitFor("streamed text", () =>
          events.find((event) => event.type === "content.delta"),
        );

        // Aether settles the run; Synara has not seen it yet when the user steers.
        h.aether.close("text-live");
        h.workspace.serve(diffOf(h.baseCommit, added("steered.txt", "late\n")));
        h.aether.hooks.onTurn = (late) => {
          h.aether.say(late, "text-late", "Renamed it.");
          h.aether.settle(late);
        };
        h.aether.settle(running!);
        const steered = yield* adapter.steerTurn!(h.send("Also rename the module"));
        expect(steered.turnId).toBe(turn.turnId);
        // Aether started a new turn with the steer.
        expect(h.aether.turns).toHaveLength(2);

        yield* waitFor("the steered run", () =>
          events.find((event) => event.itemId === "text-late"),
        );
        const completed = yield* waitFor("turn.completed", () => completions(events)[0]);
        expect(completed).toMatchObject({ turnId: turn.turnId, payload: { state: "completed" } });
        // Completed after the steered run, with its changes mirrored.
        expect(events.indexOf(completed)).toBeGreaterThan(
          events.findIndex((event) => event.itemId === "text-late"),
        );
        expect(yield* h.read("steered.txt")).toBe("late\n");
        expect(events.find((event) => event.itemId === "text-late")).toMatchObject({
          turnId: turn.turnId,
        });
        expect(events.filter((event) => event.type === "runtime.warning")).toEqual([]);
        yield* Effect.sleep("1500 millis");
        expect(completions(events)).toHaveLength(1);
      }),
    );
    it.effect("completes the turn with its own run when a late steer fails before it lands", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter, events } = yield* h.launch();
        yield* h.start(adapter);
        let running: FakeTurn | undefined;
        h.aether.hooks.onTurn = (turn) => {
          running = turn;
          h.aether.say(turn, "text-live", "Working", { open: true });
        };
        const turn = yield* adapter.sendTurn(h.send("Refactor"));
        yield* waitFor("streamed text", () =>
          events.find((event) => event.type === "content.delta"),
        );

        // Aether settles the run; the steer that follows fails with nothing sent.
        h.aether.close("text-live");
        h.aether.settle(running!);
        h.aether.faults.refuseRespond = new AetherApiHttpError({
          endpoint: "POST /tasks/{taskID}/respond",
          status: 503,
        });
        const lost = yield* adapter.steerTurn!(h.send("Also rename the module")).pipe(Effect.flip);
        expect(lost).toMatchObject({ _tag: "ProviderAdapterRequestError" });

        const completed = yield* waitFor("turn.completed", () => completions(events)[0]);
        expect(completed).toMatchObject({ turnId: turn.turnId, payload: { state: "completed" } });
        yield* Effect.sleep("1500 millis");
        expect(completions(events)).toHaveLength(1);
      }),
    );

    it.effect(
      "binds a late steer sent again after its response was lost to the turn it started",
      () =>
        Effect.gen(function* () {
          const h = yield* makeHarness;
          const { adapter, events } = yield* h.launch();
          yield* h.start(adapter);
          let running: FakeTurn | undefined;
          h.aether.hooks.onTurn = (turn) => {
            running = turn;
            h.aether.say(turn, "text-live", "Working", { open: true });
          };
          const turn = yield* adapter.sendTurn(h.send("Refactor"));
          yield* waitFor("streamed text", () =>
            events.find((event) => event.type === "content.delta"),
          );

          // The loop cannot tick: Synara sees nothing of what follows until the gate opens.
          const gate = yield* Latch.make(false);
          h.aether.faults.liveGate = gate;
          h.aether.close("text-live");
          h.workspace.serve(diffOf(h.baseCommit, added("steered.txt", "late\n")));
          h.aether.hooks.onTurn = (late) => {
            h.aether.say(late, "text-late", "Renamed it.");
            h.aether.settle(late);
          };
          h.aether.settle(running!);
          // The steer lands as a new turn, but its response is lost; the user sends it again.
          h.aether.faults.loseRespond = true;
          const steer = h.send("Also rename the module");
          yield* adapter.steerTurn!(steer).pipe(Effect.flip);
          const steered = yield* adapter.steerTurn!(steer);
          expect(steered.turnId).toBe(turn.turnId);
          expect(h.aether.turns).toHaveLength(2);
          h.aether.faults.liveGate = undefined;
          yield* gate.open;

          const completed = yield* waitFor("turn.completed", () => completions(events)[0]);
          expect(completed).toMatchObject({ turnId: turn.turnId, payload: { state: "completed" } });
          const late = events.find((event) => event.itemId === "text-late");
          expect(late).toMatchObject({ turnId: turn.turnId });
          expect(events.indexOf(completed)).toBeGreaterThan(events.indexOf(late!));
          expect(yield* h.read("steered.txt")).toBe("late\n");
          expect(events.filter((event) => event.type === "runtime.warning")).toEqual([]);
          yield* Effect.sleep("1500 millis");
          expect(completions(events)).toHaveLength(1);
        }),
    );
  });

  describe("interrupt", () => {
    it.effect("stops the task, surfaces discarded queued text, and completes as interrupted", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter, events } = yield* h.launch();
        yield* h.start(adapter);
        h.aether.hooks.onTurn = (turn) =>
          h.aether.say(turn, "text-live", "Working", { open: true });
        const turn = yield* adapter.sendTurn(h.send("Refactor"));
        yield* waitFor("streamed text", () =>
          events.find((event) => event.type === "content.delta"),
        );
        h.aether.remoteTurn("Also update the docs", true);

        yield* adapter.interruptTurn(THREAD_ID, turn.turnId);
        expect(h.aether.calls.stop).toEqual([{ discard_queued_messages: true }]);
        const discarded = yield* waitFor("the discarded messages", () =>
          events.find(
            (event) =>
              event.type === "runtime.warning" && event.payload.message.startsWith("Stopping"),
          ),
        );
        expect(discarded).toMatchObject({
          turnId: turn.turnId,
          payload: {
            message: "Stopping the Aether task discarded 1 queued message.",
            detail: ["Also update the docs"],
          },
        });
        const completed = yield* waitFor("turn.completed", () => completions(events)[0]);
        expect(completed).toMatchObject({ turnId: turn.turnId, payload: { state: "interrupted" } });
        yield* Effect.sleep("1500 millis");
        expect(completions(events)).toHaveLength(1);
      }),
    );

    it.effect("completes a queued turn at once when a stop discards it", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter, events } = yield* h.launch();
        yield* h.start(adapter);
        h.finishTurns();
        yield* adapter.sendTurn(h.send("First"));
        yield* waitFor("the first turn", () => completions(events)[0]);
        // A message sent from Aether's own UI keeps the task busy, so the next one queues.
        h.aether.hooks.onTurn = (turn) => h.aether.say(turn, "text-remote", "Busy", { open: true });
        h.aether.remoteTurn("Remote work", false);
        const queued = yield* adapter.sendTurn(h.send("Queued behind it"));
        expect(h.aether.turns.at(-1)?.state).toBe("queued");

        yield* adapter.interruptTurn(THREAD_ID, queued.turnId);
        const completed = yield* waitFor("turn.completed", () => completions(events)[1]);
        expect(completed).toMatchObject({
          turnId: queued.turnId,
          payload: { state: "interrupted" },
        });
        expect(
          events.find(
            (event) =>
              event.type === "runtime.warning" && event.payload.message.startsWith("Stopping"),
          ),
        ).toMatchObject({ payload: { detail: ["Queued behind it"] } });
        yield* Effect.sleep("1500 millis");
        expect(completions(events)).toHaveLength(2);
      }),
    );

    it.effect(
      "completes an interrupted turn through ProviderService, after the mirror, with no reconciler settle",
      () =>
        Effect.gen(function* () {
          const h = yield* makeHarness;
          const { adapter } = yield* h.makeAdapter();
          yield* Effect.gen(function* () {
            const providerService = yield* ProviderService;
            const directory = yield* ProviderSessionDirectory;
            // What ProviderService accepts and publishes, after its generation checks.
            const accepted: ProviderRuntimeEvent[] = [];
            yield* Stream.runForEach(providerService.streamEvents, (event) =>
              Effect.sync(() => void accepted.push(event)),
            ).pipe(Effect.forkScoped);

            yield* providerService.startSession(THREAD_ID, {
              threadId: THREAD_ID,
              provider: "aether",
              cwd: h.worktree,
              modelSelection: { provider: "aether", model: MODEL },
              runtimeMode: "full-access",
            });
            h.aether.hooks.onTurn = (turn) =>
              h.aether.say(turn, "text-live", "Working", { open: true });
            const turn = yield* providerService.sendTurn({
              threadId: THREAD_ID,
              input: "Sleep 90",
            });
            yield* waitFor("streamed text", () =>
              accepted.find((event) => event.type === "content.delta"),
            );
            h.workspace.serve(diffOf(h.baseCommit, added("partial.txt", "made before stop\n")));

            yield* providerService.interruptTurn({ threadId: THREAD_ID, turnId: turn.turnId });

            // The live reconciler runs in the gap, on the inputs it gathers.
            const runningSince = new Date(Date.now() - 60_000).toISOString();
            const plans = planProviderRuntimeReconciliation({
              threads: [reconcilableThread(turn.turnId, runningSince)],
              bindings: yield* directory.listBindings(),
              liveSessions: yield* providerService.listSessions(),
              pumpHealth: yield* providerService.getRuntimeEventPumpHealth!(),
              nowMs: Date.now(),
            });
            expect(plans).toEqual([]);

            const completed = yield* waitFor(
              "the accepted turn.completed",
              () => completions(accepted)[0],
            );
            expect(completed).toMatchObject({
              turnId: turn.turnId,
              payload: { state: "interrupted" },
            });
            // Mirrored before the turn completed.
            expect(yield* h.read("partial.txt")).toBe("made before stop\n");
            yield* Effect.sleep("1500 millis");
            expect(completions(accepted)).toHaveLength(1);
            const [binding] = yield* directory.listBindings();
            expect(binding?.runtimePayload).toMatchObject({ activeTurnId: null });
          }).pipe(
            Effect.scoped,
            Effect.provide(
              Layer.mergeAll(
                providerServiceOver(adapter).pipe(Layer.provide(sessionDirectoryLayer)),
                sessionDirectoryLayer,
              ),
            ),
          );
        }),
    );

    it.effect.each(["graceful", "crash"] as const)(
      "can still stop a turn reattached after a %s restart, which completes after the mirror",
      (restart) =>
        Effect.gen(function* () {
          const h = yield* makeHarness;
          yield* Effect.gen(function* () {
            const directory = yield* ProviderSessionDirectory;

            // The first process sends a turn that is still running when it goes away.
            const first = yield* h.makeAdapter();
            const firstServices = yield* Scope.make();
            const before = yield* launchProviderService(first.adapter, firstServices);
            yield* before.startSession(THREAD_ID, {
              threadId: THREAD_ID,
              provider: "aether",
              cwd: h.worktree,
              modelSelection: { provider: "aether", model: MODEL },
              runtimeMode: "full-access",
            });
            h.aether.hooks.onTurn = (turn) =>
              h.aether.say(turn, "text-live", "Working", { open: true });
            const turn = yield* before.sendTurn({ threadId: THREAD_ID, input: "Sleep 90" });
            yield* waitFor("the running turn", () => h.aether.turns[0]);
            // A graceful stop marks every binding stopped with no active turn; a crash leaves it.
            if (restart === "graceful") yield* Scope.close(firstServices, Exit.void);
            yield* Scope.close(first.scope, Exit.void);
            const [binding] = yield* directory.listBindings();
            expect(binding?.runtimePayload).toMatchObject({
              activeTurnId: restart === "graceful" ? null : turn.turnId,
            });

            // The second process reattaches the turn, and the user stops it.
            const second = yield* h.makeAdapter([binding!]);
            const after = yield* launchProviderService(second.adapter, yield* Scope.make());
            const accepted: ProviderRuntimeEvent[] = [];
            yield* Stream.runForEach(after.streamEvents, (event) =>
              Effect.sync(() => void accepted.push(event)),
            ).pipe(Effect.forkScoped);
            h.workspace.serve(diffOf(h.baseCommit, added("partial.txt", "made before stop\n")));
            expect(
              yield* after.reattachTurn!({
                threadId: THREAD_ID,
                provider: "aether",
                turnId: turn.turnId,
                runtimeMode: "full-access",
              }),
            ).toBe(true);
            yield* after.interruptTurn({ threadId: THREAD_ID, turnId: turn.turnId });

            const completed = yield* waitFor(
              "the accepted turn.completed",
              () => completions(accepted)[0],
            );
            expect(completed).toMatchObject({
              turnId: turn.turnId,
              payload: { state: "interrupted" },
            });
            expect(yield* h.read("partial.txt")).toBe("made before stop\n");
            yield* Effect.sleep("1500 millis");
            expect(completions(accepted)).toHaveLength(1);
          }).pipe(Effect.scoped, Effect.provide(sessionDirectoryLayer));
        }),
    );
  });

  describe("the mirror", () => {
    it.effect(
      "pauses on local edits with a loud error naming the files, and overwrites nothing",
      () =>
        Effect.gen(function* () {
          const h = yield* makeHarness;
          const { adapter, events } = yield* h.launch();
          yield* h.start(adapter);
          h.finishTurns();
          h.workspace.serve(
            diffOf(h.baseCommit, modified("app.txt", hunk(1, 1, " one", "-two", "+TWO"))),
          );
          yield* adapter.sendTurn(h.send("Edit"));
          yield* waitFor("the first turn", () => completions(events)[0]);
          expect(yield* h.read("app.txt")).toBe("one\nTWO\n");

          yield* h.write("app.txt", "my local edit\n");
          h.workspace.serve(
            diffOf(h.baseCommit, modified("app.txt", hunk(1, 1, " one", "-two", "+THREE"))),
          );
          const second = yield* adapter.sendTurn(h.send("Edit again"));
          const completed = yield* waitFor("the second turn", () => completions(events)[1]);
          expect(completed).toMatchObject({ turnId: second.turnId, payload: { state: "failed" } });
          expect(completed.type === "turn.completed" && completed.payload.errorMessage).toContain(
            "the worktree has local changes (app.txt)",
          );
          expect(yield* h.read("app.txt")).toBe("my local edit\n");
        }),
    );

    it.effect.each([
      [
        "an API error opening the workspace",
        (h: Harness) => {
          h.vm.failure = new AetherApiHttpError({
            endpoint: "POST /workspaces/{workspaceID}/connect",
            status: 401,
            serverMessage: "Invalid API key.",
          });
        },
        "HTTP 401",
      ],
      [
        "a dropped workspace socket",
        (h: Harness) => h.workspace.failDiff("socket closed"),
        "(transport): git.diff: socket closed",
      ],
      [
        "a file too large to mirror",
        (h: Harness) => h.workspace.serve(diffOf(h.baseCommit, tooLarge("video.mp4", 100))),
        "(too-large): video.mp4 exceeds 100 bytes",
      ],
      [
        "a path Git refuses",
        (h: Harness) => h.workspace.serve(diffOf(h.baseCommit, added(".git/hooks/x", "boom\n"))),
        "(path-escape)",
      ],
      [
        "a task moved off its base",
        (h: Harness) => h.workspace.serve(diffOf("0".repeat(40), added("feature.txt", "hi\n"))),
        "(base-mismatch): The workspace diff is from 0000000000000000000000000000000000000000",
      ],
    ] as const)("fails the turn loudly on %s", ([, arrange, expected]) =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter, events } = yield* h.launch();
        yield* h.start(adapter);
        h.finishTurns();
        arrange(h);
        const turn = yield* adapter.sendTurn(h.send("Edit"));
        const completed = yield* waitFor("turn.completed", () => completions(events)[0]);
        expect(completed).toMatchObject({ turnId: turn.turnId, payload: { state: "failed" } });
        expect(completed.type === "turn.completed" && completed.payload.errorMessage).toContain(
          expected,
        );
        const [session] = yield* adapter.listSessions();
        expect(session?.resumeCursor).toMatchObject({ mirrorPending: false });
      }),
    );

    it.effect("settles while the VM is suspended, and mirrors when the next turn starts", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter, events } = yield* h.launch();
        yield* h.start(adapter);
        h.finishTurns();
        h.workspace.serve(diffOf(h.baseCommit, added("feature.txt", "hello\n")));
        h.vm.running = false;
        const first = yield* adapter.sendTurn(h.send("Add the feature"));
        const completed = yield* waitFor("the first turn", () => completions(events)[0]);
        expect(completed).toMatchObject({ turnId: first.turnId, payload: { state: "completed" } });
        expect(events.find((event) => event.type === "runtime.warning")?.payload).toMatchObject({
          message: expect.stringContaining("tries again when the next turn starts"),
        });
        expect(yield* h.exists("feature.txt")).toBe(false);
        const [suspended] = yield* adapter.listSessions();
        expect(suspended?.resumeCursor).toMatchObject({ mirrorPending: true });

        h.vm.running = true;
        const next = yield* adapter.sendTurn(h.send("Next"));
        // Mirrored before the next turn went out: its checkpoints hold the combined change.
        expect(yield* h.read("feature.txt")).toBe("hello\n");
        expect(next.resumeCursor).toMatchObject({ mirrorPending: false });
        yield* waitFor("the next turn", () => completions(events)[1]);
      }),
    );
  });

  describe("openRunningWorkspace", () => {
    const workspaceId = "0b0e4f7e-5c1a-4d2b-9e3f-6a7b8c9d0e1f" as AetherWorkspaceId;
    const connectRefused = (error: AetherApiError) =>
      openRunningWorkspace({
        ...makeFakeAetherTask().client,
        connectWorkspace: () => Effect.fail(error),
      })(workspaceId).pipe(Effect.scoped);
    const conflict = (body: AetherApiConflictError["body"]) =>
      new AetherApiConflictError({ endpoint: "POST /workspaces/{workspaceID}/connect", body });

    it.effect("treats a workspace that is not running as absent", () =>
      Effect.gen(function* () {
        for (const body of [
          { kind: "transitional", error: "Stopping.", retry_after_ms: 1000 },
          { kind: "startable", error: "Workspace is stopped." },
          { kind: "not_connectable", error: "Workspace is suspended." },
        ] as const) {
          expect(Option.isNone(yield* connectRefused(conflict(body)))).toBe(true);
        }
      }),
    );

    it.effect("fails on any other API error", () =>
      Effect.gen(function* () {
        for (const error of [
          conflict({ error: "The task's creator was deleted.", code: "task_creator_deleted" }),
          new AetherApiHttpError({ endpoint: "POST /workspaces/{id}/connect", status: 401 }),
          new AetherApiHttpError({ endpoint: "POST /workspaces/{id}/connect", status: 404 }),
          new AetherApiHttpError({ endpoint: "POST /workspaces/{id}/connect", status: 500 }),
        ]) {
          expect(yield* connectRefused(error).pipe(Effect.flip)).toBe(error);
        }
      }),
    );
  });

  describe("restarts and ownership", () => {
    it.effect(
      "reattaches mid-turn after a restart and settles the turn once, without repeats",
      () =>
        Effect.gen(function* () {
          const h = yield* makeHarness;
          const first = yield* h.launch();
          yield* h.start(first.adapter);
          let running: FakeTurn | undefined;
          h.aether.hooks.onTurn = (turn) => {
            running = turn;
            h.aether.tool(turn, "toolu_1", "ls");
            h.aether.say(turn, "text-live", "Hello", { open: true });
          };
          const turn = yield* first.adapter.sendTurn(h.send("Greet"));
          yield* waitFor("the streamed text", () =>
            first.events.find((event) => event.type === "content.delta"),
          );
          const [session] = yield* first.adapter.listSessions();
          yield* first.stop;

          // While Synara is down the message finishes and the turn settles.
          h.aether.say(running!, "text-live", "Hello, world", { open: true });
          h.aether.close("text-live");
          h.workspace.serve(diffOf(h.baseCommit, added("greeting.txt", "hi\n")));
          h.aether.settle(running!);

          const binding: ProviderRuntimeBinding = {
            threadId: THREAD_ID,
            provider: "aether",
            runtimeMode: "full-access",
            status: "stopped",
            resumeCursor: session!.resumeCursor,
          };
          const second = yield* h.launch([binding]);
          // Startup reconciliation reattaches the turn in flight.
          yield* second.adapter.reattachTurn!({
            threadId: THREAD_ID,
            turnId: turn.turnId,
            resumeCursor: binding.resumeCursor,
            runtimeMode: "full-access",
            lifecycleGeneration: "generation-boot",
          });
          expect(yield* second.adapter.listSessions()).toMatchObject([
            { status: "running", activeTurnId: turn.turnId },
          ]);
          const completed = yield* waitFor("turn.completed", () => completions(second.events)[0]);
          expect(completed).toMatchObject({
            turnId: turn.turnId,
            lifecycleGeneration: "generation-boot",
            payload: { state: "completed" },
          });
          expect(yield* h.read("greeting.txt")).toBe("hi\n");

          const all = [...first.events, ...second.events];
          expect(completions(all)).toHaveLength(1);
          // Nothing the first process emitted is emitted again, and the text does not repeat.
          const firstIds = new Set(first.events.map((event) => event.eventId));
          expect(second.events.filter((event) => firstIds.has(event.eventId))).toEqual([]);
          const streamed = all.flatMap((event) =>
            event.type === "content.delta" ? [event.payload.delta] : [],
          );
          expect(streamed.join("")).toBe("Hello, world");
        }),
    );

    it.effect(
      "keeps a turn running through a desktop quit, and follows it again at relaunch with no continuation",
      () =>
        Effect.gen(function* () {
          const h = yield* makeHarness;
          const recordPath = path.join(h.root, "quit-resume.json");
          yield* Effect.gen(function* () {
            const directory = yield* ProviderSessionDirectory;
            const first = yield* h.makeAdapter();
            const firstServices = yield* Scope.make();
            const before = yield* launchProviderService(first.adapter, firstServices);
            yield* before.startSession(THREAD_ID, {
              threadId: THREAD_ID,
              provider: "aether",
              cwd: h.worktree,
              modelSelection: { provider: "aether", model: MODEL },
              runtimeMode: "full-access",
            });
            h.aether.hooks.onTurn = (turn) =>
              h.aether.say(turn, "text-live", "Working", { open: true });
            const turn = yield* before.sendTurn({ threadId: THREAD_ID, input: "Sleep 90" });
            yield* waitFor("the running turn", () => h.aether.turns[0]);

            // The user quits with "Resume chats automatically": the renderer hands the server the
            // running chats, which it records and interrupts.
            const dispatched: OrchestrationCommand[] = [];
            const prepared = yield* prepareQuitResume({
              request: {
                threadIds: [THREAD_ID],
                continuationPrompt:
                  "Synara was closed while this chat was still running. Continue where you left off.",
              },
              recordPath,
              getReadModel: () =>
                Effect.succeed({
                  threads: [
                    {
                      ...reconcilableThread(turn.turnId, new Date().toISOString()),
                      deletedAt: null,
                    },
                  ],
                }),
              dispatch: (command) => Effect.sync(() => void dispatched.push(command)),
              abandonAfter: Duration.hours(1),
            });
            expect(prepared.recordedThreadIds).toEqual([]);
            // The desktop then stops the server, which only stops following the task.
            yield* Scope.close(firstServices, Exit.void);
            yield* Scope.close(first.scope, Exit.void);
            expect(dispatched).toEqual([]);
            expect(h.aether.calls.stop).toEqual([]);

            // The turn finishes in the cloud while Synara is closed.
            h.aether.close("text-live");
            h.workspace.serve(diffOf(h.baseCommit, added("done.txt", "done while closed\n")));
            h.aether.settle(h.aether.turns[0]!);

            // Relaunch: the record holds no chat to continue, and startup reconciliation follows
            // the turn again, which completes once, after the mirror.
            expect(yield* claimQuitResumeRecord(recordPath)).toMatchObject({
              kind: "record",
              record: { threads: [] },
            });
            const [binding] = yield* directory.listBindings();
            const second = yield* h.makeAdapter([binding!]);
            const after = yield* launchProviderService(second.adapter, yield* Scope.make());
            const accepted: ProviderRuntimeEvent[] = [];
            yield* Stream.runForEach(after.streamEvents, (event) =>
              Effect.sync(() => void accepted.push(event)),
            ).pipe(Effect.forkScoped);
            expect(
              yield* after.reattachTurn!({
                threadId: THREAD_ID,
                provider: "aether",
                turnId: turn.turnId,
                runtimeMode: "full-access",
              }),
            ).toBe(true);
            const completed = yield* waitFor(
              "the accepted turn.completed",
              () => completions(accepted)[0],
            );
            expect(completed).toMatchObject({
              turnId: turn.turnId,
              payload: { state: "completed" },
            });
            expect(yield* h.read("done.txt")).toBe("done while closed\n");
            yield* Effect.sleep("1500 millis");
            expect(completions(accepted)).toHaveLength(1);
            expect(h.aether.calls.stop).toEqual([]);
            expect(h.aether.calls.respond).toEqual([]);
          }).pipe(Effect.scoped, Effect.provide(sessionDirectoryLayer));
        }),
    );

    it.effect(
      "fails a reattach loudly with its cause: a worktree gone, or a task never confirmed",
      () =>
        Effect.gen(function* () {
          const h = yield* makeHarness;
          const first = yield* h.launch();
          const creating = yield* h.start(first.adapter);
          h.aether.hooks.onTurn = (turn) => h.aether.say(turn, "text-live", "Hi", { open: true });
          const turn = yield* first.adapter.sendTurn(h.send("Greet"));
          const [running] = yield* first.adapter.listSessions();
          yield* first.stop;

          // Synara stopped before it persisted the created cursor.
          const second = yield* h.launch();
          const unconfirmed = yield* second.adapter.reattachTurn!({
            threadId: THREAD_ID,
            turnId: turn.turnId,
            resumeCursor: creating.resumeCursor,
            runtimeMode: "full-access",
            lifecycleGeneration: undefined,
          }).pipe(Effect.flip);
          expect(unconfirmed.message).toContain(
            "Synara stopped before Aether confirmed creating the task for this thread's first message. Send that message again",
          );
          yield* second.stop;

          // The worktree was removed while Synara was down.
          yield* git(h.repo, ["worktree", "remove", "--force", h.worktree]);
          const third = yield* h.launch();
          const gone = yield* third.adapter.reattachTurn!({
            threadId: THREAD_ID,
            turnId: turn.turnId,
            resumeCursor: running!.resumeCursor,
            runtimeMode: "full-access",
            lifecycleGeneration: undefined,
          }).pipe(Effect.flip);
          expect(gone.message).toContain(h.worktree);
          expect(yield* third.adapter.hasSession(THREAD_ID)).toBe(false);
        }),
    );

    it.effect("reattaches after an idle stop, re-emitting no history the journal pruned", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter, events } = yield* h.launch();
        yield* h.start(adapter);
        h.aether.hooks.onTurn = (turn) => {
          h.aether.tool(turn, `toolu-${turn.id}`, "ls");
          h.aether.say(turn, `text-${turn.id}`, "Done.");
          h.aether.settle(turn);
        };
        yield* adapter.sendTurn(h.send("First"));
        yield* waitFor("the first turn", () => completions(events)[0]);
        yield* adapter.sendTurn(h.send("Second"));
        yield* waitFor("the second turn", () => completions(events)[1]);

        // The idle reaper stops the session; retention drops the settled turns' rows.
        const [idle] = yield* adapter.listSessions();
        yield* adapter.stopSession(THREAD_ID);
        h.pruneCompletedTurns();
        expect(h.journal).toEqual([]);
        const before = events.length;
        // A reattach reports the branch and pull request again: a report, not history.
        const history = (from: number) =>
          events.slice(from).filter((event) => event.type !== "vcs.state.changed");
        const resumed = yield* adapter.startSession({
          threadId: THREAD_ID,
          provider: "aether",
          cwd: h.worktree,
          resumeCursor: idle!.resumeCursor,
          runtimeMode: "full-access",
        });
        expect(resumed).toMatchObject({ status: "ready" });
        yield* Effect.sleep("1500 millis");
        expect(history(before)).toEqual([]);

        const third = yield* adapter.sendTurn(h.send("Third"));
        yield* waitFor("the third turn", () => completions(events)[2]);
        expect(history(before).every((event) => event.turnId === third.turnId)).toBe(true);
      }),
    );

    it.effect("holds the worktree across stopSession, and claims it again at boot", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const gitCore = yield* GitCore;
        const first = yield* h.launch();
        yield* h.start(first.adapter);
        h.finishTurns();
        yield* first.adapter.sendTurn(h.send("Go"));
        yield* waitFor("turn.completed", () => completions(first.events)[0]);
        const [session] = yield* first.adapter.listSessions();

        yield* first.adapter.stopSession(THREAD_ID);
        yield* first.adapter.stopSession(THREAD_ID);
        expect(yield* first.adapter.hasSession(THREAD_ID)).toBe(false);
        const owned = yield* gitCore.assertWorktreeNotOwned(h.worktree).pipe(Effect.flip);
        expect(owned._tag).toBe("WorktreeOwnedError");

        yield* first.stop;
        yield* gitCore.assertWorktreeNotOwned(h.worktree);

        const second = yield* h.launch([
          {
            threadId: THREAD_ID,
            provider: "aether",
            runtimeMode: "full-access",
            status: "stopped",
            resumeCursor: session!.resumeCursor,
          },
        ]);
        expect(yield* second.adapter.hasSession(THREAD_ID)).toBe(false);
        const reclaimed = yield* gitCore.assertWorktreeNotOwned(h.worktree).pipe(Effect.flip);
        expect(reclaimed._tag).toBe("WorktreeOwnedError");
      }),
    );

    it.effect("reattaches a turn it already follows on its session, and refuses any other", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter, events } = yield* h.launch();
        yield* h.start(adapter);
        let running: FakeTurn | undefined;
        h.aether.hooks.onTurn = (turn) => {
          running = turn;
        };
        const turn = yield* adapter.sendTurn(h.send("Go"));
        const reattach = (turnId: TurnId) =>
          adapter.reattachTurn!({
            threadId: THREAD_ID,
            turnId,
            resumeCursor: turn.resumeCursor,
            runtimeMode: "full-access",
            lifecycleGeneration: undefined,
          });
        yield* reattach(turn.turnId);
        const other = yield* reattach(TurnId.makeUnsafe("turn-other")).pipe(Effect.flip);
        expect(other.message).toContain("Synara already follows this Aether task");

        // One poll loop follows the task: the turn completes once.
        h.aether.say(running!, "text-done", "Done.");
        h.aether.settle(running!);
        yield* waitFor("turn.completed", () => completions(events)[0]);
        yield* Effect.sleep("1500 millis");
        expect(completions(events)).toHaveLength(1);
      }),
    );

    it.effect.each([
      "deleted",
      "deleted, whose first stop fails,",
      "archived",
      "archived, whose stop fails,",
    ] as const)("stops the turn in flight of a thread %s and releases its worktree", (retired) =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const gitCore = yield* GitCore;
        const { adapter } = yield* h.makeAdapter();
        h.aether.hooks.onTurn = (turn) =>
          h.aether.say(turn, "text-live", "Working", { open: true });
        yield* Effect.gen(function* () {
          const providerService = yield* ProviderService;
          yield* providerService.startSession(THREAD_ID, {
            threadId: THREAD_ID,
            provider: "aether",
            cwd: h.worktree,
            modelSelection: { provider: "aether", model: MODEL },
            runtimeMode: "full-access",
          });
          yield* providerService.sendTurn({ threadId: THREAD_ID, input: "Go" });
          yield* waitFor("the running turn", () => h.aether.turns[0]);
          const refuseStop = () => {
            h.aether.faults.refuseStop = new AetherApiHttpError({
              endpoint: "POST /tasks/{taskID}/stop",
              status: 503,
            });
          };
          // Deleting a thread stops its session and removes its binding; archiving it stops
          // its runtime, keeping the binding, then releases what the thread held.
          if (retired === "deleted") {
            yield* providerService.stopSession({ threadId: THREAD_ID });
          } else if (retired === "deleted, whose first stop fails,") {
            refuseStop();
            const failed = yield* providerService
              .stopSession({ threadId: THREAD_ID })
              .pipe(Effect.flip);
            expect(failed).toMatchObject({ _tag: "ProviderAdapterRequestError" });
            // The binding, whose cursor names the turn in flight, stays for the retry.
            const binding = yield* (yield* ProviderSessionDirectory).getBinding(THREAD_ID);
            expect(Option.getOrUndefined(binding)?.resumeCursor).toMatchObject({ active: true });
            yield* providerService.stopSession({ threadId: THREAD_ID });
            expect(yield* (yield* ProviderSessionDirectory).getBinding(THREAD_ID)).toEqual(
              Option.none(),
            );
          } else {
            yield* providerService.stopRuntimeSession!({ threadId: THREAD_ID });
            if (retired === "archived") {
              yield* providerService.releaseThread!({ threadId: THREAD_ID });
            } else {
              refuseStop();
              const failed = yield* providerService.releaseThread!({ threadId: THREAD_ID }).pipe(
                Effect.flip,
              );
              expect(failed).toMatchObject({
                _tag: "ProviderAdapterRequestError",
                method: "stopTask",
              });
            }
          }
        }).pipe(
          Effect.scoped,
          Effect.provide(
            Layer.mergeAll(
              providerServiceOver(adapter).pipe(Layer.provide(sessionDirectoryLayer)),
              sessionDirectoryLayer,
            ),
          ),
        );

        const stops = retired === "deleted, whose first stop fails," ? 2 : 1;
        expect(h.aether.calls.stop).toEqual(
          Array.from({ length: stops }, () => ({ discard_queued_messages: true })),
        );
        yield* gitCore.removeWorktree({ cwd: h.repo, path: h.worktree });
        expect(yield* h.exists("app.txt")).toBe(false);
      }),
    );

    it.effect(
      "stops an archived turn again when the thread is unarchived, until a stop lands",
      () =>
        Effect.gen(function* () {
          const h = yield* makeHarness;
          yield* Effect.gen(function* () {
            const directory = yield* ProviderSessionDirectory;
            const cursor = directory
              .getBinding(THREAD_ID)
              .pipe(Effect.map((binding) => Option.getOrUndefined(binding)?.resumeCursor));
            const { adapter } = yield* h.makeAdapter([], { keepCursor: keepCursorIn(directory) });
            const providerService = yield* launchProviderService(adapter, yield* Scope.make());
            const accepted: ProviderRuntimeEvent[] = [];
            yield* Stream.runForEach(providerService.streamEvents, (event) =>
              Effect.sync(() => void accepted.push(event)),
            ).pipe(Effect.forkScoped);
            const start = providerService.startSession(THREAD_ID, {
              threadId: THREAD_ID,
              provider: "aether",
              cwd: h.worktree,
              modelSelection: { provider: "aether", model: MODEL },
              runtimeMode: "full-access",
            });
            yield* start;
            h.aether.hooks.onTurn = (turn) =>
              h.aether.say(turn, "text-live", "Working", { open: true });
            const archived = yield* providerService.sendTurn({ threadId: THREAD_ID, input: "Go" });
            yield* waitFor("the running turn", () => h.aether.turns[0]);

            // Archiving stops the runtime, then the turn; that stop fails, so the run goes on and the
            // cursor keeps the turn in flight.
            yield* providerService.stopRuntimeSession!({ threadId: THREAD_ID });
            h.aether.faults.refuseStop = new AetherApiHttpError({
              endpoint: "POST /tasks/{taskID}/stop",
              status: 503,
            });
            yield* providerService.releaseThread!({ threadId: THREAD_ID }).pipe(Effect.flip);
            expect(h.aether.turns[0]!.state).toBe("running");
            expect(yield* cursor).toMatchObject({ active: true });

            // Unarchiving sends it again. Once it lands, the cursor has nothing in flight, so no
            // later release sends it a third time.
            yield* providerService.releaseThread!({ threadId: THREAD_ID });
            expect(h.aether.turns[0]!.state).toBe("settled");
            expect(yield* cursor).toMatchObject({ active: false });
            yield* providerService.releaseThread!({ threadId: THREAD_ID });
            expect(h.aether.calls.stop).toHaveLength(2);

            // The thread resumes with its next message; the stopped turn, which archiving already
            // ended, completes nothing.
            h.finishTurns();
            yield* start;
            const next = yield* providerService.sendTurn({ threadId: THREAD_ID, input: "Next" });
            yield* waitFor("the next turn's completion", () => completions(accepted)[0]);
            yield* Effect.sleep("1500 millis");
            expect(completions(accepted).map((event) => event.turnId)).toEqual([next.turnId]);
            expect(next.turnId).not.toBe(archived.turnId);
          }).pipe(Effect.scoped, Effect.provide(sessionDirectoryLayer));
        }),
    );

    it.effect(
      "stops at boot the turn an archived thread still has in flight, once, before its next session",
      () =>
        Effect.gen(function* () {
          const h = yield* makeHarness;
          // Synara stops while an archived thread's turn runs on: its archive's stop failed.
          const first = yield* h.launch();
          yield* h.start(first.adapter);
          h.aether.hooks.onTurn = (turn) =>
            h.aether.say(turn, "text-live", "Working", { open: true });
          yield* first.adapter.sendTurn(h.send("Go"));
          yield* waitFor("the running turn", () => h.aether.turns[0]);
          const [session] = yield* first.adapter.listSessions();
          yield* first.stop;

          const thread = { archivedAt: "2026-09-25T00:00:00.000Z", deletedAt: null };
          // Aether answers a stop only once the gate opens.
          const gate = yield* Latch.make();
          const boot = AetherAdapterLive.pipe(
            Layer.provide(
              Layer.mergeAll(
                Layer.succeed(AetherApiClient, {
                  ...h.aether.client,
                  stopTask: (taskId, request) =>
                    gate.await.pipe(Effect.andThen(h.aether.client.stopTask(taskId, request))),
                }),
                Layer.succeed(ProjectionThreadRepository, {
                  getById: () => Effect.succeed(Option.some(thread as ProjectionThread)),
                } as unknown as ProjectionThreadRepositoryShape),
                ProviderRuntimeEventRepositoryLive.pipe(Layer.provide(SqlitePersistenceMemory)),
              ),
            ),
          );
          yield* Effect.gen(function* () {
            const directory = yield* ProviderSessionDirectory;
            yield* directory.upsert({
              threadId: THREAD_ID,
              provider: "aether",
              runtimeMode: "full-access",
              status: "stopped",
              resumeCursor: session!.resumeCursor,
            });
            yield* Effect.gen(function* () {
              const adapter = yield* AetherAdapter;
              const events: ProviderRuntimeEvent[] = [];
              yield* Stream.runForEach(adapter.streamEvents, (event) =>
                Effect.sync(() => void events.push(event)),
              ).pipe(Effect.forkScoped);
              // Unarchived and sent to at once: its session waits for the boot's stop, and then
              // follows no turn, though its cursor was read before the stop landed.
              const starting = yield* adapter
                .startSession({
                  threadId: THREAD_ID,
                  provider: "aether",
                  cwd: h.worktree,
                  modelSelection: { provider: "aether", model: MODEL },
                  runtimeMode: "full-access",
                  resumeCursor: session!.resumeCursor,
                })
                .pipe(Effect.forkScoped);
              yield* Effect.sleep("100 millis");
              expect(yield* adapter.hasSession(THREAD_ID)).toBe(false);
              yield* gate.open;
              yield* Fiber.join(starting);
              expect(h.aether.calls.stop).toEqual([{ discard_queued_messages: true }]);
              expect(h.aether.turns[0]!.state).toBe("settled");
              const [binding] = yield* directory.listBindings();
              expect(binding?.resumeCursor).toMatchObject({ active: false });
              expect(yield* adapter.listSessions()).toMatchObject([{ status: "ready" }]);
              yield* Effect.sleep("1500 millis");
              expect(completions(events)).toEqual([]);

              // A later turn is not the one the boot stopped.
              h.aether.hooks.onTurn = (turn) =>
                h.aether.say(turn, "text-next", "Working", { open: true });
              yield* adapter.sendTurn(h.send("Next"));
              yield* waitFor("the next running turn", () => h.aether.turns[1]);
              const [current] = yield* adapter.listSessions();
              // A release handed the cursor read before the boot's stop sends no second stop; one
              // handed the cursor with the later turn in flight stops that turn.
              yield* adapter.releaseThread!({
                threadId: THREAD_ID,
                resumeCursor: session!.resumeCursor,
              });
              expect(h.aether.calls.stop).toHaveLength(1);
              yield* adapter.releaseThread!({
                threadId: THREAD_ID,
                resumeCursor: current!.resumeCursor,
              });
              expect(h.aether.calls.stop).toHaveLength(2);
              expect(h.aether.turns[1]!.state).toBe("settled");
            }).pipe(Effect.scoped, Effect.provide(boot));

            // The next boot finds nothing in flight.
            yield* Effect.gen(function* () {
              yield* AetherAdapter;
              yield* Effect.sleep("100 millis");
            }).pipe(Effect.scoped, Effect.provide(boot));
            expect(h.aether.calls.stop).toHaveLength(2);
          }).pipe(Effect.scoped, Effect.provide(sessionDirectoryLayer));
        }),
    );

    it.effect.each(["stops it as idle", "shuts down"] as const)(
      "leaves the turn in flight running when Synara %s",
      (detach) =>
        Effect.gen(function* () {
          const h = yield* makeHarness;
          const { adapter } = yield* h.makeAdapter();
          h.aether.hooks.onTurn = (turn) =>
            h.aether.say(turn, "text-live", "Working", { open: true });
          yield* Effect.gen(function* () {
            const providerService = yield* ProviderService;
            yield* providerService.startSession(THREAD_ID, {
              threadId: THREAD_ID,
              provider: "aether",
              cwd: h.worktree,
              modelSelection: { provider: "aether", model: MODEL },
              runtimeMode: "full-access",
            });
            yield* providerService.sendTurn({ threadId: THREAD_ID, input: "Go" });
            yield* waitFor("the running turn", () => h.aether.turns[0]);
            // The idle reaper stops the runtime; closing the scope shuts the service down.
            if (detach === "stops it as idle") {
              yield* providerService.stopRuntimeSession!({ threadId: THREAD_ID });
            }
          }).pipe(
            Effect.scoped,
            Effect.provide(providerServiceOver(adapter).pipe(Layer.provide(sessionDirectoryLayer))),
          );

          expect(h.aether.calls.stop).toEqual([]);
          expect(h.aether.turns[0]!.state).toBe("running");
        }),
    );

    it.effect.each([
      ["claims the worktree of a live thread", null],
      ["claims no worktree of an archived thread", "2026-09-25T00:00:00.000Z"],
    ] as const)("%s at boot", ([, archivedAt]) =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const gitCore = yield* GitCore;
        const first = yield* h.launch();
        yield* h.start(first.adapter);
        h.finishTurns();
        yield* first.adapter.sendTurn(h.send("Go"));
        yield* waitFor("turn.completed", () => completions(first.events)[0]);
        const [session] = yield* first.adapter.listSessions();
        yield* first.stop;

        const thread = { archivedAt, deletedAt: null } as ProjectionThread;
        yield* Effect.gen(function* () {
          yield* (yield* ProviderSessionDirectory).upsert({
            threadId: THREAD_ID,
            provider: "aether",
            runtimeMode: "full-access",
            status: "stopped",
            resumeCursor: session!.resumeCursor,
          });
          // The adapter claims at construction; the claim lasts while the layer does.
          const owned = yield* Effect.gen(function* () {
            yield* AetherAdapter;
            return yield* Effect.exit(gitCore.assertWorktreeNotOwned(h.worktree));
          }).pipe(
            Effect.provide(
              AetherAdapterLive.pipe(
                Layer.provide(
                  Layer.mergeAll(
                    Layer.succeed(AetherApiClient, h.aether.client),
                    Layer.succeed(ProjectionThreadRepository, {
                      getById: () => Effect.succeed(Option.some(thread)),
                    } as unknown as ProjectionThreadRepositoryShape),
                    ProviderRuntimeEventRepositoryLive.pipe(Layer.provide(SqlitePersistenceMemory)),
                  ),
                ),
              ),
            ),
          );
          expect(Exit.isFailure(owned)).toBe(archivedAt === null);
        }).pipe(Effect.scoped, Effect.provide(sessionDirectoryLayer));
      }),
    );

    it.effect("renders a turn sent from Aether's own UI as a warning carrying its text", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter, events } = yield* h.launch();
        yield* h.start(adapter);
        h.finishTurns();
        yield* adapter.sendTurn(h.send("Go"));
        yield* waitFor("turn.completed", () => completions(events)[0]);
        h.aether.remoteTurn("Also bump the version", false);
        const warning = yield* waitFor("the warning", () =>
          events.find((event) => event.type === "runtime.warning"),
        );
        expect(warning.payload).toEqual({
          message: "A message reached this Aether task from outside Synara.",
          detail: "Also bump the version",
        });
        yield* Effect.sleep("1500 millis");
        expect(completions(events)).toHaveLength(1);
      }),
    );
  });
});

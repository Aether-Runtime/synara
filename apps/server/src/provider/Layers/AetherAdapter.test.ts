// FILE: AetherAdapter.test.ts
// Purpose: The Aether adapter's lifecycle against a fake Aether API, a fake workspace socket and a
// real Git worktree whose origin is a local bare repository: task creation, turns, settlement
// behind the mirror, retries of lost sends, questions, interrupts, restarts and ownership.
// Layer: Server provider tests (Aether).
import { existsSync } from "node:fs";
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
  type OrchestrationThreadShell,
  type ProviderRuntimeEvent,
  type ProviderSendTurnInput,
} from "@synara/contracts";
import { Effect, Exit, FileSystem, Latch, Layer, Option, Scope, Stream } from "effect";
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
  FAKE_REPO_URL,
  type FakeTurn,
} from "../aether/testing/aetherFakeTask.ts";
import { PROJECT_ID, TASK_ID } from "../aether/testing/aetherApiFixtures.ts";
import {
  added,
  diffOf,
  hunk,
  makeFakeWorkspace,
  modified,
  tooLarge,
} from "../aether/testing/aetherMirrorFixtures.ts";
import { classifyProviderAttemptOutcome } from "../../orchestration/Layers/ProviderCommandReactor.ts";
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
import { AetherAdapterLive, makeAetherAdapter, openRunningWorkspace } from "./AetherAdapter.ts";
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
  yield* git(repo, ["worktree", "add", "-b", "synara/aether-thread", worktree, "main"]);
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
  const makeAdapter = (bindings: ReadonlyArray<ProviderRuntimeBinding> = []) =>
    Effect.gen(function* () {
      const scope = yield* Scope.make();
      const adapter = yield* makeAetherAdapter({
        client: aether.client,
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

  return {
    root,
    repo,
    worktree,
    baseCommit,
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
                options: [{ label: "Red" }, { label: "Blue" }],
              },
              { question: "What should it be called?", options: [] },
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
            const launchProviderService = (adapter: AetherAdapterShape, scope: Scope.Scope) =>
              Layer.buildWithScope(providerServiceOver(adapter), scope).pipe(
                Effect.flatMap((services) =>
                  Effect.service(ProviderService).pipe(Effect.provide(services)),
                ),
              );

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
        const resumed = yield* adapter.startSession({
          threadId: THREAD_ID,
          provider: "aether",
          cwd: h.worktree,
          resumeCursor: idle!.resumeCursor,
          runtimeMode: "full-access",
        });
        expect(resumed).toMatchObject({ status: "ready" });
        yield* Effect.sleep("1500 millis");
        expect(events.slice(before)).toEqual([]);

        const third = yield* adapter.sendTurn(h.send("Third"));
        yield* waitFor("the third turn", () => completions(events)[2]);
        expect(events.slice(before).every((event) => event.turnId === third.turnId)).toBe(true);
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

    it.effect.each(["deleted", "archived", "archived, whose stop fails,"] as const)(
      "stops the turn in flight of a thread %s and releases its worktree",
      (retired) =>
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
            // Deleting a thread stops its session and removes its binding; archiving it stops
            // its runtime, keeping the binding, then releases what the thread held.
            if (retired === "deleted") {
              yield* providerService.stopSession({ threadId: THREAD_ID });
            } else {
              yield* providerService.stopRuntimeSession!({ threadId: THREAD_ID });
              if (retired === "archived") {
                yield* providerService.releaseThread!({ threadId: THREAD_ID });
              } else {
                h.aether.faults.refuseStop = new AetherApiHttpError({
                  endpoint: "POST /tasks/{taskID}/stop",
                  status: 503,
                });
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
            Effect.provide(providerServiceOver(adapter).pipe(Layer.provide(sessionDirectoryLayer))),
          );

          expect(h.aether.calls.stop).toEqual([{ discard_queued_messages: true }]);
          yield* gitCore.removeWorktree({ cwd: h.repo, path: h.worktree });
          expect(yield* h.exists("app.txt")).toBe(false);
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

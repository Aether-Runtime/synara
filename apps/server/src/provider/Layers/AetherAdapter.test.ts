// FILE: AetherAdapter.test.ts
// Purpose: The Aether adapter's lifecycle against a fake Aether API, a fake workspace socket and a
// real Git worktree whose origin is a local bare repository: task creation, turns, settlement
// behind the mirror, retries of lost sends, questions, interrupts, restarts and ownership.
// Layer: Server provider tests (Aether).
import * as fs from "node:fs/promises";
import path from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import {
  ApprovalRequestId,
  CheckpointRef,
  OrchestrationProposedPlanId,
  ThreadId,
  type ProviderRuntimeEvent,
  type ProviderSendTurnInput,
} from "@synara/contracts";
import { Effect, Exit, FileSystem, Layer, Option, Scope, Stream } from "effect";
import { describe, expect } from "vitest";

import { CheckpointStoreLive } from "../../checkpointing/Layers/CheckpointStore.ts";
import { CheckpointStore } from "../../checkpointing/Services/CheckpointStore.ts";
import { ServerConfig } from "../../config.ts";
import { GitCoreLive } from "../../git/Layers/GitCore.ts";
import { GitCore } from "../../git/Services/GitCore.ts";
import type { PersistedProviderRuntimeEvent } from "../../persistence/Services/ProviderRuntimeEvents.ts";
import {
  AetherApiConflictError,
  AetherApiHttpError,
  type AetherApiError,
} from "../aether/aetherApiErrors.ts";
import type { AetherWorkspaceId } from "../aether/aetherApiSchemas.ts";
import {
  makeFakeAetherTask,
  FAKE_REPO_URL,
  type FakeTurn,
} from "../aether/testing/aetherFakeTask.ts";
import { PROJECT_ID } from "../aether/testing/aetherApiFixtures.ts";
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
import type { AetherAdapterShape } from "../Services/AetherAdapter.ts";
import type { ProviderRuntimeBinding } from "../Services/ProviderSessionDirectory.ts";
import { makeAetherAdapter, openRunningWorkspace } from "./AetherAdapter.ts";

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

  /** Starts an adapter in its own scope; closing the scope is the process going away. */
  const launch = (bindings: ReadonlyArray<ProviderRuntimeBinding> = []) =>
    Effect.gen(function* () {
      const scope = yield* Scope.make();
      const adapter = yield* makeAetherAdapter({
        client: aether.client,
        openWorkspace: () =>
          vm.failure !== undefined
            ? Effect.fail(vm.failure)
            : Effect.succeed(vm.running ? Option.some(workspace.workspace) : Option.none()),
        journal: {
          getHighWaterSequence: Effect.sync(() => journal.length),
          readThreadEvents: (input) =>
            Effect.sync(() =>
              journal
                .filter(
                  ({ sequence, event }) =>
                    event.threadId === input.threadId &&
                    sequence <= input.throughSequenceInclusive &&
                    sequence < (input.beforeSequenceExclusive ?? Number.POSITIVE_INFINITY),
                )
                .toReversed()
                .slice(0, input.limit),
            ),
        },
        bindings,
      }).pipe(Scope.provide(scope));
      const events: ProviderRuntimeEvent[] = [];
      yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          events.push(event);
          journal.push({ sequence: journal.length + 1, event });
        }),
      ).pipe(Effect.forkIn(scope));
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
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
          managesWorktreeBranch: true,
          supportsTurnSteering: true,
          sessionModelSwitch: "unsupported",
          turnsSurviveRestart: true,
        });

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
        expect(idle).toMatchObject({ status: "ready", resumeCursor: { activeTurnId: null } });
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
        expect(otherHost.message).toContain("No Aether project is connected to github.com/acme/widgets");

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
        const error = "Your inference balance is empty. Add funds in Settings › Inference to continue.";
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
    it.effect("answers Aether's question shape: options by index, free text as custom", () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const { adapter, events } = yield* h.launch();
        yield* h.start(adapter);
        // A Codex task's questions, as Aether sends them: no header, no descriptions.
        h.aether.hooks.onTurn = (turn) =>
          h.aether.ask(turn, "exec-1", [], {
            questions: [
              { question: "Which color should I use?", options: [{ label: "Red" }, { label: "Blue" }] },
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
          h.aether.hooks.onTurn = (revised) => h.aether.propose(revised, "toolu_plan_2", "1. Use Postgres");
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
        expect(
          events.filter((event) => event.type === "turn.proposed.completed").at(-1),
        ).toMatchObject({ turnId: revising.turnId, payload: { planMarkdown: "1. Use Postgres" } });
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

          const second = yield* h.launch([
            {
              threadId: THREAD_ID,
              provider: "aether",
              runtimeMode: "full-access",
              status: "stopped",
              resumeCursor: session!.resumeCursor,
            },
          ]);
          expect(yield* second.adapter.listSessions()).toMatchObject([
            { status: "running", activeTurnId: turn.turnId },
          ]);
          const completed = yield* waitFor("turn.completed", () => completions(second.events)[0]);
          expect(completed).toMatchObject({ turnId: turn.turnId, payload: { state: "completed" } });
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

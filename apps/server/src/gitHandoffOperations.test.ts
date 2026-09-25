import {
  CommandId,
  ThreadId,
  type ModelSelection,
  type OrchestrationCommand,
} from "@synara/contracts";
import { assert, it } from "@effect/vitest";
import { Effect } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  beginGitHandoff,
  recordGitHandoffResult,
  recoverGitHandoffOperations,
  runGitHandoff,
} from "./gitHandoffOperations.ts";
import { decideOrchestrationCommand } from "./orchestration/decider.ts";
import {
  AETHER,
  makeReadModel,
  THREAD_ID,
  withSession,
  WORKTREE_PATH,
} from "./orchestration/testing/providerCapabilityReadModel.ts";
import { SqlitePersistenceMemory } from "./persistence/Layers/Sqlite.ts";

const layer = it.layer(SqlitePersistenceMemory);

const input = {
  commandId: CommandId.makeUnsafe("git-handoff-recovery-command"),
  threadId: ThreadId.makeUnsafe("git-handoff-recovery-thread"),
  cwd: "/repo",
  targetMode: "worktree" as const,
  currentBranch: "main",
  worktreePath: null,
  associatedWorktreePath: null,
  associatedWorktreeBranch: null,
  associatedWorktreeRef: null,
  preferredLocalBranch: null,
  preferredWorktreeBaseBranch: "main",
  preferredNewWorktreeName: "feature/recovery",
};

const result = {
  targetMode: "worktree" as const,
  branch: "feature/recovery",
  worktreePath: "/worktrees/recovery",
  associatedWorktreePath: "/worktrees/recovery",
  associatedWorktreeBranch: "feature/recovery",
  associatedWorktreeRef: "abc123",
  changesTransferred: true,
  conflictsDetected: false,
  message: "Recovered",
};

layer("Git handoff operation recovery", (it) => {
  it.effect(
    "replays durable Git results without running Git again and fences incomplete work",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`DELETE FROM git_handoff_operations`;

        assert.deepStrictEqual(yield* beginGitHandoff(input), { phase: "new" });
        yield* recordGitHandoffResult(input.commandId, result);
        assert.deepStrictEqual(yield* beginGitHandoff(input), {
          phase: "git_applied",
          result,
        });

        const dispatched: unknown[] = [];
        yield* recoverGitHandoffOperations((command) =>
          Effect.sync(() => {
            dispatched.push(command);
          }),
        );
        assert.lengthOf(dispatched, 1);
        assert.deepInclude(dispatched[0] as object, {
          type: "thread.meta.update",
          commandId: input.commandId,
          threadId: input.threadId,
          worktreePath: result.worktreePath,
        });
        assert.deepStrictEqual(yield* beginGitHandoff(input), { phase: "completed", result });

        const interruptedInput = {
          ...input,
          commandId: CommandId.makeUnsafe("git-handoff-interrupted-command"),
        };
        assert.deepStrictEqual(yield* beginGitHandoff(interruptedInput), { phase: "new" });
        yield* recoverGitHandoffOperations(() => Effect.void);
        assert.deepStrictEqual(yield* beginGitHandoff(interruptedInput), { phase: "uncertain" });
      }),
  );

  it.effect(
    "refuses the handoff RPC to Local before any git call when the provider owns the worktree",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`DELETE FROM git_handoff_operations`;
        const gitCalls: unknown[] = [];
        const dispatched: OrchestrationCommand[] = [];
        const toLocal = { ...input, targetMode: "local" as const, worktreePath: "/worktrees/x" };
        const handoff = (modelSelection: ModelSelection) =>
          runGitHandoff(toLocal, {
            // Idle: no session, so no live worktree claim either.
            threads: [{ id: input.threadId, session: null, latestTurn: null, modelSelection }],
            handoffThread: (gitInput) =>
              Effect.sync(() => {
                gitCalls.push(gitInput);
                return { ...result, targetMode: "local" as const, worktreePath: null };
              }),
            dispatch: (command) =>
              Effect.sync(() => {
                dispatched.push(command);
              }),
          });

        const refusal = yield* handoff({ provider: "aether", model: "claude-opus-5-5" }).pipe(
          Effect.flip,
        );
        assert.strictEqual(
          refusal.message,
          "Aether threads run only in a worktree that Aether owns, never in the local checkout.",
        );
        assert.lengthOf(gitCalls, 0);
        assert.lengthOf(dispatched, 0);
        const rows = yield* sql`SELECT command_id FROM git_handoff_operations`;
        assert.lengthOf(rows, 0);

        yield* handoff({ provider: "codex", model: "gpt-5-codex" });
        assert.lengthOf(gitCalls, 1);
        assert.deepInclude(dispatched[0] as object, {
          type: "thread.meta.update",
          envMode: "local",
          worktreePath: null,
        });
      }),
  );

  it.effect(
    "refuses the handoff RPC to a new worktree for a started Aether thread before any git call",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`DELETE FROM git_handoff_operations`;
        // A started Aether thread in its worktree; the real decider dispatches the metadata.
        const readModel = withSession(
          yield* Effect.promise(() => makeReadModel({ modelSelection: AETHER })),
          "aether",
        );
        const gitCalls: unknown[] = [];
        const refusal = yield* runGitHandoff(
          {
            ...input,
            threadId: THREAD_ID,
            targetMode: "worktree",
            worktreePath: WORKTREE_PATH,
          },
          {
            threads: readModel.threads,
            handoffThread: (gitInput) =>
              Effect.sync(() => {
                gitCalls.push(gitInput);
                return result;
              }),
            dispatch: (command) => decideOrchestrationCommand({ command, readModel }),
          },
        ).pipe(Effect.flip);

        assert.include(
          refusal.message,
          "Aether owns this thread's worktree, so the thread cannot move to another checkout once a turn has started.",
        );
        assert.lengthOf(gitCalls, 0);
        const rows = yield* sql`SELECT command_id FROM git_handoff_operations`;
        assert.lengthOf(rows, 0);
      }),
  );
});

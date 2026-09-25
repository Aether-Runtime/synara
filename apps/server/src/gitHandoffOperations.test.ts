import { CommandId, ThreadId, type ModelSelection } from "@synara/contracts";
import { assert, it } from "@effect/vitest";
import { Effect } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  beginGitHandoff,
  recordGitHandoffResult,
  recoverGitHandoffOperations,
  validateGitHandoffTarget,
} from "./gitHandoffOperations.ts";
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
});

it.effect(
  "refuses a handoff to Local before any git operation when the provider owns the worktree",
  () =>
    Effect.gen(function* () {
      const thread = (modelSelection: ModelSelection) => [
        { id: input.threadId, session: null, modelSelection },
      ];
      const aether = thread({ provider: "aether", model: "claude-opus-5-5" });
      const toLocal = { ...input, targetMode: "local" as const };

      // Idle: no session and no live worktree claim.
      const refusal = yield* validateGitHandoffTarget(toLocal, aether).pipe(Effect.flip);
      assert.include(refusal.message, "Aether needs a git repository with a GitHub remote");

      yield* validateGitHandoffTarget(toLocal, thread({ provider: "codex", model: "gpt-5-codex" }));
      yield* validateGitHandoffTarget(input, aether);
      const missing = yield* validateGitHandoffTarget(toLocal, []).pipe(Effect.flip);
      assert.include(missing.message, "was not found");
    }),
);

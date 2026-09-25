import {
  GitHandoffThreadInput,
  GitHandoffThreadResult,
  ProviderKind,
  type OrchestrationCommand,
  type OrchestrationThread,
} from "@synara/contracts";
import {
  providerFeatureUnavailableMessage,
  providerLackingFeature,
} from "@synara/shared/providerMetadata";
import { resolveWorktreeHandoffWorkspaceMetadata } from "@synara/shared/worktreeHandoff";
import { Data, Effect, Schema } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

type HandoffPhase = "pending" | "git_applied" | "completed" | "uncertain";

interface HandoffRow {
  readonly commandId: string;
  readonly threadId: string;
  readonly inputJson: string;
  readonly phase: HandoffPhase;
  readonly resultJson: string | null;
}

export type GitHandoffOperation =
  | { readonly phase: "new" }
  | { readonly phase: "pending" | "uncertain" }
  | {
      readonly phase: "git_applied" | "completed";
      readonly result: GitHandoffThreadResult;
    };

export class GitHandoffOperationError extends Data.TaggedError("GitHandoffOperationError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

const operationError = (message: string) => (cause: unknown) =>
  new GitHandoffOperationError({ message, cause });

const parseResult = (row: HandoffRow) =>
  Effect.try({
    try: () => Schema.decodeUnknownSync(GitHandoffThreadResult)(JSON.parse(row.resultJson ?? "")),
    catch: operationError(`Invalid persisted Git handoff result for ${row.commandId}.`),
  });

const readOperation = (commandId: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql<HandoffRow>`
      SELECT
        command_id AS "commandId",
        thread_id AS "threadId",
        input_json AS "inputJson",
        phase,
        result_json AS "resultJson"
      FROM git_handoff_operations
      WHERE command_id = ${commandId}
    `.pipe(Effect.mapError(operationError("Failed to read Git handoff operation.")));
    return rows[0] ?? null;
  });

const decodeOperation = (
  row: HandoffRow,
): Effect.Effect<GitHandoffOperation, GitHandoffOperationError> =>
  row.phase === "git_applied" || row.phase === "completed"
    ? parseResult(row).pipe(Effect.map((result) => ({ phase: row.phase, result })))
    : Effect.succeed({ phase: row.phase });

export const beginGitHandoff = (input: GitHandoffThreadInput) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const inputJson = JSON.stringify(input);
    const now = new Date().toISOString();
    const inserted = yield* sql<{ readonly commandId: string }>`
      INSERT INTO git_handoff_operations (
        command_id, thread_id, input_json, phase, result_json, created_at, updated_at
      ) VALUES (
        ${input.commandId}, ${input.threadId}, ${inputJson}, 'pending', NULL, ${now}, ${now}
      )
      ON CONFLICT (command_id) DO NOTHING
      RETURNING command_id AS "commandId"
    `.pipe(Effect.mapError(operationError("Failed to begin Git handoff operation.")));
    if (inserted.length > 0) return { phase: "new" } as const;

    const existing = yield* readOperation(input.commandId);
    if (!existing || existing.threadId !== input.threadId || existing.inputJson !== inputJson) {
      return yield* new GitHandoffOperationError({
        message: `Git handoff command identity ${input.commandId} was reused with different input.`,
      });
    }
    return yield* decodeOperation(existing);
  });

export const recordGitHandoffResult = (commandId: string, result: GitHandoffThreadResult) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const resultJson = JSON.stringify(result);
    const now = new Date().toISOString();
    yield* sql`
      UPDATE git_handoff_operations
      SET phase = 'git_applied', result_json = ${resultJson}, updated_at = ${now}
      WHERE command_id = ${commandId} AND phase = 'pending'
    `.pipe(Effect.mapError(operationError("Failed to persist applied Git handoff result.")));
  });

export const completeGitHandoff = (commandId: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`
      UPDATE git_handoff_operations
      SET phase = 'completed', updated_at = ${new Date().toISOString()}
      WHERE command_id = ${commandId} AND phase = 'git_applied'
    `.pipe(Effect.mapError(operationError("Failed to complete Git handoff operation.")));
  });

export const discardPendingGitHandoff = (commandId: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`
      DELETE FROM git_handoff_operations
      WHERE command_id = ${commandId} AND phase = 'pending'
    `.pipe(Effect.mapError(operationError("Failed to discard failed Git handoff operation.")));
  });

export const gitHandoffMetadataCommand = (
  input: Pick<GitHandoffThreadInput, "commandId" | "threadId">,
  result: GitHandoffThreadResult,
): OrchestrationCommand => ({
  type: "thread.meta.update",
  commandId: input.commandId,
  threadId: input.threadId,
  ...resolveWorktreeHandoffWorkspaceMetadata(result),
});

const isProviderKind = Schema.is(ProviderKind);

/**
 * The git.handoffThread RPC. A handoff to Local ends with a `thread.meta.update` to Local, which
 * the decider refuses for a thread whose provider owns its worktree, so that thread is refused
 * here, before any git operation stashes and removes the worktree.
 */
export const runGitHandoff = <HandoffError, HandoffContext, DispatchError, DispatchContext>(
  input: GitHandoffThreadInput,
  dependencies: {
    readonly threads: ReadonlyArray<Pick<OrchestrationThread, "id" | "session" | "modelSelection">>;
    readonly handoffThread: (
      gitInput: Omit<GitHandoffThreadInput, "commandId" | "threadId">,
    ) => Effect.Effect<GitHandoffThreadResult, HandoffError, HandoffContext>;
    readonly dispatch: (
      command: OrchestrationCommand,
    ) => Effect.Effect<unknown, DispatchError, DispatchContext>;
  },
) =>
  Effect.gen(function* () {
    const { commandId, threadId, ...gitInput } = input;
    const thread = dependencies.threads.find((entry) => entry.id === threadId);
    if (thread === undefined) {
      return yield* new GitHandoffOperationError({
        message: `Thread '${threadId}' was not found.`,
      });
    }
    if (input.targetMode === "local") {
      const sessionProvider = thread.session?.providerName ?? null;
      if (sessionProvider !== null && !isProviderKind(sessionProvider)) {
        return yield* new GitHandoffOperationError({
          message: `Thread '${threadId}' is bound to unknown provider '${sessionProvider}'.`,
        });
      }
      const refused = providerLackingFeature(
        [sessionProvider, thread.modelSelection.provider],
        "local-workspace",
      );
      if (refused !== null) {
        return yield* new GitHandoffOperationError({
          message: providerFeatureUnavailableMessage(refused, "local-workspace"),
        });
      }
    }
    const operation = yield* beginGitHandoff(input);
    if (operation.phase === "pending" || operation.phase === "uncertain") {
      return yield* new GitHandoffOperationError({
        message:
          operation.phase === "pending"
            ? "This Git handoff is already running."
            : "This Git handoff was interrupted before its filesystem result became durable; inspect the repository before retrying.",
      });
    }
    if (operation.phase === "completed") return operation.result;

    const result =
      operation.phase === "git_applied"
        ? operation.result
        : yield* dependencies.handoffThread(gitInput).pipe(
            Effect.catch((error) =>
              discardPendingGitHandoff(commandId).pipe(
                Effect.catch(() => Effect.void),
                Effect.andThen(Effect.fail(error)),
              ),
            ),
            Effect.tap((gitResult) => recordGitHandoffResult(commandId, gitResult)),
          );
    yield* dependencies.dispatch(gitHandoffMetadataCommand({ commandId, threadId }, result));
    yield* completeGitHandoff(commandId);
    return result;
  });

export const recoverGitHandoffOperations = (
  dispatch: (command: OrchestrationCommand) => Effect.Effect<unknown, unknown>,
) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const interrupted = yield* sql<{ readonly commandId: string }>`
      UPDATE git_handoff_operations
      SET phase = 'uncertain', updated_at = ${new Date().toISOString()}
      WHERE phase = 'pending'
      RETURNING command_id AS "commandId"
    `.pipe(Effect.mapError(operationError("Failed to fence interrupted Git handoffs.")));
    if (interrupted.length > 0) {
      yield* Effect.logWarning("Git handoffs were interrupted before their result was durable", {
        commandIds: interrupted.map(({ commandId }) => commandId),
      });
    }

    const rows = yield* sql<HandoffRow>`
      SELECT
        command_id AS "commandId",
        thread_id AS "threadId",
        input_json AS "inputJson",
        phase,
        result_json AS "resultJson"
      FROM git_handoff_operations
      WHERE phase = 'git_applied'
      ORDER BY updated_at ASC, command_id ASC
    `.pipe(Effect.mapError(operationError("Failed to list recoverable Git handoffs.")));

    for (const row of rows) {
      const input = yield* Effect.try({
        try: () => Schema.decodeUnknownSync(GitHandoffThreadInput)(JSON.parse(row.inputJson)),
        catch: operationError(`Invalid persisted Git handoff input for ${row.commandId}.`),
      });
      const result = yield* parseResult(row);
      yield* dispatch(gitHandoffMetadataCommand(input, result)).pipe(
        Effect.mapError(operationError(`Failed to recover Git handoff ${row.commandId}.`)),
      );
      yield* completeGitHandoff(row.commandId);
    }
  });

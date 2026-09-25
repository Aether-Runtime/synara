// FILE: aetherResumeCursor.ts
// Purpose: The Aether adapter's resume cursor: what Synara persists per thread to find its task
// again, retry a lost send without sending it twice, map Aether turns back to Synara turns, and
// pick the conversation up after a restart.
// Layer: Server provider boundary (Aether). Parsed strictly where it enters the adapter.

import { NonNegativeInt, TurnId } from "@synara/contracts";
import { Cause, Exit, Schema } from "effect";

import { ProviderAdapterValidationError } from "../Errors.ts";
import { AetherProjectId, AetherTaskId, AetherTurnId } from "./aetherApiSchemas.ts";

const NonEmptyString = Schema.String.check(Schema.isMinLength(1));
const Uuid = Schema.String.check(Schema.isUUID(undefined));
const CommitId = Schema.String.check(Schema.isPattern(/^[0-9a-f]{40}(?:[0-9a-f]{24})?$/u));

/** A Synara turn, and the Aether turn it runs in once that is known. */
export const AetherSentTurn = Schema.Struct({
  turnId: TurnId,
  /** The respond's `client_message_id`; null for the turn `POST /tasks` started. */
  clientMessageId: Schema.NullOr(Uuid),
  aetherTurnId: Schema.NullOr(AetherTurnId),
});
export type AetherSentTurn = typeof AetherSentTurn.Type;

export const AetherResumeCursor = Schema.Union([
  /** No task yet. */
  Schema.Struct({
    schemaVersion: Schema.Literal(1),
    status: Schema.Literal("creating"),
    worktreePath: NonEmptyString,
    /** Sent with `POST /tasks` and every retry of it, so a lost 202 never starts a second task. */
    idempotencyKey: Uuid,
  }),
  Schema.Struct({
    schemaVersion: Schema.Literal(1),
    status: Schema.Literal("created"),
    worktreePath: NonEmptyString,
    taskId: AetherTaskId,
    projectId: AetherProjectId,
    baseBranch: NonEmptyString,
    /** The commit the VM cloned: the mirror's merge-base, and the worktree's HEAD. */
    baseCommit: CommitId,
    model: NonEmptyString,
    /** Every turn Synara started in the task, oldest first. */
    turns: Schema.Array(AetherSentTurn),
    /** Accepted responds; with the request, it names the next respond's `client_message_id`. */
    sends: NonNegativeInt,
    /** The last run the adapter settled: where a restarted poll loop picks up. */
    after: Schema.NullOr(Schema.Struct({ turnId: AetherTurnId, runMessageId: NonEmptyString })),
    /** The Synara turn in flight; it settles when its Aether run does. */
    activeTurnId: Schema.NullOr(TurnId),
    /** A run settled while its workspace was unreachable: mirror at the next turn start. */
    mirrorPending: Schema.Boolean,
  }),
]);
export type AetherResumeCursor = typeof AetherResumeCursor.Type;
export type AetherCreatedCursor = Extract<AetherResumeCursor, { readonly status: "created" }>;

/** Parses a persisted cursor; anything else is a loud validation error, never a fresh task. */
export function parseAetherResumeCursor(
  raw: unknown,
): Exit.Exit<AetherResumeCursor, ProviderAdapterValidationError> {
  const decoded = Schema.decodeUnknownExit(AetherResumeCursor)(raw);
  return Exit.isSuccess(decoded)
    ? Exit.succeed(decoded.value)
    : Exit.fail(
        new ProviderAdapterValidationError({
          provider: "aether",
          operation: "startSession",
          issue: `The saved Aether resume state is unreadable: ${Cause.pretty(decoded.cause)}`,
        }),
      );
}

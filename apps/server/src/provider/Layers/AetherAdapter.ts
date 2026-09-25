/**
 * AetherAdapterLive - The Aether cloud provider: each Synara thread runs one Aether task.
 *
 * The first turn creates the task from the thread's worktree (clean, and at the tip of its base
 * branch on origin, since the VM clones from origin); later turns respond to it. One poll loop per
 * task (`aetherConversationPoller`) turns the durable conversation into runtime events. When a
 * run settles, the VM's changes are mirrored into the worktree before `turn.completed`, so the
 * checkpoint Synara takes on completion holds them; each Synara turn completes exactly once.
 *
 * The worktree is claimed for the thread's lifetime, not the session's: `stopSession` only stops
 * following the task, which keeps running in the cloud. The resume cursor
 * (`aetherResumeCursor.ts`) carries everything a restart needs; at construction the adapter
 * claims every Aether thread's worktree again and reattaches to a turn still in flight, which
 * startup reconciliation leaves alone (`turnsSurviveRestart`).
 *
 * @module AetherAdapterLive
 */
import { createHash, randomUUID } from "node:crypto";

import {
  EventId,
  resolveAetherAgentFamily,
  TurnId,
  type ProviderRuntimeEvent,
  type ProviderSendTurnInput,
  type ProviderSession,
  type ProviderUserInputAnswers,
  type RuntimeMode,
  type ThreadId,
} from "@synara/contracts";
import {
  Cause,
  Clock,
  Effect,
  Exit,
  type FileSystem,
  Layer,
  Option,
  type Path,
  Queue,
  Result,
  Schema,
  Scope,
  Semaphore,
  Stream,
} from "effect";

import type { WorktreeOwnedError } from "../../git/Errors.ts";
import { GitCore, type WorktreeClaim } from "../../git/Services/GitCore.ts";
import {
  ProviderRuntimeEventRepository,
  type ProviderRuntimeEventRepositoryShape,
} from "../../persistence/Services/ProviderRuntimeEvents.ts";
import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import type { AetherApiError } from "../aether/aetherApiErrors.ts";
import type {
  AetherRespondToTaskRequest,
  AetherTurnId,
  AetherWorkspaceId,
} from "../aether/aetherApiSchemas.ts";
import {
  AskUserInput,
  type AetherIngestOutput,
  type AetherIngestResume,
} from "../aether/aetherConversationIngest.ts";
import {
  makeAetherConversationPoller,
  type AetherConversationPollFailure,
  type AetherConversationPoller,
  type AetherPollerOutput,
} from "../aether/aetherConversationPoller.ts";
import type { AetherMirrorError } from "../aether/aetherMirrorErrors.ts";
import { syncAetherMirror, type AetherMirrorWorkspace } from "../aether/aetherMirrorSync.ts";
import {
  parseAetherResumeCursor,
  type AetherCreatedCursor,
  type AetherResumeCursor,
} from "../aether/aetherResumeCursor.ts";
import { requireAetherWorktree, resolveAetherTaskBase } from "../aether/aetherTaskBase.ts";
import {
  connectAetherWorkspace,
  type AetherWorkspaceSocketError,
} from "../aether/aetherWorkspaceSocket.ts";
import { AetherAdapter, type AetherAdapterShape } from "../Services/AetherAdapter.ts";
import { AetherApiClient, type AetherApiClientShape } from "../Services/AetherApiClient.ts";
import { PROVIDER_ADAPTER_RUNTIME_EVENT_BUFFER_CAPACITY } from "../Services/ProviderAdapter.ts";
import {
  ProviderSessionDirectory,
  type ProviderRuntimeBinding,
} from "../Services/ProviderSessionDirectory.ts";

const PROVIDER = "aether" as const;

/** Aether's task error when the org's inference balance runs out (no failure kind yet: AS4). */
const INFERENCE_BALANCE_EMPTY =
  "Your inference balance is empty. Add funds in Settings › Inference to continue.";

/** Opens the task's workspace without booting it: `None` while it is not running. */
export type AetherWorkspaceOpener = (
  workspaceId: AetherWorkspaceId,
) => Effect.Effect<
  Option.Option<AetherMirrorWorkspace>,
  AetherApiError | AetherWorkspaceSocketError,
  Scope.Scope
>;

export interface AetherAdapterOptions {
  readonly client: AetherApiClientShape;
  readonly openWorkspace: AetherWorkspaceOpener;
  /** The runtime event journal: a resumed task continues from what it holds. */
  readonly journal: Pick<
    ProviderRuntimeEventRepositoryShape,
    "getHighWaterSequence" | "readThreadEvents"
  >;
  /** Persisted bindings: every Aether thread with a task is claimed and, mid-turn, reattached. */
  readonly bindings: ReadonlyArray<ProviderRuntimeBinding>;
}

interface Session {
  readonly threadId: ThreadId;
  readonly runtimeMode: RuntimeMode;
  readonly model: string;
  readonly createdAt: string;
  readonly scope: Scope.Closeable;
  readonly lock: Semaphore.Semaphore;
  readonly claim: WorktreeClaim;
  cursor: AetherResumeCursor;
  updatedAt: string;
  poller: AetherConversationPoller | undefined;
  /** Event ids the journal holds: a resumed poll loop derives them again, and they are dropped. */
  replayed: ReadonlySet<string>;
  /** Aether turns that did not come from Synara (sent from Aether's own UI). */
  readonly remoteTurns: Set<AetherTurnId>;
  /** The Synara turn an interrupt was sent for: it completes as interrupted. */
  interrupted: TurnId | undefined;
  /**
   * Runs that ended before their Synara turn started: a send whose response was lost landed
   * anyway. The retry, which is the same request and so the same turn, completes at once.
   */
  readonly settledEarly: Map<TurnId, TurnEnding>;
}

interface TurnEnding {
  readonly state: "completed" | "failed";
  readonly errors: ReadonlyArray<string>;
}

type MirrorOutcome =
  | { readonly _tag: "Mirrored"; readonly note: string | undefined }
  /** The workspace was not reachable: mirror when the next turn starts or settles. */
  | { readonly _tag: "Deferred"; readonly note: string }
  | { readonly _tag: "Failed"; readonly note: string };

type TurnSettled = Extract<AetherIngestOutput, { readonly _tag: "TurnSettled" }>;

interface TurnRequest {
  readonly message: string;
  readonly mode: "default" | "plan";
  readonly effort: string | undefined;
}

/** Aether refused the send, so nothing landed; otherwise its response may just have been lost. */
function sendRefused(error: AetherApiError): boolean {
  switch (error._tag) {
    case "AetherApiNotConfiguredError":
    case "AetherApiPaymentRequiredError":
    case "AetherApiConflictError":
      return true;
    case "AetherApiHttpError":
      return error.status < 500 && error.status !== 408 && error.status !== 429;
    default:
      return false;
  }
}

const invalid = (operation: string, issue: string) =>
  new ProviderAdapterValidationError({ provider: PROVIDER, operation, issue });

/** A UUID derived from `text`, in the RFC 9562 version 8 layout. */
function derivedUuid(text: string): string {
  const hex = createHash("sha256").update(text).digest("hex");
  const variant = ((Number.parseInt(hex[16]!, 16) & 0x3) | 0x8).toString(16);
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-8${hex.slice(13, 16)}-${variant}${hex.slice(17, 20)}-${hex.slice(20, 32)}`;
}

/**
 * The respond's `client_message_id`. Derived from what is persisted (the task and its count of
 * accepted responds) and the request, so a retry of the same request after a lost response, even
 * after a restart, reuses the id and Aether keeps the original, while any other request is new.
 */
const clientMessageIdFor = (cursor: AetherCreatedCursor, body: unknown) =>
  derivedUuid(`${cursor.taskId}:${cursor.sends}:${JSON.stringify(body)}`);

/** The cursor of a session whose task exists: every caller runs only once it does. */
function created(session: Session): AetherCreatedCursor {
  if (session.cursor.status !== "created") throw new Error("The Aether task does not exist yet.");
  return session.cursor;
}

function describeApiError(error: AetherApiError): string {
  if (error._tag === "AetherApiPaymentRequiredError") {
    return `Aether did not admit this task on your plan (${error.code}): ${error.serverMessage} Add funds or change your plan at https://runaether.dev, then send again.`;
  }
  if (error._tag === "AetherApiConflictError" && "code" in error.body) {
    if (error.body.code === "idempotency_key_reused") {
      return "Aether already started a task for an earlier attempt at this thread's first message, with a different request. Send that same message again to continue it, or start a new thread.";
    }
    if (error.body.code === "stale_pending_input") {
      return "Aether's question changed before this answer arrived. Synara read the task again: answer the question it shows now.";
    }
  }
  return error.message;
}

const apiFailure = (method: string) => (error: AetherApiError) =>
  new ProviderAdapterRequestError({
    provider: PROVIDER,
    method,
    detail: describeApiError(error),
    cause: error,
  });

function taskErrorMessage(message: string | undefined): string {
  if (message === INFERENCE_BALANCE_EMPTY) {
    return "Your Aether inference balance is empty. Add funds in Aether (https://runaether.dev, Settings › Inference), then send your message again.";
  }
  return message ?? "Aether reported the turn as failed.";
}

function mirrorFailure(
  error: AetherMirrorError | WorktreeOwnedError | AetherApiError | AetherWorkspaceSocketError,
): MirrorOutcome {
  if (error._tag !== "AetherMirrorError") {
    return error._tag === "WorktreeOwnedError"
      ? { _tag: "Failed", note: error.message }
      : deferred(`the workspace could not be reached: ${error.message}`);
  }
  const paths = error.paths.length > 0 ? error.paths.join(", ") : error.detail;
  switch (error.reason) {
    case "transport":
      return deferred(`the workspace could not be reached: ${error.detail}`);
    case "local-edits":
      return {
        _tag: "Failed",
        note: `Synara paused mirroring Aether's changes: the worktree has local changes (${paths}). Discard or move them; the next turn mirrors again. Nothing was overwritten.`,
      };
    case "too-large":
      return {
        _tag: "Failed",
        note: `Aether changed files too large to mirror (${paths}), so the worktree does not show this turn's changes.`,
      };
    case "path-escape":
      return {
        _tag: "Failed",
        note: `Aether changed paths Git refuses to write (${paths}), so the worktree does not show this turn's changes.`,
      };
    case "base-mismatch":
      return {
        _tag: "Failed",
        note: `The agent moved the task off its base commit, so Synara cannot mirror its changes any more: ${error.detail}`,
      };
    default:
      return {
        _tag: "Failed",
        note: `Synara could not mirror Aether's changes (${error.reason}): ${error.detail}`,
      };
  }
}

const deferred = (reason: string): MirrorOutcome => ({
  _tag: "Deferred",
  note: `Synara did not mirror Aether's changes yet (${reason}). It tries again when the next turn starts or settles.`,
});

/** Aether's `ask_user` answer: option indexes by question index, `-1` for a typed answer. */
function askUserAnswer(input: unknown, answers: ProviderUserInputAnswers) {
  const decoded = Schema.decodeUnknownExit(AskUserInput)(input);
  if (Exit.isFailure(decoded)) {
    return Result.fail(`the question is unreadable: ${Cause.pretty(decoded.cause)}`);
  }
  const { questions } = decoded.value;
  const unknown = Object.keys(answers).find((id) => !(Number(id) in questions));
  if (unknown !== undefined) return Result.fail(`there is no question ${unknown}`);
  const chosen: Record<string, number[]> = {};
  const customAnswers: Record<string, string> = {};
  for (const [index, question] of questions.entries()) {
    const id = String(index);
    const answer = answers[id];
    const labels = answer === undefined || answer === null ? [] : [answer].flat();
    chosen[id] = [];
    for (const label of labels) {
      const option = question.options.findIndex((candidate) => candidate.label.trim() === label);
      if (option === -1 && customAnswers[id] !== undefined) {
        return Result.fail(`question ${id} has more than one answer outside its options`);
      }
      if (option === -1) customAnswers[id] = label;
      chosen[id].push(option);
    }
  }
  return Result.succeed({
    answers: chosen,
    ...(Object.keys(customAnswers).length === 0 ? {} : { customAnswers }),
  });
}

function turnText(input: ProviderSendTurnInput, operation: string) {
  if ((input.attachments?.length ?? 0) > 0) {
    return Exit.fail(invalid(operation, "Aether turns do not take attachments yet."));
  }
  if ((input.skills?.length ?? 0) > 0 || (input.mentions?.length ?? 0) > 0) {
    return Exit.fail(invalid(operation, "Aether turns do not take skills or plugin mentions."));
  }
  return input.input === undefined
    ? Exit.fail(invalid(operation, "An Aether turn needs a message."))
    : Exit.succeed(input.input);
}

export const makeAetherAdapter = (
  options: AetherAdapterOptions,
): Effect.Effect<
  AetherAdapterShape,
  never,
  Scope.Scope | GitCore | FileSystem.FileSystem | Path.Path
> =>
  Effect.gen(function* () {
    const { client, openWorkspace, journal } = options;
    const gitCore = yield* GitCore;
    const services = yield* Effect.services<GitCore | FileSystem.FileSystem | Path.Path>();
    const adapterScope = yield* Scope.Scope;
    const events = yield* Queue.bounded<ProviderRuntimeEvent>(
      PROVIDER_ADAPTER_RUNTIME_EVENT_BUFFER_CAPACITY,
    );
    const sessions = new Map<ThreadId, Session>();
    /** Held for the thread's lifetime: released only when the adapter shuts down. */
    const claims = new Map<ThreadId, WorktreeClaim>();
    const nowIso = Effect.map(Clock.currentTimeMillis, (millis) => new Date(millis).toISOString());

    const requireTask = (session: Session, operation: string) =>
      session.cursor.status === "created"
        ? Effect.succeed(session.cursor)
        : Effect.fail(invalid(operation, "No Aether task has started for this thread yet."));

    const snapshot = (session: Session): ProviderSession => {
      const active = session.cursor.status === "created" ? session.cursor.activeTurnId : null;
      return {
        provider: PROVIDER,
        status: active === null ? "ready" : "running",
        runtimeMode: session.runtimeMode,
        cwd: session.cursor.worktreePath,
        model: session.model,
        threadId: session.threadId,
        resumeCursor: session.cursor,
        ...(active === null ? {} : { activeTurnId: active }),
        createdAt: session.createdAt,
        updatedAt: session.updatedAt,
      };
    };

    // -----------------------------------------------------------------------
    // Events
    // -----------------------------------------------------------------------

    /** Every event goes out here; one the journal already holds is not sent again. */
    const publish = (session: Session, event: ProviderRuntimeEvent) =>
      session.replayed.has(event.eventId)
        ? Effect.void
        : Effect.suspend(() => {
            session.updatedAt = event.createdAt;
            return Queue.offer(events, event);
          }).pipe(Effect.asVoid);

    const base = (session: Session, turnId: TurnId | undefined, parts: ReadonlyArray<string>) =>
      Effect.map(nowIso, (createdAt) => ({
        eventId: EventId.makeUnsafe(
          [
            "aether",
            created(session).taskId,
            turnId === undefined ? "task" : `synara:${turnId}`,
            ...parts,
          ].join(":"),
        ),
        provider: PROVIDER,
        threadId: session.threadId,
        createdAt,
        ...(turnId === undefined ? {} : { turnId }),
      }));

    const warn = (
      session: Session,
      turnId: TurnId | undefined,
      parts: ReadonlyArray<string>,
      message: string,
      detail?: unknown,
    ) =>
      base(session, turnId, parts).pipe(
        Effect.flatMap((fields) =>
          publish(session, {
            ...fields,
            type: "runtime.warning",
            payload: { message, ...(detail === undefined ? {} : { detail }) },
          }),
        ),
      );

    /** The turn's one `turn.completed`; its id is the turn's, so a replay of it is dropped. */
    const complete = (
      session: Session,
      turnId: TurnId,
      state: "completed" | "failed" | "interrupted",
      errors: ReadonlyArray<string> = [],
    ) =>
      Effect.gen(function* () {
        const cursor = created(session);
        session.cursor = {
          ...cursor,
          activeTurnId: cursor.activeTurnId === turnId ? null : cursor.activeTurnId,
        };
        yield* publish(session, {
          ...(yield* base(session, turnId, ["completed"])),
          type: "turn.completed",
          payload: { state, ...(errors.length === 0 ? {} : { errorMessage: errors.join("\n\n") }) },
        });
      });

    // -----------------------------------------------------------------------
    // Turn mapping
    // -----------------------------------------------------------------------

    /**
     * The Synara turn an Aether turn's events belong to: the latest bound to it (an approved plan
     * moves its Aether turn to the approving turn); for a turn not seen yet, the turn in flight if
     * it still waits for its own (its text can stream before its turn is listed); otherwise an id
     * of its own, for a turn sent from outside Synara.
     */
    const turnIdFor = (session: Session, aetherTurnId: AetherTurnId): TurnId => {
      const { turns, activeTurnId } = created(session);
      if (!session.remoteTurns.has(aetherTurnId)) {
        const bound = turns.findLast((turn) => turn.aetherTurnId === aetherTurnId);
        if (bound !== undefined) return bound.turnId;
        const waiting = turns.find(
          (turn) => turn.aetherTurnId === null && turn.turnId === activeTurnId,
        );
        if (waiting !== undefined) return waiting.turnId;
      }
      return TurnId.makeUnsafe(`aether-remote:${aetherTurnId}`);
    };

    /** Binds a new Aether turn to the Synara turn that sent it; false when none did. */
    const bindOrigin = (
      session: Session,
      aetherTurnId: AetherTurnId,
      clientMessageId: string | undefined,
    ): boolean => {
      const cursor = created(session);
      if (cursor.turns.some((turn) => turn.aetherTurnId === aetherTurnId)) return true;
      if (session.remoteTurns.has(aetherTurnId)) return false;
      const unbound = (id: string | null) =>
        cursor.turns.findIndex((turn) => turn.aetherTurnId === null && turn.clientMessageId === id);
      const bySend = clientMessageId === undefined ? -1 : unbound(clientMessageId);
      // The task's first turn is the one `POST /tasks` started.
      const index = bySend !== -1 ? bySend : unbound(null);
      if (index === -1) {
        session.remoteTurns.add(aetherTurnId);
        return false;
      }
      session.cursor = {
        ...cursor,
        turns: cursor.turns.with(index, { ...cursor.turns[index]!, aetherTurnId }),
      };
      return true;
    };

    // -----------------------------------------------------------------------
    // Mirror
    // -----------------------------------------------------------------------

    /** Mirrors the VM's working tree into the worktree; never boots a suspended VM. */
    const mirror = (session: Session): Effect.Effect<MirrorOutcome> =>
      Effect.gen(function* () {
        const cursor = created(session);
        const page = yield* client.getConversationTurns(cursor.taskId, { limit: 1 });
        const workspaceId = page.task.run_context?.workspace_id;
        if (workspaceId === undefined) return deferred("the task has no workspace yet");
        const workspace = yield* openWorkspace(workspaceId);
        if (Option.isNone(workspace)) return deferred("its workspace is not running");
        const result = yield* syncAetherMirror({
          workspace: workspace.value,
          claim: session.claim,
          taskId: cursor.taskId,
          baseCommit: cursor.baseCommit,
        });
        return {
          _tag: "Mirrored",
          note:
            result._tag === "synced" && result.modeOnlyPaths.length > 0
              ? `Aether changed only the mode of ${result.modeOnlyPaths.join(", ")}; the worktree keeps the old mode.`
              : undefined,
        } satisfies MirrorOutcome;
      }).pipe(
        Effect.scoped,
        Effect.provideServices(services),
        Effect.catch((error) => Effect.succeed(mirrorFailure(error))),
      );

    /** Mirrors a run a suspended workspace kept back, before the next turn is sent. */
    const mirrorPending = (session: Session) =>
      Effect.gen(function* () {
        if (!created(session).mirrorPending) return undefined;
        const outcome = yield* mirror(session);
        session.cursor = { ...created(session), mirrorPending: outcome._tag === "Deferred" };
        return outcome._tag === "Deferred" ? undefined : outcome.note;
      });

    // -----------------------------------------------------------------------
    // Poll loop outputs
    // -----------------------------------------------------------------------

    /** A run ended: mirror, record it, then complete its Synara turn if it is the one in flight. */
    const settle = (session: Session, settled: TurnSettled) =>
      Effect.gen(function* () {
        const turnId = session.remoteTurns.has(settled.turnId)
          ? undefined
          : created(session).turns.findLast((turn) => turn.aetherTurnId === settled.turnId)?.turnId;
        const outcome = yield* mirror(session);
        const cursor = created(session);
        const completes = turnId !== undefined && cursor.activeTurnId === turnId;
        session.cursor = {
          ...cursor,
          after: { turnId: settled.turnId, runMessageId: settled.runMessageId },
          mirrorPending: outcome._tag === "Deferred",
        };
        const failed = outcome._tag === "Failed" ? outcome.note : undefined;
        const note = completes && failed !== undefined ? undefined : outcome.note;
        if (note !== undefined) {
          yield* warn(session, turnId, ["mirror", settled.turnId, settled.runMessageId], note);
        }
        if (turnId === undefined) return;
        const errors = [
          ...(settled.state === "errored" ? [taskErrorMessage(settled.errorMessage)] : []),
          ...(failed === undefined ? [] : [failed]),
        ];
        const ending: TurnEnding = { state: errors.length > 0 ? "failed" : "completed", errors };
        if (!completes) {
          session.settledEarly.set(turnId, ending);
          return;
        }
        yield* complete(
          session,
          turnId,
          session.interrupted === turnId ? "interrupted" : ending.state,
          errors,
        );
      });

    /** The loop stopped: fail the turn in flight. The next message, answer or stop wakes it. */
    const pollingStopped = (session: Session, error: AetherConversationPollFailure) =>
      Effect.gen(function* () {
        const message = `Synara stopped following the Aether task: ${error.message} It follows it again at the next message.`;
        const turnId = created(session).activeTurnId;
        if (turnId !== null) return yield* complete(session, turnId, "failed", [message]);
        yield* warn(session, undefined, ["polling-stopped", yield* nowIso], message);
      });

    const handleOutput = (session: Session, output: AetherPollerOutput) => {
      switch (output._tag) {
        case "Event":
          return publish(session, output.event);
        case "TurnOrigin":
          return bindOrigin(session, output.turnId, output.clientMessageId)
            ? Effect.void
            : Effect.gen(function* () {
                yield* publish(session, {
                  eventId: EventId.makeUnsafe(
                    `aether:${created(session).taskId}:${output.turnId}:remote-origin`,
                  ),
                  provider: PROVIDER,
                  threadId: session.threadId,
                  createdAt: yield* nowIso,
                  turnId: turnIdFor(session, output.turnId),
                  type: "runtime.warning",
                  payload: {
                    message: "A message reached this Aether task from outside Synara.",
                    detail: output.rootMessageText,
                  },
                });
              });
        case "TurnSettled":
          return settle(session, output);
        case "PollingStopped":
          return pollingStopped(session, output.error);
        case "PendingInput":
        case "PendingInputResolved":
        case "ConversationReset":
          return Effect.void;
      }
    };

    /** What the journal holds for the thread: ids to drop, and the text already streamed. */
    const readJournal = (threadId: ThreadId) =>
      Effect.gen(function* () {
        const through = yield* journal.getHighWaterSequence;
        const replayed = new Set<string>();
        const emittedText = new Map<string, number>();
        let before: number | undefined;
        for (;;) {
          const page = yield* journal.readThreadEvents({
            threadId,
            throughSequenceInclusive: through,
            limit: 200,
            ...(before === undefined ? {} : { beforeSequenceExclusive: before }),
          });
          for (const { event } of page) {
            replayed.add(event.eventId);
            if (event.type === "content.delta" && event.itemId !== undefined) {
              emittedText.set(
                event.itemId,
                (emittedText.get(event.itemId) ?? 0) + event.payload.delta.length,
              );
            }
          }
          if (page.length < 200) return { replayed, emittedText };
          before = page.at(-1)!.sequence;
        }
      }).pipe(
        Effect.mapError(
          (error) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "readThreadEvents",
              detail: `Synara could not read what it already holds of this Aether task: ${error.message}`,
              cause: error,
            }),
        ),
      );

    /** Starts the task's poll loop in the session's scope; the caller wakes it. */
    const follow = (session: Session, resume: AetherIngestResume) =>
      Effect.gen(function* () {
        session.poller = yield* makeAetherConversationPoller({
          client,
          context: {
            taskId: created(session).taskId,
            threadId: session.threadId,
            turnIdFor: (turnId) => turnIdFor(session, turnId),
          },
          resume,
          emit: (output) => handleOutput(session, output),
        }).pipe(Scope.provide(session.scope));
      });

    // -----------------------------------------------------------------------
    // Sessions
    // -----------------------------------------------------------------------

    const claimWorktree = (threadId: ThreadId, worktreePath: string) =>
      Effect.gen(function* () {
        const held = claims.get(threadId);
        if (held !== undefined) return held;
        const claim = yield* gitCore
          .claimWorktree(worktreePath, { threadId, provider: PROVIDER })
          .pipe(
            Scope.provide(adapterScope),
            Effect.mapError((error) =>
              error._tag === "WorktreeOwnedError"
                ? invalid("startSession", error.message)
                : new ProviderAdapterRequestError({
                    provider: PROVIDER,
                    method: "claimWorktree",
                    detail: error.message,
                    cause: error,
                  }),
            ),
          );
        claims.set(threadId, claim);
        return claim;
      });

    /** Claims the worktree, starts following a created task, and registers the session. */
    const attach = (input: {
      readonly threadId: ThreadId;
      readonly cursor: AetherResumeCursor;
      readonly runtimeMode: RuntimeMode;
      readonly model: string;
    }) =>
      Effect.gen(function* () {
        const claim = yield* claimWorktree(input.threadId, input.cursor.worktreePath);
        const at = yield* nowIso;
        const session: Session = {
          threadId: input.threadId,
          runtimeMode: input.runtimeMode,
          model: input.model,
          createdAt: at,
          updatedAt: at,
          scope: yield* Scope.fork(adapterScope, "sequential"),
          lock: yield* Semaphore.make(1),
          claim,
          cursor: input.cursor,
          poller: undefined,
          replayed: new Set(),
          remoteTurns: new Set(),
          interrupted: undefined,
          settledEarly: new Map(),
        };
        if (input.cursor.status === "created") {
          const { replayed, emittedText } = yield* readJournal(input.threadId).pipe(
            Effect.onError(() => Scope.close(session.scope, Exit.void)),
          );
          session.replayed = replayed;
          yield* follow(session, { after: input.cursor.after, emittedText });
        }
        sessions.set(input.threadId, session);
        return session;
      });

    const withSession = <A, E>(
      threadId: ThreadId,
      run: (session: Session) => Effect.Effect<A, E>,
    ): Effect.Effect<A, E | ProviderAdapterSessionNotFoundError> =>
      Effect.suspend((): Effect.Effect<A, E | ProviderAdapterSessionNotFoundError> => {
        const session = sessions.get(threadId);
        return session === undefined
          ? Effect.fail(new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }))
          : run(session);
      });

    const wake = (session: Session) => session.poller?.wake ?? Effect.void;

    const stopSession: AetherAdapterShape["stopSession"] = (threadId) =>
      Effect.suspend(() => {
        const session = sessions.get(threadId);
        if (session === undefined) return Effect.void;
        sessions.delete(threadId);
        return Scope.close(session.scope, Exit.void);
      });

    // -----------------------------------------------------------------------
    // Sends
    // -----------------------------------------------------------------------

    /**
     * One respond; returns the Synara turn it starts, whose id is the send's `client_message_id`.
     * A new turn is recorded before it goes out, so the poll loop binds its Aether turn at once.
     * A send Aether refused is forgotten; one whose response was lost is kept, because it may
     * have landed: its retry is the same request, so it has the same id and the same turn.
     */
    const respond = (
      session: Session,
      operation: string,
      body: Omit<AetherRespondToTaskRequest, "client_message_id">,
      turn?: { readonly aetherTurnId: AetherTurnId | null },
    ) =>
      Effect.gen(function* () {
        const cursor = created(session);
        const clientMessageId = clientMessageIdFor(cursor, body);
        const turnId = TurnId.makeUnsafe(clientMessageId);
        if (turn !== undefined && !cursor.turns.some((sent) => sent.turnId === turnId)) {
          session.cursor = {
            ...cursor,
            turns: [...cursor.turns, { turnId, clientMessageId, aetherTurnId: turn.aetherTurnId }],
          };
        }
        yield* client
          .respondToTask(cursor.taskId, { ...body, client_message_id: clientMessageId })
          .pipe(
            Effect.tapError((error) =>
              Effect.gen(function* () {
                if (sendRefused(error)) {
                  const current = created(session);
                  session.cursor = {
                    ...current,
                    turns: current.turns.filter((sent) => sent.turnId !== turnId),
                  };
                }
                // A stale answer: read the task again rather than retry.
                if (error._tag === "AetherApiConflictError") yield* wake(session);
              }),
            ),
            Effect.mapError(apiFailure(operation)),
          );
        const sent = created(session);
        session.cursor = { ...sent, sends: sent.sends + 1 };
        return turnId;
      });

    /** The first turn: creates the task. Its Synara turn id is the idempotency key. */
    const createTask = (session: Session, turn: TurnRequest) =>
      Effect.gen(function* () {
        if (session.cursor.status !== "creating")
          throw new Error("The Aether task exists already.");
        const { worktreePath, idempotencyKey } = session.cursor;
        const turnId = TurnId.makeUnsafe(idempotencyKey);
        const agentType = resolveAetherAgentFamily(session.model);
        if (agentType === null) {
          return yield* invalid("sendTurn", `Aether has no model ${session.model}.`);
        }
        const taskBase = yield* resolveAetherTaskBase({ cwd: worktreePath, client }).pipe(
          Effect.provideServices(services),
          Effect.mapError((error) =>
            error._tag === "ProviderAdapterValidationError" ||
            error._tag === "ProviderAdapterRequestError"
              ? error
              : apiFailure("listProjects")(error),
          ),
        );
        const task = yield* client
          .createTask({
            project_id: taskBase.projectId,
            prompt: turn.message,
            agent_type: agentType,
            model: session.model,
            interaction_mode: turn.mode,
            idempotency_key: idempotencyKey,
            base_branch: taskBase.baseBranch,
            ...(turn.effort === undefined ? {} : { reasoning_effort: turn.effort }),
            // The mirror needs the task to stay on its base commit.
            auto_fix_ci: false,
            auto_fix_pr_comments: false,
            auto_rebase: false,
          })
          .pipe(Effect.mapError(apiFailure("createTask")));
        session.cursor = {
          schemaVersion: 1,
          status: "created",
          worktreePath,
          taskId: task.id,
          projectId: taskBase.projectId,
          baseBranch: taskBase.baseBranch,
          baseCommit: taskBase.baseCommit,
          model: session.model,
          turns: [{ turnId, clientMessageId: null, aetherTurnId: null }],
          sends: 0,
          after: null,
          activeTurnId: null,
          mirrorPending: false,
        };
        yield* follow(session, { after: null, emittedText: new Map() });
        return turnId;
      });

    /** A later turn: a new message, or the answer to a proposed plan; with a note to show. */
    const respondTurn = (session: Session, turn: TurnRequest) =>
      Effect.gen(function* () {
        const page = yield* client
          .getConversationTurns(created(session).taskId)
          .pipe(Effect.mapError(apiFailure("getConversationTurns")));
        const awaiting =
          page.task.status === "awaiting_input" ? page.task.awaiting_input : undefined;
        if (awaiting?.kind === "questions") {
          return yield* invalid(
            "sendTurn",
            "Aether is waiting for an answer to its question. Answer it before sending a new message.",
          );
        }
        const note = yield* mirrorPending(session);
        if (awaiting?.kind === "plan") {
          // Synara's plan flow: implementing leaves plan mode; anything typed in plan mode is feedback.
          const waiting = page.turns.filter((candidate) => candidate.state === "awaiting_input");
          if (waiting.length !== 1) {
            return yield* invalid(
              "sendTurn",
              `Aether waits on a plan approval but ${waiting.length} turns are waiting.`,
            );
          }
          const data =
            turn.mode === "default"
              ? { approved: true }
              : { approved: false, feedback: turn.message };
          const turnId = yield* respond(
            session,
            "sendTurn",
            {
              message: JSON.stringify(data),
              tool_response: {
                tool_name: "propose_plan",
                data,
                expected_tool_id: awaiting.tool_id,
              },
            },
            { aetherTurnId: waiting[0]!.id },
          );
          return { turnId, note };
        }
        const turnId = yield* respond(
          session,
          "sendTurn",
          {
            message: turn.message,
            // A message sent while a run is busy waits for it instead of steering it.
            queue_for_later: true,
            interaction_mode: turn.mode,
            ...(turn.effort === undefined ? {} : { reasoning_effort: turn.effort }),
          },
          { aetherTurnId: null },
        );
        return { turnId, note };
      });

    const startTurn = (session: Session, input: ProviderSendTurnInput) =>
      Effect.gen(function* () {
        const message = yield* turnText(input, "sendTurn");
        // Debug mode is a prompt prefix the command reactor adds; Aether runs it as a default turn.
        const mode = input.interactionMode === "plan" ? "plan" : "default";
        const effort =
          input.modelSelection?.provider === PROVIDER
            ? input.modelSelection.options?.reasoningEffort
            : undefined;
        const turn: TurnRequest = { message, mode, effort };
        const { turnId, note } =
          session.cursor.status === "creating"
            ? { turnId: yield* createTask(session, turn), note: undefined }
            : yield* respondTurn(session, turn);
        session.cursor = { ...created(session), activeTurnId: turnId };
        session.interrupted = undefined;
        yield* publish(session, {
          ...(yield* base(session, turnId, ["started"])),
          type: "turn.started",
          payload: { model: session.model, ...(effort === undefined ? {} : { effort }) },
        });
        if (note !== undefined) yield* warn(session, turnId, ["mirror", "turn-start"], note);
        const early = session.settledEarly.get(turnId);
        if (early !== undefined) {
          session.settledEarly.delete(turnId);
          yield* complete(session, turnId, early.state, early.errors);
        }
        yield* wake(session);
        return { threadId: session.threadId, turnId, resumeCursor: session.cursor };
      });

    // -----------------------------------------------------------------------
    // Construction: claim again, and reattach to turns in flight
    // -----------------------------------------------------------------------

    for (const binding of options.bindings) {
      if (binding.provider !== PROVIDER) continue;
      yield* Effect.gen(function* () {
        const cursor = yield* parseAetherResumeCursor(binding.resumeCursor);
        if (cursor.status !== "created") return;
        yield* claimWorktree(binding.threadId, cursor.worktreePath);
        if (cursor.activeTurnId === null) return;
        if (binding.runtimeMode === undefined) {
          return yield* invalid("reattach", "The thread's provider binding has no runtime mode.");
        }
        const session = yield* attach({
          threadId: binding.threadId,
          cursor,
          runtimeMode: binding.runtimeMode,
          model: cursor.model,
        });
        yield* wake(session);
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logError("aether.reattach.failed", {
            threadId: binding.threadId,
            cause: Cause.pretty(cause),
          }),
        ),
      );
    }

    const notSupported = (method: string, detail: string) =>
      Effect.fail(new ProviderAdapterRequestError({ provider: PROVIDER, method, detail }));

    return {
      provider: PROVIDER,
      capabilities: {
        // The task's agent and model are fixed when it is created.
        sessionModelSwitch: "unsupported",
        conversationRollback: "restart-session",
        supportsSkillMentions: false,
        supportsSkillDiscovery: false,
        supportsNativeSlashCommandDiscovery: false,
        supportsPluginMentions: false,
        supportsPluginDiscovery: false,
        supportsRuntimeModelList: false,
        supportsTurnSteering: true,
        supportsLiveTurnDiffPatch: false,
        managesWorktreeBranch: true,
        turnsSurviveRestart: true,
      },
      startSession: (input) =>
        Effect.gen(function* () {
          const existing = sessions.get(input.threadId);
          if (existing !== undefined) {
            if (input.cwd !== existing.cursor.worktreePath) {
              return yield* invalid("startSession", "The Aether thread's worktree changed.");
            }
            return snapshot(existing);
          }
          const cwd = input.cwd;
          if (cwd === undefined) {
            return yield* invalid("startSession", "An Aether thread needs its own worktree.");
          }
          const cursor: AetherResumeCursor =
            input.resumeCursor === undefined
              ? yield* requireAetherWorktree(cwd).pipe(
                  Effect.provideServices(services),
                  Effect.as({
                    schemaVersion: 1,
                    status: "creating",
                    worktreePath: cwd,
                    idempotencyKey: randomUUID(),
                  } as const),
                )
              : yield* parseAetherResumeCursor(input.resumeCursor);
          if (cursor.worktreePath !== cwd) {
            return yield* invalid(
              "startSession",
              `The Aether task belongs to the worktree ${cursor.worktreePath}, not ${cwd}.`,
            );
          }
          const model =
            cursor.status === "created"
              ? cursor.model
              : input.modelSelection?.provider === PROVIDER
                ? input.modelSelection.model
                : undefined;
          if (model === undefined || resolveAetherAgentFamily(model) === null) {
            return yield* invalid("startSession", `Aether has no model ${model ?? "(none)"}.`);
          }
          const session = yield* attach({
            threadId: input.threadId,
            cursor,
            runtimeMode: input.runtimeMode,
            model,
          });
          // Reattached: read the task again, including anything answered in Aether meanwhile.
          yield* wake(session);
          return snapshot(session);
        }),
      sendTurn: (input) =>
        withSession(input.threadId, (session) =>
          session.lock.withPermit(startTurn(session, input)),
        ),
      steerTurn: (input) =>
        withSession(input.threadId, (session) =>
          session.lock.withPermit(
            Effect.gen(function* () {
              const activeTurnId =
                session.cursor.status === "created" ? session.cursor.activeTurnId : null;
              if (activeTurnId === null) return yield* startTurn(session, input);
              const message = yield* turnText(input, "steerTurn");
              // Without `queue_for_later`, Aether hands the message to the running agent.
              yield* respond(session, "steerTurn", { message });
              yield* wake(session);
              return {
                threadId: session.threadId,
                turnId: activeTurnId,
                resumeCursor: session.cursor,
              };
            }),
          ),
        ),
      interruptTurn: (threadId) =>
        withSession(threadId, (session) =>
          session.lock.withPermit(
            Effect.gen(function* () {
              if (session.cursor.status !== "created") return;
              const { taskId } = session.cursor;
              const page = yield* client
                .getConversationTurns(taskId)
                .pipe(Effect.mapError(apiFailure("getConversationTurns")));
              for (const turn of page.turns) {
                bindOrigin(session, turn.id, turn.root_message.clientMessageId);
              }
              yield* client
                .stopTask(taskId, { discard_queued_messages: true })
                .pipe(Effect.mapError(apiFailure("stopTask")));
              const discarded = page.turns
                .filter((turn) => turn.state === "queued")
                .map((turn) => turn.root_message.content);
              const activeTurnId = created(session).activeTurnId ?? undefined;
              if (discarded.length > 0) {
                yield* warn(
                  session,
                  activeTurnId,
                  ["stop-discarded", yield* nowIso],
                  `Stopping the Aether task discarded ${discarded.length} queued message${discarded.length === 1 ? "" : "s"}.`,
                  discarded,
                );
              }
              if (activeTurnId !== undefined) {
                session.interrupted = activeTurnId;
                const aetherTurnId = created(session).turns.findLast(
                  (turn) => turn.turnId === activeTurnId && turn.aetherTurnId !== null,
                )?.aetherTurnId;
                const state = page.turns.find((turn) => turn.id === aetherTurnId)?.state;
                // Never started, so no run will settle it: it ends now.
                if (aetherTurnId === undefined || state === "queued") {
                  const cursor = created(session);
                  session.cursor = {
                    ...cursor,
                    turns: cursor.turns.filter(
                      (turn) => turn.turnId !== activeTurnId || turn.aetherTurnId !== null,
                    ),
                  };
                  yield* complete(session, activeTurnId, "interrupted");
                }
              }
              yield* wake(session);
            }),
          ),
        ),
      respondToRequest: () => notSupported("respondToRequest", "Aether asks for no approvals."),
      respondToUserInput: (threadId, requestId, answers) =>
        withSession(threadId, (session) =>
          session.lock.withPermit(
            Effect.gen(function* () {
              const cursor = yield* requireTask(session, "respondToUserInput");
              const page = yield* client
                .getConversationTurns(cursor.taskId)
                .pipe(Effect.mapError(apiFailure("getConversationTurns")));
              const awaiting =
                page.task.status === "awaiting_input" ? page.task.awaiting_input : undefined;
              if (awaiting?.kind !== "questions" || awaiting.tool_id !== (requestId as string)) {
                yield* wake(session);
                return yield* invalid(
                  "respondToUserInput",
                  "Aether is no longer waiting for an answer to this question: it was answered or cancelled elsewhere.",
                );
              }
              const answer = askUserAnswer(awaiting.input, answers);
              if (Result.isFailure(answer)) {
                return yield* invalid(
                  "respondToUserInput",
                  `Synara cannot send this answer to Aether: ${answer.failure}.`,
                );
              }
              yield* respond(session, "respondToUserInput", {
                message: JSON.stringify(answer.success),
                // The tool id fences the answer to the question actually shown.
                tool_response: {
                  tool_name: "ask_user",
                  data: answer.success,
                  expected_tool_id: awaiting.tool_id,
                },
              });
              yield* wake(session);
            }),
          ),
        ),
      readThread: () =>
        notSupported("readThread", "Aether threads cannot be read back as a provider thread."),
      rollbackThread: () =>
        notSupported("rollbackThread", "Aether cannot roll back a task's conversation."),
      stopSession,
      stopAll: () => Effect.forEach([...sessions.keys()], stopSession, { discard: true }),
      listSessions: () => Effect.sync(() => [...sessions.values()].map(snapshot)),
      hasSession: (threadId) => Effect.sync(() => sessions.has(threadId)),
      streamEvents: Stream.fromQueue(events),
    } satisfies AetherAdapterShape;
  });

/** Connects to the task's workspace only if it is running; a stopped one is not booted. */
const openRunningWorkspace =
  (client: AetherApiClientShape): AetherWorkspaceOpener =>
  (workspaceId) =>
    connectAetherWorkspace({ client, workspaceId, start: false }).pipe(
      Effect.map((attach) =>
        attach._tag === "Connected" ? Option.some(attach.socket) : Option.none(),
      ),
      Effect.catchTag("AetherApiConflictError", () => Effect.succeed(Option.none())),
    );

export const AetherAdapterLive = Layer.effect(
  AetherAdapter,
  Effect.gen(function* () {
    const client = yield* AetherApiClient;
    const directory = yield* ProviderSessionDirectory;
    return yield* makeAetherAdapter({
      client,
      openWorkspace: openRunningWorkspace(client),
      journal: yield* ProviderRuntimeEventRepository,
      bindings: yield* directory.listBindings().pipe(Effect.orDie),
    });
  }),
);

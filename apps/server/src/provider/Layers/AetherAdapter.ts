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
 * (`aetherResumeCursor.ts`) carries everything a restart needs: at construction the adapter
 * claims every Aether thread's worktree again, and startup reconciliation reattaches each turn
 * still in flight (`reattachTurn`), or fails it with the reason it cannot.
 *
 * @module AetherAdapterLive
 */
import { createHash, randomUUID } from "node:crypto";

import {
  EventId,
  resolveAetherAgentFamily,
  TurnId,
  type ProviderInteractionMode,
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
  activeTurnId,
  awaitedTurn,
  ownerOf,
  parseAetherResumeCursor,
  type AetherCreatedCursor,
  type AetherCreatingCursor,
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
  /** Persisted bindings: the worktree of every Aether thread with a task is claimed again. */
  readonly bindings: ReadonlyArray<ProviderRuntimeBinding>;
}

/** A session, whatever its task's state. */
interface SessionBase {
  readonly threadId: ThreadId;
  readonly runtimeMode: RuntimeMode;
  readonly model: string;
  readonly createdAt: string;
  readonly scope: Scope.Closeable;
  /** Held by every call that changes the session, and by every poll loop output. */
  readonly lock: Semaphore.Semaphore;
  readonly claim: WorktreeClaim;
  /**
   * ProviderService's lifecycle generation for this session, stamped on every event: a request
   * the thread answers is routed by it, and events of a superseded generation are dropped.
   */
  lifecycleGeneration: string | undefined;
  updatedAt: string;
  /**
   * A stop came while a turn was in flight (ProviderService stops the session after every
   * interrupt): the session keeps following the task until that turn completes, then stops.
   */
  stopAfterTurn: boolean;
}

interface Session extends SessionBase {
  /** No task until the first turn creates one; then the task and the loop following it. */
  state: Creating | Task;
}

interface Creating {
  readonly _tag: "Creating";
  readonly cursor: AetherCreatingCursor;
}

interface Task {
  readonly _tag: "Task";
  /** Changed only through `transition`, under the session lock. */
  cursor: AetherCreatedCursor;
  readonly poller: AetherConversationPoller;
  /** Event ids the journal holds: a resumed poll loop derives them again, and they are dropped. */
  readonly replayed: ReadonlySet<string>;
  /** Aether turns that did not come from Synara (sent from Aether's own UI). */
  readonly remoteTurns: Set<AetherTurnId>;
  /** The Synara turn an interrupt was sent for: it completes as interrupted. */
  interrupted: TurnId | undefined;
}

/** The one writer of a task's cursor. Every caller holds the session lock. */
function transition(task: Task, change: (cursor: AetherCreatedCursor) => AetherCreatedCursor) {
  task.cursor = change(task.cursor);
}

type MirrorOutcome =
  | { readonly _tag: "Mirrored"; readonly note: string | undefined }
  /** The workspace is not running: mirror when the next turn starts or settles. */
  | { readonly _tag: "Deferred"; readonly note: string }
  | { readonly _tag: "Failed"; readonly note: string };

type TurnSettled = Extract<AetherIngestOutput, { readonly _tag: "TurnSettled" }>;

interface TurnRequest {
  readonly message: string;
  readonly mode: "default" | "plan";
  readonly effort: string | undefined;
  /** The turn implements a proposed plan (it carries `sourceProposedPlan`). */
  readonly implementsPlan: boolean;
}

/** Aether refused the request, so nothing changed; otherwise its response may just be lost. */
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

/**
 * An API failure as an adapter error. A request Aether refused changed nothing, so it is a
 * validation error, which the command reactor treats as rejected; any other failure (5xx,
 * timeout, transport) may have landed, so it stays a request error, which is uncertain.
 */
const apiFailure = (method: string) => (error: AetherApiError) =>
  sendRefused(error)
    ? new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation: method,
        issue: error.message,
        cause: error,
      })
    : new ProviderAdapterRequestError({
        provider: PROVIDER,
        method,
        detail: error.message,
        cause: error,
      });

/** Why a mirror failed; only a workspace that is not running defers it (`openWorkspace`). */
function mirrorFailure(
  error: AetherMirrorError | WorktreeOwnedError | AetherApiError | AetherWorkspaceSocketError,
): MirrorOutcome {
  if (error._tag !== "AetherMirrorError") {
    return { _tag: "Failed", note: `Synara could not mirror Aether's changes: ${error.message}` };
  }
  if (error.reason === "local-edits") {
    return {
      _tag: "Failed",
      note: `Synara paused mirroring Aether's changes: the worktree has local changes (${error.paths.join(", ") || error.detail}). Discard or move them; the next turn mirrors again. Nothing was overwritten.`,
    };
  }
  return {
    _tag: "Failed",
    note: `Synara could not mirror Aether's changes (${error.reason}): ${error.detail}`,
  };
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

/** The Aether interaction mode of a Synara turn. */
function aetherMode(mode: ProviderInteractionMode | undefined): "default" | "plan" {
  switch (mode) {
    case undefined:
    case "default":
    // Debug mode is a prompt prefix the command reactor adds; Aether runs it as a default turn.
    case "debug":
      return "default";
    case "plan":
      return "plan";
  }
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
      session.state._tag === "Task"
        ? Effect.succeed(session.state)
        : Effect.fail(invalid(operation, "No Aether task has started for this thread yet."));

    const snapshot = (session: Session): ProviderSession => {
      const { state } = session;
      const active = state._tag === "Task" ? activeTurnId(state.cursor) : null;
      return {
        provider: PROVIDER,
        status: active === null ? "ready" : "running",
        runtimeMode: session.runtimeMode,
        cwd: state.cursor.worktreePath,
        model: session.model,
        threadId: session.threadId,
        resumeCursor: state.cursor,
        ...(active === null ? {} : { activeTurnId: active }),
        createdAt: session.createdAt,
        updatedAt: session.updatedAt,
      };
    };

    // -----------------------------------------------------------------------
    // Events
    // -----------------------------------------------------------------------

    /** Every event goes out here; one the journal already holds is not sent again. */
    const publish = (session: SessionBase, task: Task, event: ProviderRuntimeEvent) =>
      task.replayed.has(event.eventId)
        ? Effect.void
        : Effect.suspend(() => {
            session.updatedAt = event.createdAt;
            return Queue.offer(
              events,
              session.lifecycleGeneration === undefined
                ? event
                : { ...event, lifecycleGeneration: session.lifecycleGeneration },
            );
          }).pipe(Effect.asVoid);

    const base = (
      session: SessionBase,
      task: Task,
      turnId: TurnId | undefined,
      parts: ReadonlyArray<string>,
    ) =>
      Effect.map(nowIso, (createdAt) => ({
        eventId: EventId.makeUnsafe(
          [
            "aether",
            task.cursor.taskId,
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
      session: SessionBase,
      task: Task,
      turnId: TurnId | undefined,
      parts: ReadonlyArray<string>,
      message: string,
      detail?: unknown,
    ) =>
      base(session, task, turnId, parts).pipe(
        Effect.flatMap((fields) =>
          publish(session, task, {
            ...fields,
            type: "runtime.warning",
            payload: { message, ...(detail === undefined ? {} : { detail }) },
          }),
        ),
      );

    /** The turn's one `turn.completed`; its id is the turn's, so a replay of it is dropped. */
    const complete = (
      session: SessionBase,
      task: Task,
      turnId: TurnId,
      state: "completed" | "failed" | "interrupted",
      errors: ReadonlyArray<string> = [],
    ) =>
      Effect.gen(function* () {
        transition(task, (cursor) => ({
          ...cursor,
          active: activeTurnId(cursor) === turnId ? false : cursor.active,
        }));
        yield* publish(session, task, {
          ...(yield* base(session, task, turnId, ["completed"])),
          type: "turn.completed",
          payload: { state, ...(errors.length === 0 ? {} : { errorMessage: errors.join("\n\n") }) },
        });
        // Outside the session's scope: this may run on its poll loop.
        if (session.stopAfterTurn) yield* detach(session).pipe(Effect.forkIn(adapterScope));
      });

    // -----------------------------------------------------------------------
    // Turn mapping
    // -----------------------------------------------------------------------

    /**
     * The Synara turn an Aether turn's events belong to: its owner (an approved plan moves its
     * Aether turn to the approving turn); for a turn not seen yet, the turn in flight if it still
     * waits for its Aether turn (its text can stream before its turn is listed); otherwise an id
     * of its own, for a turn sent from outside Synara.
     */
    const turnIdFor = (task: Task, aetherTurnId: AetherTurnId): TurnId => {
      const { cursor } = task;
      const owner = ownerOf(cursor, aetherTurnId);
      if (owner !== undefined) return owner;
      const active = activeTurnId(cursor);
      if (!task.remoteTurns.has(aetherTurnId) && active !== null) {
        if (awaitedTurn(cursor, active) === null) return active;
      }
      return TurnId.makeUnsafe(`aether-remote:${aetherTurnId}`);
    };

    /**
     * Binds a new Aether turn to the Synara turn that sent it: the task's own first turn has no
     * `client_message_id`, every other turn Synara started has one it derived. False otherwise.
     */
    const bindOrigin = (
      task: Task,
      aetherTurnId: AetherTurnId,
      clientMessageId: string | undefined,
    ): boolean => {
      const { cursor } = task;
      if (cursor.turns.some((turn) => turn.aetherTurnId === aetherTurnId)) return true;
      if (task.remoteTurns.has(aetherTurnId)) return false;
      const [first, ...sent] = cursor.turns;
      const bySend =
        clientMessageId === undefined
          ? -1
          : sent.findIndex(
              (turn) => turn.aetherTurnId === null && turn.clientMessageId === clientMessageId,
            );
      if (bySend !== -1) {
        transition(task, (current) => ({
          ...current,
          turns: [first, ...sent.with(bySend, { ...sent[bySend]!, aetherTurnId })],
        }));
        return true;
      }
      if (clientMessageId === undefined && first.aetherTurnId === null) {
        transition(task, (current) => ({
          ...current,
          turns: [{ ...first, aetherTurnId }, ...sent],
        }));
        return true;
      }
      task.remoteTurns.add(aetherTurnId);
      return false;
    };

    // -----------------------------------------------------------------------
    // Mirror
    // -----------------------------------------------------------------------

    /** Mirrors the VM's working tree into the worktree; never boots a suspended VM. */
    const mirror = (session: SessionBase, task: Task): Effect.Effect<MirrorOutcome> =>
      Effect.gen(function* () {
        const { cursor } = task;
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
    const mirrorPending = (session: SessionBase, task: Task) =>
      Effect.gen(function* () {
        if (!task.cursor.mirrorPending) return undefined;
        const outcome = yield* mirror(session, task);
        transition(task, (cursor) => ({ ...cursor, mirrorPending: outcome._tag === "Deferred" }));
        return outcome._tag === "Deferred" ? undefined : outcome.note;
      });

    // -----------------------------------------------------------------------
    // Poll loop outputs
    // -----------------------------------------------------------------------

    /** A run ended: mirror, record it, then complete its Synara turn if it is the one in flight. */
    const settle = (session: SessionBase, task: Task, settled: TurnSettled) =>
      Effect.gen(function* () {
        const turnId = ownerOf(task.cursor, settled.turnId);
        const outcome = yield* mirror(session, task);
        // The turn in flight completes when the Aether turn it waits on settles.
        const completes =
          turnId !== undefined &&
          activeTurnId(task.cursor) === turnId &&
          awaitedTurn(task.cursor, turnId) === settled.turnId;
        transition(task, (cursor) => ({
          ...cursor,
          after: { turnId: settled.turnId, runMessageId: settled.runMessageId },
          mirrorPending: outcome._tag === "Deferred",
        }));
        const failed = outcome._tag === "Failed" ? outcome.note : undefined;
        const note = completes && failed !== undefined ? undefined : outcome.note;
        if (note !== undefined) {
          yield* warn(
            session,
            task,
            turnId,
            ["mirror", settled.turnId, settled.runMessageId],
            note,
          );
        }
        if (!completes) return;
        const errors = [
          ...(settled.state === "errored"
            ? [settled.errorMessage ?? "Aether reported the turn as failed."]
            : []),
          ...(failed === undefined ? [] : [failed]),
        ];
        yield* complete(
          session,
          task,
          turnId,
          task.interrupted === turnId ? "interrupted" : errors.length > 0 ? "failed" : "completed",
          errors,
        );
      });

    /** The loop stopped: fail the turn in flight. The next message, answer or stop wakes it. */
    const pollingStopped = (
      session: SessionBase,
      task: Task,
      error: AetherConversationPollFailure,
    ) =>
      Effect.gen(function* () {
        const message = `Synara stopped following the Aether task: ${error.message} It follows it again at the next message.`;
        const turnId = activeTurnId(task.cursor);
        if (turnId !== null) return yield* complete(session, task, turnId, "failed", [message]);
        yield* warn(session, task, undefined, ["polling-stopped", yield* nowIso], message);
      });

    const handleOutput = (session: SessionBase, task: Task, output: AetherPollerOutput) => {
      switch (output._tag) {
        case "Event":
          return publish(session, task, output.event);
        case "TurnOrigin":
          return bindOrigin(task, output.turnId, output.clientMessageId)
            ? Effect.void
            : Effect.gen(function* () {
                yield* publish(session, task, {
                  eventId: EventId.makeUnsafe(
                    `aether:${task.cursor.taskId}:${output.turnId}:remote-origin`,
                  ),
                  provider: PROVIDER,
                  threadId: session.threadId,
                  createdAt: yield* nowIso,
                  turnId: turnIdFor(task, output.turnId),
                  type: "runtime.warning",
                  payload: {
                    message: "A message reached this Aether task from outside Synara.",
                    detail: output.rootMessageText,
                  },
                });
              });
        case "TurnSettled":
          return settle(session, task, output);
        case "PollingStopped":
          return pollingStopped(session, task, output.error);
        case "PendingInput":
        case "PendingInputResolved":
        case "ConversationReset":
          return Effect.void;
      }
    };

    /**
     * What the journal holds of the turn in flight: the ids a resumed poll loop derives again (they
     * are dropped), and the text already streamed. It holds every row of a turn until the turn
     * completes; a resumed loop emits nothing for the turns before it (`resume.after`), whose
     * rows the journal may have pruned.
     */
    const readJournal = (threadId: ThreadId, turnId: TurnId | null) =>
      Effect.gen(function* () {
        const replayed = new Set<string>();
        const emittedText = new Map<string, number>();
        if (turnId === null) return { replayed, emittedText };
        const through = yield* journal.getHighWaterSequence;
        let before: number | undefined;
        for (;;) {
          const page = yield* journal.readThreadEvents({
            threadId,
            turnId,
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

    /**
     * Starts following a created task: its poll loop runs in the session's scope, each output
     * under the session lock, and waits for the caller to wake it.
     */
    const follow = (
      session: SessionBase,
      cursor: AetherCreatedCursor,
      replayed: ReadonlySet<string>,
      resume: AetherIngestResume,
    ) =>
      Effect.gen(function* () {
        const poller = yield* makeAetherConversationPoller({
          client,
          context: {
            taskId: cursor.taskId,
            threadId: session.threadId,
            turnIdFor: (turnId) => turnIdFor(task, turnId),
          },
          resume,
          emit: (output) => session.lock.withPermit(handleOutput(session, task, output)),
        }).pipe(Scope.provide(session.scope));
        const task: Task = {
          _tag: "Task",
          cursor,
          poller,
          replayed,
          remoteTurns: new Set(),
          interrupted: undefined,
        };
        return task;
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
      readonly lifecycleGeneration: string | undefined;
    }) =>
      Effect.gen(function* () {
        const { cursor } = input;
        const claim = yield* claimWorktree(input.threadId, cursor.worktreePath);
        const at = yield* nowIso;
        const base: SessionBase = {
          threadId: input.threadId,
          runtimeMode: input.runtimeMode,
          model: input.model,
          createdAt: at,
          updatedAt: at,
          scope: yield* Scope.fork(adapterScope, "sequential"),
          lock: yield* Semaphore.make(1),
          claim,
          lifecycleGeneration: input.lifecycleGeneration,
          stopAfterTurn: false,
        };
        const state: Creating | Task =
          cursor.status === "creating"
            ? { _tag: "Creating", cursor }
            : yield* readJournal(input.threadId, activeTurnId(cursor)).pipe(
                Effect.flatMap(({ replayed, emittedText }) =>
                  follow(base, cursor, replayed, { after: cursor.after, emittedText }),
                ),
                Effect.onError(() => Scope.close(base.scope, Exit.void)),
              );
        // The same object the poll loop's outputs hold, now with its state.
        const session: Session = Object.assign(base, { state });
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

    /** Stops following the task; it keeps running in the cloud. */
    const detach = (session: SessionBase) =>
      Effect.suspend(() => {
        if (sessions.get(session.threadId) === session) sessions.delete(session.threadId);
        return Scope.close(session.scope, Exit.void);
      });

    /**
     * Detaches, once no turn is in flight: a turn in flight completes only after its run
     * settles and is mirrored, so until then the session keeps following it and reports it.
     */
    const stopSession: AetherAdapterShape["stopSession"] = (threadId) =>
      Effect.suspend(() => {
        const session = sessions.get(threadId);
        if (session === undefined) return Effect.void;
        const { state } = session;
        if (state._tag === "Task" && activeTurnId(state.cursor) !== null) {
          session.stopAfterTurn = true;
          return Effect.void;
        }
        return detach(session);
      });

    // -----------------------------------------------------------------------
    // Sends
    // -----------------------------------------------------------------------

    /**
     * One respond; returns its `client_message_id`. A send that can start an Aether turn is
     * recorded before it goes out, so the poll loop binds that turn at once: as a turn of its own
     * (whose id is the `client_message_id`), or as part of `send.turnId` (a steer). A send Aether
     * refused is forgotten; one whose response was lost is kept, because it may have landed: its
     * retry is the same request, so it has the same id and the same turn.
     */
    const respond = (
      task: Task,
      operation: string,
      body: Omit<AetherRespondToTaskRequest, "client_message_id">,
      send?: { readonly turnId?: TurnId; readonly aetherTurnId: AetherTurnId | null },
    ) =>
      Effect.gen(function* () {
        const clientMessageId = clientMessageIdFor(task.cursor, body);
        const [, ...recorded] = task.cursor.turns;
        if (
          send !== undefined &&
          !recorded.some((sent) => sent.clientMessageId === clientMessageId)
        ) {
          const turnId = send.turnId ?? TurnId.makeUnsafe(clientMessageId);
          transition(task, (cursor) => ({
            ...cursor,
            turns: [...cursor.turns, { turnId, clientMessageId, aetherTurnId: send.aetherTurnId }],
          }));
        }
        yield* client
          .respondToTask(task.cursor.taskId, { ...body, client_message_id: clientMessageId })
          .pipe(
            Effect.tapError((error) =>
              Effect.gen(function* () {
                if (sendRefused(error)) {
                  transition(task, (cursor) => {
                    const [first, ...sent] = cursor.turns;
                    return {
                      ...cursor,
                      turns: [
                        first,
                        ...sent.filter((other) => other.clientMessageId !== clientMessageId),
                      ],
                    };
                  });
                }
                // A stale answer: read the task again rather than retry.
                if (error._tag === "AetherApiConflictError") yield* task.poller.wake;
              }),
            ),
            Effect.mapError(apiFailure(operation)),
          );
        transition(task, (cursor) => ({ ...cursor, sends: cursor.sends + 1 }));
        return clientMessageId;
      });

    /**
     * The first turn: creates the task and starts following it. Its Synara turn id is the
     * idempotency key.
     */
    const createTask = (session: Session, creating: AetherCreatingCursor, turn: TurnRequest) =>
      Effect.gen(function* () {
        const { worktreePath, idempotencyKey } = creating;
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
        const created = yield* client
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
          .pipe(
            Effect.tapError((error) =>
              Effect.sync(() => {
                // Nothing was created: the next send starts over with a new key.
                if (sendRefused(error)) {
                  session.state = {
                    _tag: "Creating",
                    cursor: { ...creating, idempotencyKey: randomUUID() },
                  };
                }
              }),
            ),
            Effect.mapError(apiFailure("createTask")),
          );
        const task = yield* follow(
          session,
          {
            schemaVersion: 1,
            status: "created",
            worktreePath,
            taskId: created.id,
            projectId: taskBase.projectId,
            baseBranch: taskBase.baseBranch,
            baseCommit: taskBase.baseCommit,
            model: session.model,
            turns: [{ turnId, aetherTurnId: null }],
            sends: 0,
            after: null,
            active: false,
            mirrorPending: false,
          },
          new Set(),
          { after: null, emittedText: new Map() },
        );
        session.state = task;
        return { task, turnId, note: undefined, ended: undefined };
      });

    /** A later turn: a new message, or the answer to a proposed plan; with a note to show. */
    const respondTurn = (session: SessionBase, task: Task, turn: TurnRequest) =>
      Effect.gen(function* () {
        const page = yield* client
          .getConversationTurns(task.cursor.taskId)
          .pipe(Effect.mapError(apiFailure("getConversationTurns")));
        const awaiting =
          page.task.status === "awaiting_input" ? page.task.awaiting_input : undefined;
        if (awaiting?.kind === "questions") {
          return yield* invalid(
            "sendTurn",
            "Aether is waiting for an answer to its question. Answer it before sending a new message.",
          );
        }
        const note = yield* mirrorPending(session, task);
        if (awaiting?.kind === "plan") {
          const waiting = page.turns.filter((candidate) => candidate.state === "awaiting_input");
          if (waiting.length !== 1) {
            return yield* invalid(
              "sendTurn",
              `Aether waits on a plan approval but ${waiting.length} turns are waiting.`,
            );
          }
          // Synara's Implement action approves the plan and leaves plan mode (Aether keeps the
          // mode a plan answer omits); any other message is feedback, and planning goes on.
          const data = turn.implementsPlan
            ? { approved: true }
            : { approved: false, feedback: turn.message };
          const turnId = TurnId.makeUnsafe(
            yield* respond(
              task,
              "sendTurn",
              {
                message: JSON.stringify(data),
                ...(turn.implementsPlan ? { interaction_mode: "default" as const } : {}),
                tool_response: {
                  tool_name: "propose_plan",
                  data,
                  expected_tool_id: awaiting.tool_id,
                },
              },
              { aetherTurnId: waiting[0]!.id },
            ),
          );
          return { task, turnId, note, ended: undefined };
        }
        const body = {
          message: turn.message,
          // A message sent while a run is busy waits for it instead of steering it.
          queue_for_later: true,
          interaction_mode: turn.mode,
          ...(turn.effort === undefined ? {} : { reasoning_effort: turn.effort }),
        };
        // A retry of a send whose response was lost, even across a restart, is the same request
        // with the same id: its Aether turn may have started, and even settled, meanwhile.
        const landed = page.turns.find(
          (candidate) =>
            candidate.root_message.clientMessageId === clientMessageIdFor(task.cursor, body),
        );
        const turnId = TurnId.makeUnsafe(
          yield* respond(task, "sendTurn", body, { aetherTurnId: landed?.id ?? null }),
        );
        if (landed?.state === "settled") return { task, turnId, note, ended: [] };
        if (landed?.state === "errored") {
          const error =
            page.task.status === "errored"
              ? page.task.error
              : "Aether reported the turn as failed.";
          return { task, turnId, note, ended: [error] };
        }
        return { task, turnId, note, ended: undefined };
      });

    const startTurn = (session: Session, input: ProviderSendTurnInput) =>
      Effect.gen(function* () {
        const effort =
          input.modelSelection?.provider === PROVIDER
            ? input.modelSelection.options?.reasoningEffort
            : undefined;
        const turn: TurnRequest = {
          message: yield* turnText(input, "sendTurn"),
          mode: aetherMode(input.interactionMode),
          effort,
          implementsPlan: input.sourceProposedPlan !== undefined,
        };
        const { state } = session;
        const { task, turnId, note, ended } =
          state._tag === "Creating"
            ? yield* createTask(session, state.cursor, turn)
            : yield* respondTurn(session, state, turn);
        transition(task, (cursor) => ({ ...cursor, active: true }));
        task.interrupted = undefined;
        yield* publish(session, task, {
          ...(yield* base(session, task, turnId, ["started"])),
          type: "turn.started",
          payload: { model: session.model, ...(effort === undefined ? {} : { effort }) },
        });
        if (note !== undefined) yield* warn(session, task, turnId, ["mirror", "turn-start"], note);
        // Its run already settled, and was mirrored when it did: the turn ends now.
        if (ended !== undefined) {
          yield* complete(session, task, turnId, ended.length > 0 ? "failed" : "completed", ended);
        }
        yield* task.poller.wake;
        return { threadId: session.threadId, turnId, resumeCursor: task.cursor };
      });

    // -----------------------------------------------------------------------
    // Construction: claim every task's worktree again
    // -----------------------------------------------------------------------

    // A claim that fails here fails loudly again when the thread is next used (`attach`).
    for (const binding of options.bindings) {
      if (binding.provider !== PROVIDER) continue;
      yield* Effect.gen(function* () {
        const cursor = yield* parseAetherResumeCursor(binding.resumeCursor);
        if (cursor.status === "created") {
          yield* claimWorktree(binding.threadId, cursor.worktreePath);
        }
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logError("aether.claim.failed", {
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
        // No `conversationRollback`: a rollback reaches `rollbackThread`, which refuses. A
        // restart-session rollback would clear the cursor and orphan the task.
        supportsSkillMentions: false,
        supportsSkillDiscovery: false,
        supportsNativeSlashCommandDiscovery: false,
        supportsPluginMentions: false,
        supportsPluginDiscovery: false,
        supportsRuntimeModelList: false,
        supportsTurnSteering: true,
        supportsLiveTurnDiffPatch: false,
        managesWorktreeBranch: true,
      },
      startSession: (input) =>
        Effect.gen(function* () {
          const existing = sessions.get(input.threadId);
          if (existing !== undefined) {
            if (input.cwd !== existing.state.cursor.worktreePath) {
              return yield* invalid("startSession", "The Aether thread's worktree changed.");
            }
            // Wanted again: a stop that waited on the turn in flight no longer applies.
            existing.stopAfterTurn = false;
            existing.lifecycleGeneration = input.lifecycleGeneration;
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
            lifecycleGeneration: input.lifecycleGeneration,
          });
          // Reattached: read the task again, including anything answered in Aether meanwhile.
          if (session.state._tag === "Task") yield* session.state.poller.wake;
          return snapshot(session);
        }),
      reattachTurn: (input) =>
        Effect.gen(function* () {
          const cursor = yield* parseAetherResumeCursor(input.resumeCursor);
          if (cursor.status === "creating") {
            // Its request (the message) is not persisted, so the create cannot be retried here;
            // the key is, so sending the same message again continues any task it created.
            return yield* invalid(
              "reattachTurn",
              "Synara stopped before Aether confirmed creating the task for this thread's first message. Send that message again: if Aether created the task, it continues it.",
            );
          }
          if (activeTurnId(cursor) !== input.turnId) {
            return yield* invalid(
              "reattachTurn",
              `The Aether task has no turn ${input.turnId} in flight.`,
            );
          }
          const session = yield* attach({
            threadId: input.threadId,
            cursor,
            runtimeMode: input.runtimeMode,
            model: cursor.model,
            lifecycleGeneration: input.lifecycleGeneration,
          });
          if (session.state._tag === "Task") yield* session.state.poller.wake;
        }),
      sendTurn: (input) =>
        withSession(input.threadId, (session) =>
          session.lock.withPermit(startTurn(session, input)),
        ),
      steerTurn: (input) =>
        withSession(input.threadId, (session) =>
          session.lock.withPermit(
            Effect.gen(function* () {
              const task = session.state;
              const active = task._tag === "Task" ? activeTurnId(task.cursor) : null;
              if (task._tag === "Creating" || active === null) {
                return yield* startTurn(session, input);
              }
              const message = yield* turnText(input, "steerTurn");
              const page = yield* client
                .getConversationTurns(task.cursor.taskId)
                .pipe(Effect.mapError(apiFailure("getConversationTurns")));
              const awaited = awaitedTurn(task.cursor, active);
              const running = page.turns.find(
                (turn) =>
                  turn.id === awaited &&
                  (turn.state === "running" || turn.state === "awaiting_input"),
              );
              // Without `queue_for_later`, Aether hands the message to the running agent. When the
              // turn's run already settled (Synara has not seen it yet) or has not started, the
              // steer runs as a new Aether turn of the same Synara turn, which then waits on it.
              yield* respond(
                task,
                "steerTurn",
                running === undefined ? { message, queue_for_later: true } : { message },
                { turnId: active, aetherTurnId: running?.id ?? null },
              );
              yield* task.poller.wake;
              return { threadId: session.threadId, turnId: active, resumeCursor: task.cursor };
            }),
          ),
        ),
      interruptTurn: (threadId) =>
        withSession(threadId, (session) =>
          session.lock.withPermit(
            Effect.gen(function* () {
              const task = session.state;
              if (task._tag === "Creating") return;
              const { taskId } = task.cursor;
              const page = yield* client
                .getConversationTurns(taskId)
                .pipe(Effect.mapError(apiFailure("getConversationTurns")));
              for (const turn of page.turns) {
                bindOrigin(task, turn.id, turn.root_message.clientMessageId);
              }
              yield* client
                .stopTask(taskId, { discard_queued_messages: true })
                .pipe(Effect.mapError(apiFailure("stopTask")));
              const discarded = page.turns
                .filter((turn) => turn.state === "queued")
                .map((turn) => turn.root_message.content);
              const active = activeTurnId(task.cursor) ?? undefined;
              if (discarded.length > 0) {
                yield* warn(
                  session,
                  task,
                  active,
                  ["stop-discarded", yield* nowIso],
                  `Stopping the Aether task discarded ${discarded.length} queued message${discarded.length === 1 ? "" : "s"}.`,
                  discarded,
                );
              }
              if (active !== undefined) {
                task.interrupted = active;
                const aetherTurnId = awaitedTurn(task.cursor, active);
                const state = page.turns.find((turn) => turn.id === aetherTurnId)?.state;
                // Never started, so no run will settle it: it ends now.
                if (aetherTurnId === null || state === "queued") {
                  yield* complete(session, task, active, "interrupted");
                }
              }
              yield* task.poller.wake;
            }),
          ),
        ),
      respondToRequest: () => notSupported("respondToRequest", "Aether asks for no approvals."),
      respondToUserInput: (threadId, requestId, answers) =>
        withSession(threadId, (session) =>
          session.lock.withPermit(
            Effect.gen(function* () {
              const task = yield* requireTask(session, "respondToUserInput");
              const page = yield* client
                .getConversationTurns(task.cursor.taskId)
                .pipe(Effect.mapError(apiFailure("getConversationTurns")));
              const awaiting =
                page.task.status === "awaiting_input" ? page.task.awaiting_input : undefined;
              if (awaiting?.kind !== "questions" || awaiting.tool_id !== (requestId as string)) {
                yield* task.poller.wake;
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
              yield* respond(task, "respondToUserInput", {
                message: JSON.stringify(answer.success),
                // The tool id fences the answer to the question actually shown.
                tool_response: {
                  tool_name: "ask_user",
                  data: answer.success,
                  expected_tool_id: awaiting.tool_id,
                },
              });
              yield* task.poller.wake;
            }),
          ),
        ),
      readThread: () =>
        notSupported("readThread", "Aether threads cannot be read back as a provider thread."),
      rollbackThread: () =>
        notSupported("rollbackThread", "Aether cannot roll back a task's conversation."),
      stopSession,
      stopAll: () => Effect.forEach([...sessions.values()], detach, { discard: true }),
      listSessions: () => Effect.sync(() => [...sessions.values()].map(snapshot)),
      hasSession: (threadId) => Effect.sync(() => sessions.has(threadId)),
      streamEvents: Stream.fromQueue(events),
    } satisfies AetherAdapterShape;
  });

/**
 * Connects to the task's workspace only if it is running; a stopped one is not booted. `None`
 * while it is starting, or its connect is refused for its state (stopping, stopped, suspended);
 * any other failure is an error.
 */
export const openRunningWorkspace =
  (client: AetherApiClientShape): AetherWorkspaceOpener =>
  (workspaceId) =>
    connectAetherWorkspace({ client, workspaceId, start: false }).pipe(
      Effect.map((attach) =>
        attach._tag === "Connected" ? Option.some(attach.socket) : Option.none(),
      ),
      Effect.catchTag("AetherApiConflictError", (error) =>
        "kind" in error.body ? Effect.succeed(Option.none()) : Effect.fail(error),
      ),
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

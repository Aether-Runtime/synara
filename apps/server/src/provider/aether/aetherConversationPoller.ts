// FILE: aetherConversationPoller.ts
// Purpose: The one poll loop per Aether task that feeds `AetherConversationIngest`.
// Layer: Server provider boundary (Aether).
//
// Each tick reads `GET /conversation/live` for the messages open at the previous tick. Only when
// `headRevision` moves does it read `GET /conversation/turns` (which carries the task), and then
// `GET /turns/{id}/activity` for each turn whose `revision_sequence` moved. While the task is
// queued (its workspace boots), the tick reads a one-turn turns page instead of `/live`: nothing
// runs, the head stays put, and only the turns page carries the task's status line, which the
// adapter shows as boot progress. It is one small read a tick either way; the whole page is read
// only when the head moves. One fiber makes every
// request in turn, so two requests for the task are never in flight together. A tick is applied
// to a fork of the ingest and emitted only once every read in it succeeded: a failed tick
// changes nothing, so the next tick (or the next `wake`) derives the same outputs again.
//
// The loop ticks every second while a run executes, the task is busy, or a turn has not ended
// and been signalled; otherwise it goes idle until `wake`. While a surfaced question or plan
// waits on the user and nothing runs, it reads `/live` every 15 s instead, so an answer given
// in Aether's own UI shows within one slow tick; `wake` (the adapter sent an answer) ticks at
// once. Once idle, while the task's open pull request has CI or an Aether Review running, it
// reads the task (a one-turn page) once a minute, so the thread's PR shows them settle. After each
// tick it hands over the task as the ingest then holds it (`TaskUpdated`): settling a run reads
// the task again, and hands that newer read back (`noteTask`). A transport failure backs off (1 s, doubling to 10 s) with one warning while degraded;
// an outage longer than two minutes, or an error retrying cannot fix, stops the loop with a
// typed failure until the next `wake`.

import { Cause, Clock, Effect, Latch, Option, Schema, Scope } from "effect";

import type { AetherApiError } from "./aetherApiErrors.ts";
import type {
  AetherConversationTurnsPage,
  AetherTask,
  AetherTimelineMessage,
  AetherTurnActivityPage,
  AetherTurnId,
} from "./aetherApiSchemas.ts";
import type { AetherApiClientShape } from "../Services/AetherApiClient.ts";
import {
  AetherConversationIngest,
  makeAetherEventBase,
  type AetherIngestContext,
  type AetherIngestOutput,
  type AetherIngestResume,
  type AetherPendingInputError,
  type AetherTranscriptDivergedError,
} from "./aetherConversationIngest.ts";

/** Time between the end of one tick and the start of the next. */
export const AETHER_POLL_INTERVAL_MS = 1_000;
/** The same while a surfaced question or plan waits on the user and nothing runs. */
export const AETHER_POLL_AWAITING_INTERVAL_MS = 15_000;
/** Between reads of an idle task whose pull request's CI or review runs. */
export const AETHER_POLL_PULL_REQUEST_INTERVAL_MS = 60_000;
/** First retry delay after a transport failure; it doubles up to the maximum. */
export const AETHER_POLL_BACKOFF_INITIAL_MS = 1_000;
export const AETHER_POLL_BACKOFF_MAX_MS = 10_000;
/** How long the API may stay unreachable before the loop stops with a failure. */
export const AETHER_POLL_OUTAGE_LIMIT_MS = 120_000;
/**
 * After `wake`, how long to keep polling while the task still looks idle: the message just sent
 * may not show on the task yet.
 */
export const AETHER_POLL_WAKE_GRACE_MS = 30_000;
/** The turns page size: page 1 of the conversation. */
export const AETHER_TURNS_PAGE_LIMIT = 200;
/** A bound on paging back through one turn's activity in one tick. */
export const AETHER_ACTIVITY_MAX_PAGES = 50;

/** The API could not be reached for longer than `AETHER_POLL_OUTAGE_LIMIT_MS`. */
export class AetherPollOutageError extends Schema.TaggedErrorClass<AetherPollOutageError>()(
  "AetherPollOutageError",
  {
    taskId: Schema.String,
    outageMs: Schema.Number,
    lastError: Schema.Defect,
  },
) {
  override get message(): string {
    const last = this.lastError instanceof Error ? ` Last error: ${this.lastError.message}` : "";
    return `Synara could not reach Aether for ${Math.round(this.outageMs / 1000)}s while following task ${this.taskId}.${last}`;
  }
}

/** A turn's activity did not fit in `AETHER_ACTIVITY_MAX_PAGES` pages. */
export class AetherActivityPagingLimitError extends Schema.TaggedErrorClass<AetherActivityPagingLimitError>()(
  "AetherActivityPagingLimitError",
  {
    taskId: Schema.String,
    turnId: Schema.String,
    pages: Schema.Number,
  },
) {
  override get message(): string {
    return `Aether turn ${this.turnId} has more activity than ${this.pages} pages; Synara stopped reading it.`;
  }
}

/** The poll loop hit a bug, not an API failure. */
export class AetherPollDefectError extends Schema.TaggedErrorClass<AetherPollDefectError>()(
  "AetherPollDefectError",
  { taskId: Schema.String, cause: Schema.Defect },
) {
  override get message(): string {
    return `Following Aether task ${this.taskId} failed unexpectedly: ${Cause.pretty(Cause.die(this.cause))}`;
  }
}

export type AetherConversationPollFailure =
  | AetherApiError
  | AetherTranscriptDivergedError
  | AetherPendingInputError
  | AetherPollOutageError
  | AetherActivityPagingLimitError
  | AetherPollDefectError;

export type AetherPollerOutput =
  | AetherIngestOutput
  /** A turns read showed the task changed: its status line, branch, or pull request. */
  | { readonly _tag: "TaskUpdated"; readonly task: AetherTask }
  /** The loop stopped; the adapter fails the active turn. `wake` starts it again. */
  | { readonly _tag: "PollingStopped"; readonly error: AetherConversationPollFailure };

/** The three reads the loop makes. */
export type AetherConversationReader = Pick<
  AetherApiClientShape,
  "getLiveConversation" | "getConversationTurns" | "getTurnActivity"
>;

export interface AetherConversationPollerOptions {
  readonly client: AetherConversationReader;
  readonly context: AetherIngestContext;
  /** Where to pick up, from what Synara persisted: the last signalled run and emitted text. */
  readonly resume: AetherIngestResume;
  /** Receives every output in order. The loop waits for it, so a slow consumer slows polling. */
  readonly emit: (output: AetherPollerOutput) => Effect.Effect<void>;
}

export interface AetherConversationPoller {
  /** Polls now if idle, after a turn is sent or on reattach; keeps an active loop going. */
  readonly wake: Effect.Effect<void>;
  /** A turns page the consumer read itself: see `AetherConversationIngest.noteTask`. */
  readonly noteTask: (page: AetherConversationTurnsPage) => Effect.Effect<void>;
}

type TickFailure =
  | AetherApiError
  | AetherTranscriptDivergedError
  | AetherPendingInputError
  | AetherActivityPagingLimitError;

/** Transport trouble and server-side errors may pass; anything else will not. */
export function isRetryableAetherPollError(error: TickFailure): boolean {
  switch (error._tag) {
    case "AetherApiTransportError":
    case "AetherApiTimeoutError":
      return true;
    case "AetherApiHttpError":
      return error.status >= 500 || error.status === 429 || error.status === 408;
    default:
      return false;
  }
}

/** Starts the task's poll loop in the scope, idle until the first `wake`. */
export const makeAetherConversationPoller = (
  options: AetherConversationPollerOptions,
): Effect.Effect<AetherConversationPoller, never, Scope.Scope> =>
  Effect.gen(function* () {
    const { client, context, emit } = options;
    let ingest = new AetherConversationIngest(context, options.resume);
    const wakeLatch = yield* Latch.make(false);
    const wakeState = { graceUntil: 0, sawActive: false };
    /** The task last handed over (`TaskUpdated`), as JSON. */
    let emittedTask: string | undefined;

    const readActivity = (draft: AetherConversationIngest, turnId: AetherTurnId) =>
      Effect.gen(function* () {
        const pages: AetherTurnActivityPage[] = [];
        const readIds = new Set<string>();
        let page = yield* client.getTurnActivity(context.taskId, turnId);
        for (;;) {
          pages.push(page);
          for (const item of page.items) readIds.add(item.id);
          if (!draft.needsOlderActivity(turnId, page, readIds)) break;
          if (pages.length >= AETHER_ACTIVITY_MAX_PAGES) {
            return yield* new AetherActivityPagingLimitError({
              taskId: context.taskId,
              turnId,
              pages: pages.length,
            });
          }
          page = yield* client.getTurnActivity(context.taskId, turnId, {
            before: page.oldest_sequence_loaded ?? 0,
          });
        }
        const items: AetherTimelineMessage[] = pages.toReversed().flatMap((read) => read.items);
        return { items, revision: pages[0]!.turn_revision_sequence };
      });

    /** One tick on a fork of the ingest; the fork replaces it only when the tick succeeds. */
    const tick = (observedAt: string) =>
      Effect.gen(function* () {
        const draft = ingest.fork();
        const outputs: AetherPollerOutput[] = [];
        const readTurns = client.getConversationTurns(context.taskId, {
          limit: AETHER_TURNS_PAGE_LIMIT,
        });
        if (draft.readsTurnsInPlaceOfLive()) {
          const first = yield* client.getConversationTurns(context.taskId, { limit: 1 });
          if (draft.applyTaskInPlaceOfLive(first)) {
            outputs.push(...draft.applyTurnsInPlaceOfLive(yield* readTurns));
          }
        } else {
          const live = yield* client.getLiveConversation(context.taskId, draft.openMessageIds());
          const step = yield* draft.applyLive(live, observedAt);
          outputs.push(...step.outputs);
          if (step.refreshTurns) outputs.push(...draft.applyTurns(yield* readTurns));
        }
        for (const turnId of draft.turnsNeedingActivity()) {
          const { items, revision } = yield* readActivity(draft, turnId);
          outputs.push(...(yield* draft.applyActivity(turnId, items, revision, observedAt)));
        }
        outputs.push(...(yield* draft.finishRefresh(observedAt)));
        ingest = draft;
        return outputs;
      });

    const stop = (error: AetherConversationPollFailure) => emit({ _tag: "PollingStopped", error });

    /** Polls until the task goes idle or the loop stops with a failure. */
    const pollUntilIdle = Effect.gen(function* () {
      let backoffMs = AETHER_POLL_BACKOFF_INITIAL_MS;
      let failingSince: number | undefined;
      for (;;) {
        const now = yield* Clock.currentTimeMillis;
        const exit = yield* Effect.exit(tick(new Date(now).toISOString()));
        if (exit._tag === "Success") {
          yield* Effect.forEach(exit.value, emit, { discard: true });
          // Last, and as the ingest holds it after the outputs: settling a run may have handed
          // over a newer read (`noteTask`) than the tick's own.
          const task = ingest.task();
          const taskJson = JSON.stringify(task);
          if (task !== undefined && taskJson !== emittedTask) {
            emittedTask = taskJson;
            yield* emit({ _tag: "TaskUpdated", task });
          }
          backoffMs = AETHER_POLL_BACKOFF_INITIAL_MS;
          failingSince = undefined;
          if (ingest.shouldKeepPolling()) {
            wakeState.sawActive = true;
          } else if (wakeState.sawActive || now >= wakeState.graceUntil) {
            const waitsOnUser = ingest.waitsOnUser();
            const watchesPullRequest = ingest.watchesPullRequest();
            if (!waitsOnUser && !watchesPullRequest) return;
            // Someone may answer in Aether's own UI, or the PR's CI or review may move: look
            // again now and then, or at a wake.
            yield* Effect.raceFirst(
              Effect.sleep(
                waitsOnUser
                  ? AETHER_POLL_AWAITING_INTERVAL_MS
                  : AETHER_POLL_PULL_REQUEST_INTERVAL_MS,
              ),
              wakeLatch.await,
            );
            yield* wakeLatch.close;
            if (watchesPullRequest) ingest.refreshTask();
            continue;
          }
          yield* Effect.sleep(AETHER_POLL_INTERVAL_MS);
          continue;
        }
        if (Cause.hasInterruptsOnly(exit.cause)) return yield* Effect.interrupt;
        const failure = Cause.findErrorOption(exit.cause);
        if (Option.isNone(failure)) {
          return yield* stop(
            new AetherPollDefectError({ taskId: context.taskId, cause: Cause.squash(exit.cause) }),
          );
        }
        const error = failure.value;
        if (!isRetryableAetherPollError(error)) return yield* stop(error);
        if (failingSince === undefined) {
          failingSince = now;
          // One warning per degraded stretch, named by when it began.
          yield* emit({
            _tag: "Event",
            event: {
              ...makeAetherEventBase(
                context,
                undefined,
                ["poll-degraded", String(now)],
                new Date(now).toISOString(),
              ),
              type: "runtime.warning",
              payload: {
                message: "Synara lost contact with Aether and is retrying.",
                detail: error.message,
              },
            },
          });
        }
        const outageMs = now - failingSince;
        if (outageMs >= AETHER_POLL_OUTAGE_LIMIT_MS) {
          return yield* stop(
            new AetherPollOutageError({ taskId: context.taskId, outageMs, lastError: error }),
          );
        }
        yield* Effect.sleep(backoffMs);
        backoffMs = Math.min(backoffMs * 2, AETHER_POLL_BACKOFF_MAX_MS);
      }
    });

    yield* Effect.forkScoped(
      Effect.forever(
        Effect.gen(function* () {
          yield* wakeLatch.await;
          yield* wakeLatch.close;
          yield* pollUntilIdle;
        }),
      ),
    );

    return {
      noteTask: (page) => Effect.sync(() => ingest.noteTask(page)),
      wake: Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        wakeState.graceUntil = now + AETHER_POLL_WAKE_GRACE_MS;
        wakeState.sawActive = false;
        yield* wakeLatch.open;
      }),
    };
  });

// FILE: aetherConversationIngest.ts
// Purpose: Turns Aether's durable conversation (live snapshots, turn pages, turn activity) into
// Synara runtime events, plus the signals the adapter's turn lifecycle needs: turn origins,
// pending questions and plans, and one settlement per turn run.
// Layer: Server provider boundary (Aether). No I/O, no clock, no randomness: the caller passes
// `observedAt`, and the same inputs in the same order give the same outputs, so replaying a
// recording twice gives identical events.
//
// Event ids are built from (taskId, turnId, row id, status), so re-reading rows after a restart
// yields ids the durable journal already holds instead of new rows. Text is the exception: it
// streams as deltas of the full accumulated content the live endpoint returns, and a delta's id
// names the offset it ends at. A message first seen closed streams nothing; its completion
// carries the whole text, which Synara uses only when it streamed none. After a restart a message
// continues from the text Synara already holds (`AetherIngestResume.emittedText`).
//
// Settlement is not emitted as `turn.completed` here. The adapter runs the mirror sync first,
// so this module only signals `TurnSettled`, derived from durable turn state.

import { createHash } from "node:crypto";

import {
  EventId,
  ProviderItemId,
  RuntimeItemId,
  RuntimeRequestId,
  type CanonicalItemType,
  type ProviderRuntimeEvent,
  type RuntimeItemStatus,
  type ThreadId,
  type TurnId,
  type UserInputQuestion,
} from "@synara/contracts";
import { Cause, Result, Schema } from "effect";

import { normalizeClaudeTodoTasks } from "../claudeTaskTracker.ts";
import type {
  AetherConversationTurnsPage,
  AetherLiveConversationSnapshot,
  AetherMessageId,
  AetherTask,
  AetherTimelineMessage,
  AetherTimelineTool,
  AetherToolId,
  AetherTaskId,
  AetherTurnActivityPage,
  AetherTurnId,
  AetherUnknownValue,
  AetherUserMessage,
} from "./aetherApiSchemas.ts";

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

export interface AetherIngestContext {
  readonly taskId: AetherTaskId;
  readonly threadId: ThreadId;
  /** The Synara turn an Aether turn's events belong to. The adapter owns this mapping. */
  readonly turnIdFor: (turnId: AetherTurnId) => TurnId;
}

/** A turn run: its turn, and the user message that started it (the root, or an answer). */
export interface AetherRunKey {
  readonly turnId: AetherTurnId;
  readonly runMessageId: string;
}

/**
 * Where a new ingest picks up, from what Synara already persisted; empty for a new task.
 *
 * `after` is the last run the adapter signalled. That run, and every turn listed before its turn
 * on the first turns page, count as signalled, so history does not settle again. If its turn is
 * not on that page (the conversation was truncated past it), every turn there counts. Verified
 * on real 8- and 32-turn tasks: the turns page lists turns oldest first (root messages in
 * ascending time), and `revision_sequence` is not monotonic across turns, so turns are only ever
 * ordered by their place on the page, never by revision.
 *
 * `emittedText` is how much of each message's text Synara already holds (UTF-16 length). The
 * message continues from there: nothing repeats, and a message that closed in the meantime emits
 * only its missing suffix. A length the server text cannot hold fails loudly.
 */
export interface AetherIngestResume {
  readonly after: AetherRunKey | null;
  readonly emittedText: ReadonlyMap<string, number>;
}

export type AetherPendingInputKind = "questions" | "plan";

/** How a turn run ended, read from durable turn state. */
export type AetherTurnSettlement =
  | { readonly state: "settled" }
  | { readonly state: "errored"; readonly errorMessage?: string }
  /** The run stopped at a proposed plan; approving it resumes the same Aether turn. */
  | { readonly state: "awaiting_input"; readonly kind: "plan" };

export type AetherIngestOutput =
  | { readonly _tag: "Event"; readonly event: ProviderRuntimeEvent }
  /** Once per turn: its root message, and the client message id Synara sent it with, if any. */
  | {
      readonly _tag: "TurnOrigin";
      readonly turnId: AetherTurnId;
      readonly rootMessageId: AetherMessageId;
      readonly rootMessageText: string;
      readonly clientMessageId?: string;
    }
  /**
   * The task waits on an answer. `toolId` is the respond fence (`expected_tool_id`); `input` is
   * the whole input the input digest is computed from. The recordings carry no digest.
   */
  | {
      readonly _tag: "PendingInput";
      readonly turnId: AetherTurnId;
      readonly toolId: AetherToolId;
      readonly kind: AetherPendingInputKind;
      readonly input: Readonly<Record<string, unknown>>;
    }
  | {
      readonly _tag: "PendingInputResolved";
      readonly turnId: AetherTurnId;
      readonly toolId: AetherToolId;
      readonly kind: AetherPendingInputKind;
    }
  /**
   * Once per turn run, after the run's last event. A run is keyed by the turn's last user message
   * (`runMessageId`): the root message, or the answer that resumed the turn. A proposed plan
   * therefore settles its Aether turn twice: `awaiting_input(plan)` when the plan is proposed,
   * then `settled` (or `errored`) when the approved run ends. That is intended: approving a plan
   * starts a new Synara turn, so the adapter maps each settle to its own Synara turn.
   */
  | ({ readonly _tag: "TurnSettled" } & AetherRunKey & AetherTurnSettlement)
  /** `resetSequence` moved: the conversation was truncated and is being re-read. */
  | { readonly _tag: "ConversationReset"; readonly resetSequence: number };

/**
 * Accumulated text stopped being a prefix extension of what was already streamed, or the text
 * Synara holds from before a restart (`source: "resume"`) does not fit the server's text.
 */
export class AetherTranscriptDivergedError extends Schema.TaggedErrorClass<AetherTranscriptDivergedError>()(
  "AetherTranscriptDivergedError",
  {
    taskId: Schema.String,
    turnId: Schema.String,
    messageId: Schema.String,
    source: Schema.Literals(["live", "activity", "resume"]),
    streamedLength: Schema.Number,
    receivedLength: Schema.Number,
  },
) {
  override get message(): string {
    return this.source === "resume"
      ? `Synara holds ${this.streamedLength} characters of Aether message ${this.messageId} (turn ${this.turnId}), which does not fit its ${this.receivedLength} characters on the server.`
      : `Aether message ${this.messageId} (turn ${this.turnId}) no longer extends the ${this.streamedLength} characters already streamed (${this.source} sent ${this.receivedLength}).`;
  }
}

/** A pending question or plan that Synara cannot follow: no turn owns it, or its answer is unreadable. */
export class AetherPendingInputError extends Schema.TaggedErrorClass<AetherPendingInputError>()(
  "AetherPendingInputError",
  {
    taskId: Schema.String,
    toolId: Schema.String,
    detail: Schema.String,
  },
) {
  override get message(): string {
    return `Synara cannot follow Aether's pending input ${this.toolId}: ${this.detail}`;
  }
}

export interface AetherLiveStep {
  readonly outputs: ReadonlyArray<AetherIngestOutput>;
  /** `headRevision` moved (or a previous refresh did not finish): re-read the turns page. */
  readonly refreshTurns: boolean;
}

// ---------------------------------------------------------------------------
// Vocabularies
// ---------------------------------------------------------------------------

/** Tools surfaced through their own channel (questions, plans), never as a tool row. */
const CLIENT_SURFACED_TOOL_NAMES: ReadonlySet<string> = new Set(["ask_user", "propose_plan"]);

type AetherItemType = NonNullable<AetherTimelineTool["itemType"]>;
type AetherToolStatus = AetherTimelineTool["status"];

type ToolKind =
  | { readonly kind: "item"; readonly itemType: CanonicalItemType; readonly unknownRaw?: string }
  | { readonly kind: "tasks" };

function toolKind(itemType: AetherItemType | undefined): ToolKind {
  if (itemType === undefined) return { kind: "item", itemType: "dynamic_tool_call" };
  if (typeof itemType !== "string") {
    return { kind: "item", itemType: "unknown", unknownRaw: itemType.raw };
  }
  switch (itemType) {
    case "task_tracking":
      return { kind: "tasks" };
    // Synara has no dedicated read or fetch row; both render as a generic tool call.
    case "file_read":
    case "web_fetch":
      return { kind: "item", itemType: "dynamic_tool_call" };
    case "subagent_invocation":
      return { kind: "item", itemType: "collab_agent_tool_call" };
    default:
      return { kind: "item", itemType };
  }
}

interface ToolStatus {
  readonly status: RuntimeItemStatus;
  readonly terminal: boolean;
  readonly raw: string;
  readonly unknown: boolean;
}

function toolStatus(status: AetherToolStatus): ToolStatus {
  if (typeof status !== "string") {
    return { status: "inProgress", terminal: false, raw: status.raw, unknown: true };
  }
  switch (status) {
    case "output-available":
      return { status: "completed", terminal: true, raw: status, unknown: false };
    case "output-error":
      return { status: "failed", terminal: true, raw: status, unknown: false };
    case "output-denied":
      return { status: "declined", terminal: true, raw: status, unknown: false };
    default:
      return { status: "inProgress", terminal: false, raw: status, unknown: false };
  }
}

// ---------------------------------------------------------------------------
// Input decoding
// ---------------------------------------------------------------------------

/**
 * An `ask_user` call's input, in Aether's canonical shape (every agent's questions arrive in it):
 * each question needs its text and its options, each option its label; a question with no
 * options takes free text. Fields Synara does not read are ignored, as elsewhere in Aether's
 * responses. The adapter maps answers back onto its option indexes.
 */
export const AskUserInput = Schema.Struct({
  questions: Schema.Array(
    Schema.Struct({
      question: Schema.String,
      header: Schema.optionalKey(Schema.String),
      options: Schema.Array(
        Schema.Struct({
          label: Schema.String,
          description: Schema.optionalKey(Schema.String),
        }),
      ),
      multiSelect: Schema.optionalKey(Schema.Boolean),
    }),
  ),
});

const AskUserAnswer = Schema.Struct({
  answers: Schema.Record(Schema.String, Schema.Array(Schema.Int)),
  customAnswers: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
});

const ProposePlanInput = Schema.Struct({ plan: Schema.String });

type Answers = Record<string, string | ReadonlyArray<string> | null>;
type ToolResponse = NonNullable<AetherUserMessage["toolResponse"]>;

/**
 * Synara's question cards need the question and every option label non-blank. A blank header or
 * option description is shown as absent.
 */
function questionsFromInput(
  input: unknown,
): Result.Result<ReadonlyArray<UserInputQuestion>, string> {
  const decoded = Schema.decodeUnknownExit(AskUserInput)(input);
  if (decoded._tag === "Failure") return Result.fail(Cause.pretty(decoded.cause));
  if (decoded.value.questions.length === 0) return Result.fail("it asks no questions");
  const questions: UserInputQuestion[] = [];
  for (const [index, question] of decoded.value.questions.entries()) {
    const text = question.question.trim();
    const header = nonEmpty(question.header);
    const options = question.options.map((option) => ({
      label: option.label.trim(),
      description: nonEmpty(option.description),
    }));
    if (text === "" || options.some((option) => option.label === "")) {
      return Result.fail(`question ${index} has a blank question or option label`);
    }
    // Aether keys answers by question index, so the index is the question id.
    questions.push({
      id: String(index),
      ...(header === undefined ? {} : { header }),
      question: text,
      options: options.map(({ label, description }) =>
        description === undefined ? { label } : { label, description },
      ),
      multiSelect: question.multiSelect ?? false,
    });
  }
  return Result.succeed(questions);
}

/**
 * Answer labels by question id, from the `ask_user` response that answered `questions`. Option
 * index `-1` selects the question's custom ("Other") answer from `customAnswers`.
 */
function answersFromResponse(
  questions: ReadonlyArray<UserInputQuestion>,
  response: ToolResponse,
): Result.Result<Answers, string> {
  if (response.tool_name !== "ask_user") {
    return Result.fail(`it was answered with a ${response.tool_name} response`);
  }
  const decoded = Schema.decodeUnknownExit(AskUserAnswer)(response.data);
  if (decoded._tag === "Failure") return Result.fail(Cause.pretty(decoded.cause));
  const { answers: choices, customAnswers = {} } = decoded.value;
  const asked = new Set(questions.map((question) => question.id));
  const unasked = [...Object.keys(choices), ...Object.keys(customAnswers)].find(
    (id) => !asked.has(id),
  );
  if (unasked !== undefined) return Result.fail(`the answer names question ${unasked}`);
  const answers: Answers = {};
  for (const question of questions) {
    const labels: string[] = [];
    for (const choice of choices[question.id] ?? []) {
      const label =
        choice === -1 ? nonEmpty(customAnswers[question.id]) : question.options[choice]?.label;
      if (label === undefined) {
        return Result.fail(
          choice === -1
            ? `question ${question.id} selects a custom answer but has none`
            : `question ${question.id} has no option ${choice}`,
        );
      }
      labels.push(label);
    }
    if (!question.multiSelect && labels.length > 1) {
      return Result.fail(`question ${question.id} takes one answer and got ${labels.length}`);
    }
    answers[question.id] = question.multiSelect ? labels : (labels[0] ?? null);
  }
  return Result.succeed(answers);
}

function fingerprint(value: unknown): string {
  return createHash("sha256").update(JSON.stringify(value)).digest("hex").slice(0, 16);
}

function nonEmpty(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed === undefined || trimmed === "" ? undefined : trimmed;
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

type TurnState = AetherConversationTurnsPage["turns"][number]["state"];

type RowRecord =
  | { readonly kind: "tool"; readonly key: string; readonly terminal: boolean }
  | { readonly kind: "tasks"; readonly key: string | undefined; readonly terminal: boolean }
  | { readonly kind: "done" };

/**
 * How much of a turn `resume.after` covers, row by row: Synara already emitted those rows, and
 * its journal may have pruned them, so they are read for their state but emit nothing. A turn
 * listed before the mark's is covered whole; the mark's turn up to its run's message and the
 * rows of that run (`to-mark`, then `at-mark`), and no longer from the next new user message. A
 * turn's activity never holds its root message, so a mark on the root starts at `at-mark`.
 */
type Coverage = "whole" | "to-mark" | "at-mark" | "none";

interface TurnRecord {
  readonly id: AetherTurnId;
  state: TurnState;
  coverage: Coverage;
  /** `revision_sequence` from the latest turns page. */
  revision: number;
  /** The activity revision last applied; the activity is re-read while it trails `revision`. */
  revisionRead: number | undefined;
  /** The last user message in the turn: the root, or an answer that resumed it. */
  lastUserMessageId: string;
  /** Question and plan calls in the turn, each with the response of the answer row after it. */
  readonly inputs: Map<AetherToolId, ToolResponse | undefined>;
  /** The latest of `inputs`: the call the next answer row answers. */
  lastInputId: AetherToolId | undefined;
  readonly rows: Map<string, RowRecord>;
}

interface MessageRecord {
  /** Text already streamed as deltas. */
  streamed: string;
  /** The closed message's final text. */
  final: string | undefined;
}

interface PendingInput {
  readonly turnId: AetherTurnId;
  readonly toolId: AetherToolId;
  readonly kind: AetherPendingInputKind;
  /** The questions as shown, to name the chosen labels; absent when they could not be shown. */
  readonly questions: ReadonlyArray<UserInputQuestion> | undefined;
}

type AwaitingAnswer = Extract<
  Extract<AetherTask, { readonly status: "awaiting_input" }>["awaiting_input"],
  { readonly tool_id: AetherToolId }
>;

interface IngestState {
  lastLiveHead: number | undefined;
  turnsRefreshPending: boolean;
  resetSequence: number | undefined;
  executing: boolean;
  task: AetherTask | undefined;
  open: AetherLiveConversationSnapshot["open"];
  /** Turns in page order, oldest first (see `AetherIngestResume`); never sorted by revision. */
  turnOrder: ReadonlyArray<AetherTurnId>;
  /** Turns `resume.after` covers, fixed at the first turns page. */
  resumed: ReadonlySet<AetherTurnId> | undefined;
  readonly turns: Map<AetherTurnId, TurnRecord>;
  readonly messages: Map<string, MessageRecord>;
  pending: PendingInput | undefined;
  readonly surfacedToolIds: Set<string>;
  readonly settledRuns: Set<string>;
  readonly warnedOnce: Set<string>;
}

const runKey = (turn: TurnRecord) => `${turn.id}:${turn.lastUserMessageId}`;

// ---------------------------------------------------------------------------
// Ingest
// ---------------------------------------------------------------------------

/**
 * One task's conversation ingestion. Feed it every live snapshot, the turns page whenever
 * `refreshTurns` asks for it, each turn from `turnsNeedingActivity()`, then `finishRefresh`.
 * Methods change the state in place; the poller applies each tick to a `fork()` and keeps the
 * fork only when the whole tick succeeds.
 */
export class AetherConversationIngest {
  private readonly context: AetherIngestContext;
  private readonly resume: AetherIngestResume;
  private state: IngestState = {
    lastLiveHead: undefined,
    turnsRefreshPending: true,
    resetSequence: undefined,
    executing: false,
    task: undefined,
    open: [],
    turnOrder: [],
    resumed: undefined,
    turns: new Map(),
    messages: new Map(),
    pending: undefined,
    surfacedToolIds: new Set(),
    settledRuns: new Set(),
    warnedOnce: new Set(),
  };

  constructor(context: AetherIngestContext, resume: AetherIngestResume) {
    this.context = context;
    this.resume = resume;
  }

  /** An independent copy to apply a tick to. */
  fork(): AetherConversationIngest {
    const fork = new AetherConversationIngest(this.context, this.resume);
    const state = this.state;
    fork.state = {
      ...state,
      turns: new Map(
        [...state.turns].map(([id, turn]) => [
          id,
          { ...turn, inputs: new Map(turn.inputs), rows: new Map(turn.rows) },
        ]),
      ),
      messages: new Map([...state.messages].map(([id, message]) => [id, { ...message }])),
      surfacedToolIds: new Set(state.surfacedToolIds),
      settledRuns: new Set(state.settledRuns),
      warnedOnce: new Set(state.warnedOnce),
    };
    return fork;
  }

  /** The open message ids to pass to the next live read. */
  openMessageIds(): ReadonlyArray<AetherMessageId> {
    return this.state.open.map((message) => message.id);
  }

  /**
   * Whether the task still has work to watch: an executing run, a busy task, a turn still
   * running, a run not signalled yet, or data not read yet. A question or plan already surfaced
   * is not: nothing changes on the server until someone answers (`waitsOnUser`).
   */
  shouldKeepPolling(): boolean {
    const { executing, open, turnsRefreshPending, task, turnOrder, turns } = this.state;
    if (executing || open.length > 0 || turnsRefreshPending) return true;
    if (this.turnsNeedingActivity().length > 0) return true;
    const awaiting = this.awaitingAnswer();
    if (awaiting !== undefined && !this.state.surfacedToolIds.has(awaiting.tool_id)) return true;
    if (task === undefined || task.status === "queued" || task.status === "processing") return true;
    return turnOrder.some((id) => {
      const turn = turns.get(id)!;
      if (turn.state === "awaiting_input" && awaiting !== undefined) {
        return awaiting.kind === "plan" && !this.isSignalled(turn);
      }
      return (turn.state !== "settled" && turn.state !== "errored") || !this.isSignalled(turn);
    });
  }

  /** The task as the last turns page applied showed it. */
  task(): AetherTask | undefined {
    return this.state.task;
  }

  /** Whether the task's open pull request has CI or an Aether Review still running. */
  watchesPullRequest(): boolean {
    const task = this.state.task;
    const pr = task?.primary_pull_request;
    if (task === undefined || pr === undefined || pr === null || pr.state !== "open") return false;
    const review = task.pr_review_summary;
    return (
      pr.ci_status === "pending" ||
      (review !== null &&
        review.pull_request_id === pr.pull_request_id &&
        (review.run_status === "reviewing" || review.run_status === "awaiting_input"))
    );
  }

  /** Reads the turns page, and with it the task, on the next tick. */
  refreshTurns(): void {
    this.state.turnsRefreshPending = true;
  }

  /** Whether a surfaced question or plan waits on the user. */
  waitsOnUser(): boolean {
    const awaiting = this.awaitingAnswer();
    return awaiting !== undefined && this.state.surfacedToolIds.has(awaiting.tool_id);
  }

  applyLive(
    snapshot: AetherLiveConversationSnapshot,
    observedAt: string,
  ): Result.Result<AetherLiveStep, AetherTranscriptDivergedError> {
    const state = this.state;
    const outputs: AetherIngestOutput[] = [];
    if (state.resetSequence !== undefined && snapshot.resetSequence !== state.resetSequence) {
      // Rows past the reset are gone. Re-read every turn from the newest page; rows that
      // survived keep their ids, so re-reading them emits nothing new.
      for (const turn of state.turns.values()) turn.revisionRead = undefined;
      state.turnsRefreshPending = true;
      outputs.push({ _tag: "ConversationReset", resetSequence: snapshot.resetSequence });
    }
    state.resetSequence = snapshot.resetSequence;
    state.executing = snapshot.activeExecution !== undefined;

    // Closed messages first: they were opened before anything still open.
    for (const [messages, closed] of [
      [snapshot.replacements, true],
      [snapshot.open, false],
    ] as const) {
      for (const message of messages) {
        const text = this.advanceText(
          message,
          message.conversationTurnId,
          closed,
          "live",
          observedAt,
        );
        if (Result.isFailure(text)) return Result.fail(text.failure);
        outputs.push(...text.success);
      }
    }
    state.open = snapshot.open;

    const refreshTurns = state.turnsRefreshPending || snapshot.headRevision !== state.lastLiveHead;
    state.lastLiveHead = snapshot.headRevision;
    state.turnsRefreshPending = refreshTurns;
    return Result.succeed({ outputs, refreshTurns });
  }

  /**
   * Whether the next tick reads the turns page in place of `/live`: while the task is queued
   * (its workspace boots, and nothing runs or is open), the head does not move but the task's
   * status line does, and only the turns page carries it.
   */
  readsTurnsInPlaceOfLive(): boolean {
    return this.state.task?.status === "queued" && this.state.open.length === 0;
  }

  /**
   * Applies the task from a one-turn page read in place of `/live`: enough for the task and
   * the head. True when the head moved (or a refresh is pending): then read the whole turns page
   * and apply it with `applyTurnsInPlaceOfLive`.
   */
  applyTaskInPlaceOfLive(page: AetherConversationTurnsPage): boolean {
    const state = this.state;
    // A page older than the head already applied (a lagging replica) is dropped.
    if (state.lastLiveHead !== undefined && page.head_revision < state.lastLiveHead) return false;
    state.executing = false;
    if (state.turnsRefreshPending || page.head_revision !== state.lastLiveHead) return true;
    state.task = page.task;
    return false;
  }

  /** Applies a turns page read in place of `/live` (see `readsTurnsInPlaceOfLive`). */
  applyTurnsInPlaceOfLive(page: AetherConversationTurnsPage): ReadonlyArray<AetherIngestOutput> {
    const state = this.state;
    if (state.lastLiveHead !== undefined && page.head_revision < state.lastLiveHead) return [];
    state.lastLiveHead = page.head_revision;
    state.executing = false;
    return this.applyTurns(page);
  }

  /** Applies a turns page; a page older than the live head already applied is ignored. */
  applyTurns(page: AetherConversationTurnsPage): ReadonlyArray<AetherIngestOutput> {
    const state = this.state;
    if (state.lastLiveHead !== undefined && page.head_revision < state.lastLiveHead) return [];
    const outputs: AetherIngestOutput[] = [];
    state.task = page.task;
    state.turnsRefreshPending = false;
    if (state.resumed === undefined) {
      const mark = this.resume.after;
      const markAt = mark === null ? -1 : page.turns.findIndex((t) => t.id === mark.turnId);
      const covered = mark === null ? 0 : markAt === -1 ? page.turns.length : markAt + 1;
      state.resumed = new Set(page.turns.slice(0, covered).map((turn) => turn.id));
    }
    const resumed = state.resumed;
    for (const turn of page.turns) {
      const known = state.turns.get(turn.id);
      if (known !== undefined) {
        known.state = turn.state;
        known.revision = turn.revision_sequence;
        continue;
      }
      const mark = this.resume.after;
      const coverage: Coverage = !resumed.has(turn.id)
        ? "none"
        : turn.id !== mark?.turnId
          ? "whole"
          : mark.runMessageId === turn.root_message.id
            ? "at-mark"
            : "to-mark";
      state.turns.set(turn.id, {
        id: turn.id,
        state: turn.state,
        coverage,
        revision: turn.revision_sequence,
        revisionRead: undefined,
        lastUserMessageId: turn.root_message.id,
        inputs: new Map(),
        lastInputId: undefined,
        rows: new Map(),
      });
      // A covered turn was bound, or found to be from outside Synara, before.
      if (coverage !== "none") continue;
      outputs.push({
        _tag: "TurnOrigin",
        turnId: turn.id,
        rootMessageId: turn.root_message.id,
        rootMessageText: turn.root_message.content,
        ...(turn.root_message.clientMessageId === undefined
          ? {}
          : { clientMessageId: turn.root_message.clientMessageId }),
      });
    }
    state.turnOrder = page.turns.map((turn) => turn.id);
    return outputs;
  }

  /** Turns whose activity changed since it was last read. */
  turnsNeedingActivity(): ReadonlyArray<AetherTurnId> {
    return this.state.turnOrder.filter((turnId) => !this.isCurrent(this.state.turns.get(turnId)!));
  }

  /**
   * Whether to read the page older than `page`. A turn never read is read whole; otherwise
   * paging stops at the first page that starts with a known row, once every row still open
   * (a running tool, a streaming message) has been read again.
   */
  needsOlderActivity(
    turnId: AetherTurnId,
    page: AetherTurnActivityPage,
    readSoFar: ReadonlySet<string>,
  ): boolean {
    if (!page.has_more_older || page.oldest_sequence_loaded === null) return false;
    const turn = this.state.turns.get(turnId);
    if (turn === undefined || turn.revisionRead === undefined) return true;
    const oldest = page.items[0];
    if (oldest === undefined || !turn.rows.has(oldest.id)) return true;
    for (const [rowId, row] of turn.rows) {
      const message = this.state.messages.get(rowId);
      const open =
        row.kind === "done" ? message !== undefined && message.final === undefined : !row.terminal;
      if (open && !readSoFar.has(rowId)) return true;
    }
    return false;
  }

  /** Applies a turn's activity rows, oldest first, read at `revision`. */
  applyActivity(
    turnId: AetherTurnId,
    items: ReadonlyArray<AetherTimelineMessage>,
    revision: number,
    observedAt: string,
  ): Result.Result<ReadonlyArray<AetherIngestOutput>, AetherTranscriptDivergedError> {
    const turn = this.state.turns.get(turnId);
    // A read older than one already applied (a lagging replica) is dropped and read again.
    if (turn === undefined || revision < (turn.revisionRead ?? 0)) return Result.succeed([]);
    const outputs: AetherIngestOutput[] = [];
    for (const item of items) {
      if (item.role === "user") {
        if (turn.coverage === "to-mark" && item.id === this.resume.after?.runMessageId) {
          turn.coverage = "at-mark";
        } else if (turn.coverage === "at-mark" && !turn.rows.has(item.id)) {
          turn.coverage = "none";
        }
        turn.lastUserMessageId = item.id;
        const answered = turn.lastInputId;
        if (
          item.toolResponse !== undefined &&
          !turn.rows.has(item.id) &&
          answered !== undefined &&
          turn.inputs.get(answered) === undefined
        ) {
          turn.inputs.set(answered, item.toolResponse);
        }
        turn.rows.set(item.id, { kind: "done" });
        continue;
      }
      const rowOutputs: AetherIngestOutput[] = [];
      switch (item.variant) {
        case "text":
        case "thinking": {
          turn.rows.set(item.id, { kind: "done" });
          // While a message is open the live endpoint owns it.
          if (this.state.open.some((message) => message.id === item.id)) break;
          const text = this.advanceText(item, turnId, true, "activity", observedAt);
          if (Result.isFailure(text)) return text;
          rowOutputs.push(...text.success);
          break;
        }
        case "tool":
          rowOutputs.push(...this.applyTool(turn, item.id, item.tool, observedAt));
          break;
        case "seam":
          rowOutputs.push(...this.applySeam(turn, item.id, item.seam.reason, observedAt));
          break;
      }
      if (turn.coverage === "none") outputs.push(...rowOutputs);
    }
    // A read that trails the turns page is applied, and the turn is read again next tick.
    turn.revisionRead = revision;
    return Result.succeed(outputs);
  }

  /**
   * After a refresh's reads: resolves an answered question or plan, surfaces a new one, and
   * signals every turn run that settled.
   */
  finishRefresh(
    observedAt: string,
  ): Result.Result<ReadonlyArray<AetherIngestOutput>, AetherPendingInputError> {
    const state = this.state;
    const outputs: AetherIngestOutput[] = [];
    const awaiting = this.awaitingAnswer();

    // An answer is read from the pending turn's activity, so wait until that is current.
    const pending = state.pending;
    if (
      pending !== undefined &&
      awaiting?.tool_id !== pending.toolId &&
      this.isCurrent(state.turns.get(pending.turnId)!)
    ) {
      const resolved = this.resolveInput(pending, observedAt);
      if (Result.isFailure(resolved)) return resolved;
      outputs.push(...resolved.success);
      state.pending = undefined;
    }
    if (awaiting !== undefined && !state.surfacedToolIds.has(awaiting.tool_id)) {
      const surfaced = this.surfaceInput(awaiting, observedAt);
      if (Result.isFailure(surfaced)) return surfaced;
      outputs.push(...surfaced.success);
    }

    for (const turnId of state.turnOrder) {
      const turn = state.turns.get(turnId)!;
      // Never before the turn's last rows: its activity must be current and its text closed.
      if (!this.isCurrent(turn) || state.open.some((m) => m.conversationTurnId === turn.id)) {
        continue;
      }
      if (this.isSignalled(turn)) continue;
      const settlement = this.settlementOf(turn);
      if (settlement === undefined) continue;
      state.settledRuns.add(runKey(turn));
      outputs.push({
        _tag: "TurnSettled",
        turnId: turn.id,
        runMessageId: turn.lastUserMessageId,
        ...settlement,
      });
    }
    return Result.succeed(outputs);
  }

  // -------------------------------------------------------------------------
  // Settlement and pending input
  // -------------------------------------------------------------------------

  private isCurrent(turn: TurnRecord): boolean {
    return turn.revisionRead !== undefined && turn.revisionRead >= turn.revision;
  }

  private isSignalled(turn: TurnRecord): boolean {
    if (this.state.settledRuns.has(runKey(turn))) return true;
    const mark = this.resume.after;
    if (mark === null || this.state.resumed?.has(turn.id) !== true) return false;
    return turn.id !== mark.turnId || turn.lastUserMessageId === mark.runMessageId;
  }

  private awaitingAnswer(): AwaitingAnswer | undefined {
    const task = this.state.task;
    if (task?.status !== "awaiting_input" || task.awaiting_input.kind === "message") {
      return undefined;
    }
    return task.awaiting_input;
  }

  private settlementOf(turn: TurnRecord): AetherTurnSettlement | undefined {
    const task = this.state.task;
    if (task === undefined) return undefined;
    if (turn.state === "awaiting_input") {
      const awaiting = this.awaitingAnswer();
      return awaiting?.kind === "plan" && this.state.pending?.turnId === turn.id
        ? { state: "awaiting_input", kind: "plan" }
        : undefined;
    }
    if (turn.state !== "settled" && turn.state !== "errored") return undefined;
    // A turn briefly reads `settled` between an answer being queued and its run starting
    // (the task is `queued` again). It is final only once the task is idle, or the task's
    // queued work belongs to another turn.
    const taskBusy = task.status === "queued" || task.status === "processing";
    if (taskBusy && !this.anotherTurnActive(turn.id)) return undefined;
    if (turn.state === "settled") return { state: "settled" };
    return task.status === "errored"
      ? { state: "errored", errorMessage: task.error }
      : { state: "errored" };
  }

  private anotherTurnActive(turnId: AetherTurnId): boolean {
    return this.state.turnOrder.some((id) => {
      if (id === turnId) return false;
      const state = this.state.turns.get(id)?.state;
      return state === "queued" || state === "running" || state === "awaiting_input";
    });
  }

  /** Surfaces the input once the turn that asked it has been read. */
  private surfaceInput(
    awaiting: AwaitingAnswer,
    observedAt: string,
  ): Result.Result<AetherIngestOutput[], AetherPendingInputError> {
    const state = this.state;
    const toolId = awaiting.tool_id;
    const turnId = state.turnOrder.find((id) => state.turns.get(id)!.inputs.has(toolId));
    if (turnId === undefined) {
      if (this.turnsNeedingActivity().length > 0) return Result.succeed([]);
      return Result.fail(
        new AetherPendingInputError({
          taskId: this.context.taskId,
          toolId,
          detail: `no turn on the first page made the ${awaiting.kind} call`,
        }),
      );
    }
    state.surfacedToolIds.add(toolId);
    const outputs: AetherIngestOutput[] = [
      { _tag: "PendingInput", turnId, toolId, kind: awaiting.kind, input: awaiting.input },
    ];
    const refs = { requestId: toolId } as const;
    if (awaiting.kind === "questions") {
      const questions = questionsFromInput(awaiting.input);
      state.pending = {
        turnId,
        toolId,
        kind: "questions",
        questions: Result.isSuccess(questions) ? questions.success : undefined,
      };
      outputs.push(
        Result.isFailure(questions)
          ? this.warning(turnId, [toolId, "questions-unreadable"], observedAt, {
              message: "Aether is waiting on a question that Synara cannot display.",
              detail: { toolId, input: awaiting.input, error: questions.failure },
            })
          : this.event(turnId, [toolId, "user-input.requested"], observedAt, refs, {
              type: "user-input.requested",
              payload: { questions: questions.success },
            }),
      );
      return Result.succeed(outputs);
    }
    state.pending = { turnId, toolId, kind: "plan", questions: undefined };
    const decoded = Schema.decodeUnknownExit(ProposePlanInput)(awaiting.input);
    const plan = decoded._tag === "Success" ? nonEmpty(decoded.value.plan) : undefined;
    outputs.push(
      plan === undefined
        ? this.warning(turnId, [toolId, "plan-unreadable"], observedAt, {
            message: "Aether is waiting on a plan approval that Synara cannot display.",
            detail: {
              toolId,
              input: awaiting.input,
              error: decoded._tag === "Failure" ? Cause.pretty(decoded.cause) : "the plan is empty",
            },
          })
        : this.event(turnId, [toolId, "plan-proposed"], observedAt, refs, {
            type: "turn.proposed.completed",
            payload: { planMarkdown: plan },
          }),
    );
    return Result.succeed(outputs);
  }

  /**
   * The task stopped waiting on `pending`. A shown question resolves with the answer row that
   * followed its call; with no answer row (the task was stopped) nothing resolves it, and Synara
   * settles the request as stale when the turn ends.
   */
  private resolveInput(
    pending: PendingInput,
    observedAt: string,
  ): Result.Result<AetherIngestOutput[], AetherPendingInputError> {
    const { turnId, toolId, kind } = pending;
    const resolved: AetherIngestOutput = { _tag: "PendingInputResolved", turnId, toolId, kind };
    if (pending.questions === undefined) return Result.succeed([resolved]);
    const response = this.state.turns.get(turnId)!.inputs.get(toolId);
    if (response === undefined) {
      const warning = this.warning(turnId, [toolId, "unanswered"], observedAt, {
        message: "Aether stopped waiting on a question without an answer.",
        detail: { toolId },
      });
      return Result.succeed([warning, resolved]);
    }
    const answers = answersFromResponse(pending.questions, response);
    if (Result.isFailure(answers)) {
      return Result.fail(
        new AetherPendingInputError({
          taskId: this.context.taskId,
          toolId,
          detail: `the answer is unreadable: ${answers.failure}`,
        }),
      );
    }
    const event = this.event(
      turnId,
      [toolId, "user-input.resolved"],
      observedAt,
      {
        requestId: toolId,
      },
      { type: "user-input.resolved", payload: { answers: answers.success } },
    );
    return Result.succeed([event, resolved]);
  }

  // -------------------------------------------------------------------------
  // Rows
  // -------------------------------------------------------------------------

  private applyTool(
    turn: TurnRecord,
    rowId: string,
    tool: AetherTimelineTool,
    observedAt: string,
  ): AetherIngestOutput[] {
    if (CLIENT_SURFACED_TOOL_NAMES.has(tool.name)) {
      if (!turn.inputs.has(tool.id)) {
        turn.inputs.set(tool.id, undefined);
        turn.lastInputId = tool.id;
      }
      // Synara showed a covered call's question or plan before; it is not shown again.
      if (turn.coverage !== "none") this.state.surfacedToolIds.add(tool.id);
      turn.rows.set(rowId, { kind: "done" });
      return [];
    }
    const outputs: AetherIngestOutput[] = [];
    const status = toolStatus(tool.status);
    if (status.unknown) {
      outputs.push(
        ...this.warnOnce(turn.id, ["tool-status", status.raw], observedAt, {
          message: `Aether reported a tool status Synara does not recognize (${status.raw}); the row stays in progress.`,
          detail: { toolName: tool.name, status: status.raw },
        }),
      );
    }
    const kind = toolKind(tool.itemType);
    if (kind.kind === "tasks") {
      outputs.push(...this.applyTaskList(turn, rowId, tool, status, observedAt));
      return outputs;
    }
    if (kind.unknownRaw !== undefined) {
      outputs.push(
        ...this.warnOnce(turn.id, ["item-type", kind.unknownRaw], observedAt, {
          message: `Aether ran a tool type Synara does not support yet (${kind.unknownRaw}); it is shown as a generic row.`,
          detail: { toolName: tool.name, itemType: kind.unknownRaw },
        }),
      );
    }

    const detail =
      nonEmpty(tool.display.primary) ??
      (status.status === "failed" ? nonEmpty(tool.error) : undefined);
    const payload = {
      itemType: kind.itemType,
      status: status.status,
      title: tool.display.label.trim(),
      ...(detail === undefined ? {} : { detail }),
      data: {
        toolCallId: tool.id,
        toolName: tool.name,
        input: tool.input,
        aetherStatus: status.raw,
        ...(kind.unknownRaw === undefined ? {} : { aetherItemType: kind.unknownRaw }),
        ...(tool.result === undefined ? {} : { result: tool.result }),
        ...(tool.error === undefined ? {} : { error: tool.error }),
        ...(tool.exitCode === undefined ? {} : { exitCode: tool.exitCode }),
        ...(tool.durationMs === undefined ? {} : { durationMs: tool.durationMs }),
      },
    } as const;
    const key = fingerprint(payload);
    const previous = turn.rows.get(rowId);
    if (previous?.kind === "tool" && (previous.terminal || previous.key === key)) return outputs;

    const refs = { itemId: rowId, providerItemId: tool.id } as const;
    const type = status.terminal
      ? "item.completed"
      : previous === undefined
        ? "item.started"
        : "item.updated";
    // A lifecycle edge is one event per row and status; an in-progress change of the same
    // status is told apart by its content.
    const idParts = type === "item.updated" ? [rowId, status.raw, key] : [rowId, status.raw];
    turn.rows.set(rowId, { kind: "tool", key, terminal: status.terminal });
    outputs.push(this.event(turn.id, idParts, observedAt, refs, { type, payload }));
    return outputs;
  }

  /** Claude's `TodoWrite` (`todos`) becomes a task list update. */
  private applyTaskList(
    turn: TurnRecord,
    rowId: string,
    tool: AetherTimelineTool,
    status: ToolStatus,
    observedAt: string,
  ): AetherIngestOutput[] {
    const previous = turn.rows.get(rowId);
    const tasks = normalizeClaudeTodoTasks(tool.input);
    if (tasks === null) {
      turn.rows.set(rowId, { kind: "tasks", key: undefined, terminal: status.terminal });
      // The input fills in while the call streams; only a finished call is unreadable.
      if (!status.terminal || Object.keys(tool.input).length === 0) return [];
      return this.warnOnce(turn.id, [rowId, "tasks-unreadable"], observedAt, {
        message: "Aether updated a task list that Synara cannot read.",
        detail: { toolName: tool.name, input: tool.input },
      });
    }
    const key = fingerprint(tasks);
    turn.rows.set(rowId, { kind: "tasks", key, terminal: status.terminal });
    if (previous?.kind === "tasks" && previous.key === key) return [];
    return [
      this.event(
        turn.id,
        [rowId, "tasks", key],
        observedAt,
        { providerItemId: tool.id },
        { type: "turn.tasks.updated", payload: tasks },
      ),
    ];
  }

  private applySeam(
    turn: TurnRecord,
    rowId: string,
    reason: "teleport" | "compaction" | AetherUnknownValue,
    observedAt: string,
  ): AetherIngestOutput[] {
    if (turn.rows.has(rowId)) return [];
    turn.rows.set(rowId, { kind: "done" });
    if (reason === "compaction") {
      return [
        this.event(
          turn.id,
          [rowId, "seam"],
          observedAt,
          { itemId: rowId },
          {
            type: "item.completed",
            payload: {
              itemType: "context_compaction",
              status: "completed",
              title: "Context compacted",
            },
          },
        ),
      ];
    }
    return [
      this.warning(turn.id, [rowId, "seam"], observedAt, {
        message:
          typeof reason === "string"
            ? `Aether marked a conversation seam (${reason}).`
            : `Aether marked a conversation seam Synara does not recognize (${reason.raw}).`,
        detail: { reason: typeof reason === "string" ? reason : reason.raw },
      }),
    ];
  }

  /** Streams the new suffix of a message's accumulated text; completes it once closed. */
  private advanceText(
    message: {
      readonly id: AetherMessageId;
      readonly variant: "text" | "thinking";
      readonly content: string;
    },
    turnId: AetherTurnId,
    closed: boolean,
    source: "live" | "activity",
    observedAt: string,
  ): Result.Result<AetherIngestOutput[], AetherTranscriptDivergedError> {
    let record = this.state.messages.get(message.id);
    if (record === undefined) {
      // After a restart, continue from the text Synara already holds.
      const emitted = this.resume.emittedText.get(message.id) ?? 0;
      if (!Number.isSafeInteger(emitted) || emitted < 0 || emitted > message.content.length) {
        return Result.fail(
          new AetherTranscriptDivergedError({
            taskId: this.context.taskId,
            turnId,
            messageId: message.id,
            source: "resume",
            streamedLength: emitted,
            receivedLength: message.content.length,
          }),
        );
      }
      record = { streamed: message.content.slice(0, emitted), final: undefined };
      this.state.messages.set(message.id, record);
    }
    const kept = record.final ?? record.streamed;
    if (record.final !== undefined ? message.content !== kept : !message.content.startsWith(kept)) {
      return Result.fail(
        new AetherTranscriptDivergedError({
          taskId: this.context.taskId,
          turnId,
          messageId: message.id,
          source,
          streamedLength: kept.length,
          receivedLength: message.content.length,
        }),
      );
    }
    if (record.final !== undefined) return Result.succeed([]);

    const outputs: AetherIngestOutput[] = [];
    const refs = { itemId: message.id } as const;
    const suffix = message.content.slice(record.streamed.length);
    // A message first seen closed streams nothing (its completion carries the whole text), unless
    // Synara holds part of it from before a restart: then only the missing suffix streams.
    if (suffix !== "" && (!closed || record.streamed !== "")) {
      outputs.push(
        this.event(
          turnId,
          [message.id, "delta", String(message.content.length)],
          observedAt,
          refs,
          {
            type: "content.delta",
            payload: {
              streamKind: message.variant === "text" ? "assistant_text" : "reasoning_text",
              delta: suffix,
            },
          },
        ),
      );
      record.streamed = message.content;
    }
    if (closed) {
      record.final = message.content;
      // An empty message (a redacted thinking block) has nothing to show.
      if (message.content.trim() !== "") {
        outputs.push(
          this.event(turnId, [message.id, "completed"], observedAt, refs, {
            type: "item.completed",
            payload: {
              itemType: message.variant === "text" ? "assistant_message" : "reasoning",
              status: "completed",
              detail: message.content,
            },
          }),
        );
      }
    }
    return Result.succeed(outputs);
  }

  // -------------------------------------------------------------------------
  // Event construction
  // -------------------------------------------------------------------------

  private event(
    turnId: AetherTurnId | undefined,
    idParts: ReadonlyArray<string>,
    observedAt: string,
    refs: {
      readonly itemId?: string;
      readonly providerItemId?: string;
      readonly requestId?: string;
    },
    body: DistributiveOmit<ProviderRuntimeEvent, keyof ProviderRuntimeEventBaseFields>,
  ): AetherIngestOutput {
    const event = {
      ...makeAetherEventBase(this.context, turnId, idParts, observedAt, refs),
      ...body,
    } as ProviderRuntimeEvent;
    return { _tag: "Event", event };
  }

  private warning(
    turnId: AetherTurnId | undefined,
    idParts: ReadonlyArray<string>,
    observedAt: string,
    payload: { readonly message: string; readonly detail?: unknown },
  ): AetherIngestOutput {
    return this.event(turnId, idParts, observedAt, {}, { type: "runtime.warning", payload });
  }

  /** A vocabulary warning shown once per task and value, not once per row. */
  private warnOnce(
    turnId: AetherTurnId,
    idParts: ReadonlyArray<string>,
    observedAt: string,
    payload: { readonly message: string; readonly detail?: unknown },
  ): AetherIngestOutput[] {
    const key = idParts.join(":");
    if (this.state.warnedOnce.has(key)) return [];
    this.state.warnedOnce.add(key);
    return [this.warning(turnId, idParts, observedAt, payload)];
  }
}

type ProviderRuntimeEventBaseFields = Pick<
  ProviderRuntimeEvent,
  | "eventId"
  | "provider"
  | "threadId"
  | "createdAt"
  | "turnId"
  | "itemId"
  | "requestId"
  | "providerRefs"
>;

type DistributiveOmit<T, K extends PropertyKey> = T extends unknown ? Omit<T, K> : never;

/**
 * The fields every Aether event shares. The id is `aether:<task>:<turn or "task">:<parts>`:
 * the same durable row in the same state always gets the same id.
 */
export function makeAetherEventBase(
  context: AetherIngestContext,
  turnId: AetherTurnId | undefined,
  idParts: ReadonlyArray<string>,
  observedAt: string,
  refs: {
    readonly itemId?: string;
    readonly providerItemId?: string;
    readonly requestId?: string;
  } = {},
): ProviderRuntimeEventBaseFields {
  const providerItemId = refs.providerItemId ?? refs.itemId;
  return {
    eventId: EventId.makeUnsafe(["aether", context.taskId, turnId ?? "task", ...idParts].join(":")),
    provider: "aether",
    threadId: context.threadId,
    createdAt: observedAt,
    ...(turnId === undefined ? {} : { turnId: context.turnIdFor(turnId) }),
    ...(refs.itemId === undefined ? {} : { itemId: RuntimeItemId.makeUnsafe(refs.itemId) }),
    ...(refs.requestId === undefined
      ? {}
      : { requestId: RuntimeRequestId.makeUnsafe(refs.requestId) }),
    providerRefs: {
      ...(turnId === undefined ? {} : { providerTurnId: turnId }),
      ...(providerItemId === undefined
        ? {}
        : { providerItemId: ProviderItemId.makeUnsafe(providerItemId) }),
      ...(refs.requestId === undefined ? {} : { providerRequestId: refs.requestId }),
    },
  };
}

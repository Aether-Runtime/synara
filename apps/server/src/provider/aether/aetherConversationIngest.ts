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
// carries the whole text, which Synara uses only when it streamed none.
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
  type RuntimeTaskListItem,
  type ThreadId,
  type TurnId,
  type UserInputQuestion,
} from "@synara/contracts";
import { Option, Result, Schema } from "effect";

import { normalizeClaudeTodoTasks } from "../claudeTaskTracker.ts";
import { makeRuntimeTaskListItem, nonEmptyRuntimeTaskListPayload } from "../runtimeTaskList.ts";
import type {
  AetherConversationTurnsPage,
  AetherLiveConversationSnapshot,
  AetherMessageId,
  AetherTask,
  AetherTimelineMessage,
  AetherTimelineTool,
  AetherToolId,
  AetherTaskId,
  AetherTurnId,
  AetherUnknownValue,
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

export interface AetherIngestSeed {
  /**
   * How much of each message Synara already holds, for a restart in the middle of a stream:
   * streaming resumes after that many characters instead of repeating them.
   */
  readonly streamedLengthByMessageId?: ReadonlyMap<string, number>;
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
   * Exactly once per turn run. A run is keyed by the last user message in the turn
   * (`runMessageId`): the root message, or the answer that resumed the turn.
   */
  | ({
      readonly _tag: "TurnSettled";
      readonly turnId: AetherTurnId;
      readonly runMessageId: string;
    } & AetherTurnSettlement)
  /** `resetSequence` moved: the conversation was truncated and is being re-read. */
  | { readonly _tag: "ConversationReset"; readonly resetSequence: number };

/** Accumulated text stopped being a prefix extension of what was already streamed. */
export class AetherTranscriptDivergedError extends Schema.TaggedErrorClass<AetherTranscriptDivergedError>()(
  "AetherTranscriptDivergedError",
  {
    taskId: Schema.String,
    turnId: Schema.String,
    messageId: Schema.String,
    source: Schema.Literals(["live", "activity", "seed"]),
    streamedLength: Schema.Number,
    receivedLength: Schema.Number,
  },
) {
  override get message(): string {
    return `Aether message ${this.messageId} (turn ${this.turnId}) no longer extends the ${this.streamedLength} characters already streamed (${this.source} sent ${this.receivedLength}).`;
  }
}

export interface AetherLiveStep {
  readonly outputs: ReadonlyArray<AetherIngestOutput>;
  /** `headRevision` moved (or a previous refresh did not finish): re-read the turns page. */
  readonly refreshTurns: boolean;
}

/** One activity page, as far as paging back needs it. */
export interface AetherActivityPageView {
  readonly items: ReadonlyArray<AetherTimelineMessage>;
  readonly has_more_older: boolean;
  readonly oldest_sequence_loaded: number | null;
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

const isUnknown = (value: unknown): value is AetherUnknownValue =>
  typeof value === "object" && value !== null && (value as { _tag?: unknown })._tag === "Unknown";

// ---------------------------------------------------------------------------
// Input decoding
// ---------------------------------------------------------------------------

const AskUserInput = Schema.Struct({
  questions: Schema.Array(
    Schema.Struct({
      question: Schema.String,
      header: Schema.String,
      options: Schema.Array(Schema.Struct({ label: Schema.String, description: Schema.String })),
      multiSelect: Schema.optionalKey(Schema.Boolean),
    }),
  ),
});
type AskUserInput = typeof AskUserInput.Type;

const AskUserAnswer = Schema.Struct({
  answers: Schema.Record(Schema.String, Schema.Array(Schema.Int)),
  customAnswers: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
});

const ProposePlanInput = Schema.Struct({ plan: Schema.String });

/** Synara's question cards need every text field non-empty after trimming. */
function questionsFromInput(input: unknown): ReadonlyArray<UserInputQuestion> | undefined {
  const decoded = Schema.decodeUnknownOption(AskUserInput)(input);
  if (Option.isNone(decoded) || decoded.value.questions.length === 0) return undefined;
  const questions: UserInputQuestion[] = [];
  for (const [index, question] of decoded.value.questions.entries()) {
    const text = question.question.trim();
    const header = question.header.trim();
    const options = question.options.map((option) => ({
      label: option.label.trim(),
      description: option.description.trim(),
    }));
    if (
      text === "" ||
      header === "" ||
      options.some((option) => option.label === "" || option.description === "")
    ) {
      return undefined;
    }
    // Aether keys answers by question index, so the index is the question id.
    questions.push({
      id: String(index),
      header,
      question: text,
      options,
      multiSelect: question.multiSelect ?? false,
    });
  }
  return questions;
}

/** Answer labels by question id, from an `ask_user` tool response. */
function answersFromResponse(
  questions: AskUserInput["questions"],
  data: unknown,
): Record<string, string | ReadonlyArray<string> | null> | undefined {
  const decoded = Schema.decodeUnknownOption(AskUserAnswer)(data);
  if (Option.isNone(decoded)) return undefined;
  const answers: Record<string, string | ReadonlyArray<string> | null> = {};
  for (const [index, question] of questions.entries()) {
    const id = String(index);
    const labels = (decoded.value.answers[id] ?? []).flatMap((choice) => {
      const label = question.options[choice]?.label.trim();
      return label === undefined || label === "" ? [] : [label];
    });
    const custom = decoded.value.customAnswers?.[id]?.trim();
    const chosen = custom === undefined || custom === "" ? labels : [...labels, custom];
    answers[id] = question.multiSelect === true ? chosen : (chosen[0] ?? null);
  }
  return answers;
}

/** Claude's `TodoWrite` (`todos`) or Codex's `update_plan` (`plan`, `explanation`). */
function taskListFromInput(input: Readonly<Record<string, unknown>>):
  | {
      readonly explanation?: string;
      readonly tasks: ReadonlyArray<RuntimeTaskListItem>;
    }
  | undefined {
  if (Array.isArray(input.todos)) return normalizeClaudeTodoTasks(input) ?? undefined;
  if (!Array.isArray(input.plan)) return undefined;
  const tasks = input.plan.flatMap((entry: unknown) => {
    if (typeof entry !== "object" || entry === null) return [];
    const step = entry as Record<string, unknown>;
    const item = makeRuntimeTaskListItem(step.step, step.status);
    return item ? [item] : [];
  });
  const payload = nonEmptyRuntimeTaskListPayload(tasks);
  if (payload === null) return undefined;
  const explanation = typeof input.explanation === "string" ? input.explanation.trim() : "";
  return explanation === "" ? payload : { explanation, ...payload };
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
  | { readonly kind: "client" }
  | { readonly kind: "done" };

interface TurnRecord {
  readonly id: AetherTurnId;
  state: TurnState;
  /** `revision_sequence` from the latest turns page. */
  revision: number;
  /** The activity revision last applied; the activity is re-read while it trails `revision`. */
  revisionRead: number | undefined;
  /** The last user message in the turn: the root, or an answer that resumed it. */
  lastUserMessageId: string;
  /** `ask_user` answers carried by user rows, in order. */
  readonly answers: Array<unknown>;
  readonly rows: Map<string, RowRecord>;
}

interface MessageRecord {
  readonly turnId: AetherTurnId;
  readonly variant: "text" | "thinking";
  /** Text already streamed as deltas. */
  streamed: string;
  /** The closed message's final text. */
  final: string | undefined;
}

interface PendingInput {
  readonly turnId: AetherTurnId;
  readonly toolId: AetherToolId;
  readonly kind: AetherPendingInputKind;
  readonly questions: AskUserInput["questions"] | undefined;
}

type AwaitingAnswer = Extract<
  Extract<AetherTask, { readonly status: "awaiting_input" }>["awaiting_input"],
  { readonly tool_id: AetherToolId }
>;

// ---------------------------------------------------------------------------
// Ingest
// ---------------------------------------------------------------------------

/**
 * One task's conversation ingestion. Feed it every live snapshot, the turns page whenever
 * `refreshTurns` asks for it, each turn from `turnsNeedingActivity()`, then `finishRefresh`.
 */
export class AetherConversationIngest {
  private readonly context: AetherIngestContext;
  private readonly seedLengths: ReadonlyMap<string, number>;

  private lastLiveHead: number | undefined;
  private turnsRefreshPending = true;
  private resetSequence: number | undefined;
  private executing = false;
  private task: AetherTask | undefined;
  private openIds: ReadonlyArray<AetherMessageId> = [];
  private turnOrder: ReadonlyArray<AetherTurnId> = [];
  private readonly turns = new Map<AetherTurnId, TurnRecord>();
  private readonly messages = new Map<string, MessageRecord>();
  private pending: PendingInput | undefined;
  private readonly surfacedToolIds = new Set<string>();
  private readonly settledRuns = new Set<string>();
  private readonly warnedOnce = new Set<string>();

  constructor(context: AetherIngestContext, seed: AetherIngestSeed = {}) {
    this.context = context;
    this.seedLengths = seed.streamedLengthByMessageId ?? new Map();
  }

  /** The open message ids to pass to the next live read. */
  openMessageIds(): ReadonlyArray<AetherMessageId> {
    return this.openIds;
  }

  /**
   * Whether the task still has work to watch: an executing run, a queued or processing task,
   * a question or plan waiting on an answer, or data not read yet.
   */
  shouldKeepPolling(): boolean {
    if (this.executing || this.openIds.length > 0 || this.turnsRefreshPending) return true;
    if (this.turnsNeedingActivity().length > 0) return true;
    const task = this.task;
    if (task === undefined) return true;
    switch (task.status) {
      case "queued":
      case "processing":
        return true;
      case "awaiting_input":
        return task.awaiting_input.kind !== "message";
      case "errored":
        return false;
    }
  }

  applyLive(
    snapshot: AetherLiveConversationSnapshot,
    observedAt: string,
  ): Result.Result<AetherLiveStep, AetherTranscriptDivergedError> {
    const outputs: AetherIngestOutput[] = [];
    if (this.resetSequence !== undefined && snapshot.resetSequence !== this.resetSequence) {
      // Rows past the reset are gone. Re-read every turn from the newest page; rows that
      // survived keep their ids, so re-reading them emits nothing new.
      for (const turn of this.turns.values()) turn.revisionRead = undefined;
      this.turnsRefreshPending = true;
      outputs.push({ _tag: "ConversationReset", resetSequence: snapshot.resetSequence });
      outputs.push(
        this.warning(undefined, ["reset", String(snapshot.resetSequence)], observedAt, {
          message: "The Aether conversation was truncated; Synara re-read it from the first page.",
          detail: { resetSequence: snapshot.resetSequence },
        }),
      );
    }
    this.resetSequence = snapshot.resetSequence;
    this.executing = snapshot.activeExecution !== undefined;

    // Closed messages first: they were opened before anything still open.
    for (const message of snapshot.replacements) {
      const text = this.advanceText(message, message.conversationTurnId, true, "live", observedAt);
      if (Result.isFailure(text)) return Result.fail(text.failure);
      outputs.push(...text.success);
    }
    for (const message of snapshot.open) {
      const text = this.advanceText(message, message.conversationTurnId, false, "live", observedAt);
      if (Result.isFailure(text)) return Result.fail(text.failure);
      outputs.push(...text.success);
    }
    this.openIds = snapshot.open.map((message) => message.id);

    const refreshTurns = this.turnsRefreshPending || snapshot.headRevision !== this.lastLiveHead;
    this.lastLiveHead = snapshot.headRevision;
    this.turnsRefreshPending = refreshTurns;
    return Result.succeed({ outputs, refreshTurns });
  }

  applyTurns(page: AetherConversationTurnsPage): ReadonlyArray<AetherIngestOutput> {
    const outputs: AetherIngestOutput[] = [];
    this.task = page.task;
    this.turnsRefreshPending = false;
    for (const turn of page.turns) {
      const known = this.turns.get(turn.id);
      if (known === undefined) {
        this.turns.set(turn.id, {
          id: turn.id,
          state: turn.state,
          revision: turn.revision_sequence,
          revisionRead: undefined,
          lastUserMessageId: turn.root_message.id,
          answers: [],
          rows: new Map(),
        });
        outputs.push({
          _tag: "TurnOrigin",
          turnId: turn.id,
          rootMessageId: turn.root_message.id,
          ...(turn.root_message.clientMessageId === undefined
            ? {}
            : { clientMessageId: turn.root_message.clientMessageId }),
        });
      } else {
        known.state = turn.state;
        known.revision = turn.revision_sequence;
      }
    }
    this.turnOrder = page.turns.map((turn) => turn.id);
    return outputs;
  }

  /** Turns whose activity changed since it was last read. */
  turnsNeedingActivity(): ReadonlyArray<AetherTurnId> {
    return this.turnOrder.filter((turnId) => {
      const turn = this.turns.get(turnId);
      return (
        turn !== undefined && (turn.revisionRead === undefined || turn.revisionRead < turn.revision)
      );
    });
  }

  /**
   * Whether to read the page older than `page`. A turn never read is read whole; otherwise
   * paging stops at the first page that starts with a known row, once every row still open
   * (a running tool, a streaming message) has been read again.
   */
  needsOlderActivity(
    turnId: AetherTurnId,
    page: AetherActivityPageView,
    readSoFar: ReadonlySet<string>,
  ): boolean {
    if (!page.has_more_older || page.oldest_sequence_loaded === null) return false;
    const turn = this.turns.get(turnId);
    if (turn === undefined || turn.revisionRead === undefined) return true;
    const oldest = page.items[0];
    if (oldest === undefined || !turn.rows.has(oldest.id)) return true;
    for (const [rowId, row] of turn.rows) {
      const open =
        row.kind === "tool" || row.kind === "tasks"
          ? !row.terminal
          : this.messages.get(rowId)?.final === undefined && this.messages.has(rowId);
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
    const turn = this.turns.get(turnId);
    if (turn === undefined) return Result.succeed([]);
    const outputs: AetherIngestOutput[] = [];
    for (const item of items) {
      if (item.role === "user") {
        turn.lastUserMessageId = item.id;
        if (
          item.toolResponse !== undefined &&
          item.toolResponse.tool_name === "ask_user" &&
          !turn.rows.has(item.id)
        ) {
          turn.answers.push(item.toolResponse.data);
        }
        turn.rows.set(item.id, { kind: "done" });
        continue;
      }
      switch (item.variant) {
        case "text":
        case "thinking": {
          turn.rows.set(item.id, { kind: "done" });
          // While a message is open the live endpoint owns it.
          if (this.openIds.includes(item.id)) break;
          const text = this.advanceText(item, turnId, true, "activity", observedAt);
          if (Result.isFailure(text)) return text;
          outputs.push(...text.success);
          break;
        }
        case "tool":
          outputs.push(...this.applyTool(turn, item.id, item.tool, observedAt));
          break;
        case "seam":
          outputs.push(...this.applySeam(turn, item.id, item.seam.reason, observedAt));
          break;
      }
    }
    turn.revisionRead = Math.max(turn.revisionRead ?? 0, revision);
    return Result.succeed(outputs);
  }

  /**
   * After a refresh's reads: resolves an answered question or plan, surfaces a new one, and
   * signals every turn run that settled.
   */
  finishRefresh(observedAt: string): ReadonlyArray<AetherIngestOutput> {
    const outputs: AetherIngestOutput[] = [];
    const awaiting = this.awaitingAnswer();

    const pending = this.pending;
    if (pending !== undefined && awaiting?.tool_id !== pending.toolId) {
      if (pending.kind === "questions") {
        outputs.push(this.resolveQuestions(pending, observedAt));
      }
      outputs.push({
        _tag: "PendingInputResolved",
        turnId: pending.turnId,
        toolId: pending.toolId,
        kind: pending.kind,
      });
      this.pending = undefined;
    }
    if (awaiting !== undefined && !this.surfacedToolIds.has(awaiting.tool_id)) {
      outputs.push(...this.surfaceInput(awaiting, observedAt));
    }

    for (const turnId of this.turnOrder) {
      const turn = this.turns.get(turnId);
      if (turn === undefined || turn.revisionRead === undefined) continue;
      if (turn.revisionRead < turn.revision) continue;
      const settlement = this.settlementOf(turn);
      if (settlement === undefined) continue;
      const run = `${turn.id}:${turn.lastUserMessageId}`;
      if (this.settledRuns.has(run)) continue;
      this.settledRuns.add(run);
      outputs.push({
        _tag: "TurnSettled",
        turnId: turn.id,
        runMessageId: turn.lastUserMessageId,
        ...settlement,
      });
    }
    return outputs;
  }

  // -------------------------------------------------------------------------
  // Settlement and pending input
  // -------------------------------------------------------------------------

  private awaitingAnswer(): AwaitingAnswer | undefined {
    const task = this.task;
    if (task?.status !== "awaiting_input" || task.awaiting_input.kind === "message") {
      return undefined;
    }
    return task.awaiting_input;
  }

  private settlementOf(turn: TurnRecord): AetherTurnSettlement | undefined {
    const task = this.task;
    if (task === undefined) return undefined;
    if (turn.state === "awaiting_input") {
      const awaiting = this.awaitingAnswer();
      return awaiting?.kind === "plan" && this.pending?.turnId === turn.id
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
    return this.turnOrder.some((id) => {
      if (id === turnId) return false;
      const state = this.turns.get(id)?.state;
      return state === "queued" || state === "running" || state === "awaiting_input";
    });
  }

  private surfaceInput(awaiting: AwaitingAnswer, observedAt: string): AetherIngestOutput[] {
    this.surfacedToolIds.add(awaiting.tool_id);
    const turnId =
      this.turnOrder.find((id) => this.turns.get(id)?.state === "awaiting_input") ??
      this.turnOrder[this.turnOrder.length - 1];
    if (turnId === undefined) return [];
    const outputs: AetherIngestOutput[] = [
      {
        _tag: "PendingInput",
        turnId,
        toolId: awaiting.tool_id,
        kind: awaiting.kind,
        input: awaiting.input,
      },
    ];
    const refs = { requestId: awaiting.tool_id } as const;
    if (awaiting.kind === "questions") {
      const questions = questionsFromInput(awaiting.input);
      this.pending = {
        turnId,
        toolId: awaiting.tool_id,
        kind: "questions",
        questions: Option.getOrUndefined(Schema.decodeUnknownOption(AskUserInput)(awaiting.input))
          ?.questions,
      };
      outputs.push(
        questions === undefined
          ? this.warning(turnId, [awaiting.tool_id, "questions-unreadable"], observedAt, {
              message: "Aether is waiting on a question that Synara cannot display.",
              detail: { toolId: awaiting.tool_id, input: awaiting.input },
            })
          : this.event(turnId, [awaiting.tool_id, "user-input.requested"], observedAt, refs, {
              type: "user-input.requested",
              payload: { questions },
            }),
      );
      return outputs;
    }
    this.pending = { turnId, toolId: awaiting.tool_id, kind: "plan", questions: undefined };
    const plan = Schema.decodeUnknownOption(ProposePlanInput)(awaiting.input).pipe(
      Option.flatMap((input) => Option.fromNullishOr(nonEmpty(input.plan))),
    );
    outputs.push(
      Option.isNone(plan)
        ? this.warning(turnId, [awaiting.tool_id, "plan-unreadable"], observedAt, {
            message: "Aether is waiting on a plan approval that Synara cannot display.",
            detail: { toolId: awaiting.tool_id, input: awaiting.input },
          })
        : this.event(turnId, [awaiting.tool_id, "plan-proposed"], observedAt, refs, {
            type: "turn.proposed.completed",
            payload: { planMarkdown: plan.value },
          }),
    );
    return outputs;
  }

  private resolveQuestions(pending: PendingInput, observedAt: string): AetherIngestOutput {
    const turn = this.turns.get(pending.turnId);
    const response = turn?.answers[turn.answers.length - 1];
    const answers =
      pending.questions === undefined || response === undefined
        ? undefined
        : answersFromResponse(pending.questions, response);
    return this.event(
      pending.turnId,
      [pending.toolId, "user-input.resolved"],
      observedAt,
      { requestId: pending.toolId },
      // No readable answer (answered elsewhere without one, or cancelled): resolved, empty.
      { type: "user-input.resolved", payload: { answers: answers ?? {} } },
    );
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
    if (CLIENT_SURFACED_TOOL_NAMES.has(tool.name) || this.surfacedToolIds.has(tool.id)) {
      turn.rows.set(rowId, { kind: "client" });
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
      nonEmpty(tool.display.primary) ?? (status.status === "failed" ? nonEmpty(tool.error) : undefined);
    const payload = {
      itemType: kind.itemType,
      status: status.status,
      title: tool.display.label.trim(),
      ...(detail === undefined ? {} : { detail }),
      data: {
        toolCallId: tool.id,
        callId: tool.id,
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
    const type =
      status.terminal ? "item.completed" : previous === undefined ? "item.started" : "item.updated";
    // A lifecycle edge is one event per row and status; an in-progress change of the same
    // status is told apart by its content.
    const idParts = type === "item.updated" ? [rowId, status.raw, key] : [rowId, status.raw];
    turn.rows.set(rowId, { kind: "tool", key, terminal: status.terminal });
    outputs.push(this.event(turn.id, idParts, observedAt, refs, { type, payload }));
    return outputs;
  }

  private applyTaskList(
    turn: TurnRecord,
    rowId: string,
    tool: AetherTimelineTool,
    status: ToolStatus,
    observedAt: string,
  ): AetherIngestOutput[] {
    const previous = turn.rows.get(rowId);
    const tasks = taskListFromInput(tool.input);
    if (tasks === undefined) {
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
            payload: { itemType: "context_compaction", status: "completed", title: "Context compacted" },
          },
        ),
      ];
    }
    const raw = isUnknown(reason) ? reason.raw : reason;
    return [
      this.warning(turn.id, [rowId, "seam"], observedAt, {
        message: isUnknown(reason)
          ? `Aether marked a conversation seam Synara does not recognize (${raw}).`
          : `Aether marked a conversation seam (${raw}).`,
        detail: { reason: raw },
      }),
    ];
  }

  /** Streams the new suffix of a message's accumulated text; completes it once closed. */
  private advanceText(
    message: { readonly id: AetherMessageId; readonly variant: "text" | "thinking"; readonly content: string },
    turnId: AetherTurnId,
    closed: boolean,
    source: "live" | "activity",
    observedAt: string,
  ): Result.Result<AetherIngestOutput[], AetherTranscriptDivergedError> {
    let record = this.messages.get(message.id);
    if (record === undefined) {
      const seeded = this.seedLengths.get(message.id) ?? 0;
      if (seeded > message.content.length) {
        return Result.fail(this.diverged(turnId, message.id, "seed", seeded, message.content.length));
      }
      record = {
        turnId,
        variant: message.variant,
        streamed: message.content.slice(0, seeded),
        final: undefined,
      };
      this.messages.set(message.id, record);
    }
    if (record.final !== undefined) {
      return message.content === record.final
        ? Result.succeed([])
        : Result.fail(
            this.diverged(turnId, message.id, source, record.final.length, message.content.length),
          );
    }
    if (!message.content.startsWith(record.streamed)) {
      return Result.fail(
        this.diverged(turnId, message.id, source, record.streamed.length, message.content.length),
      );
    }

    const outputs: AetherIngestOutput[] = [];
    const refs = { itemId: message.id } as const;
    const suffix = message.content.slice(record.streamed.length);
    // A message first seen closed streams nothing: its completion carries the whole text.
    if (suffix !== "" && (!closed || record.streamed !== "")) {
      outputs.push(
        this.event(turnId, [message.id, "delta", String(message.content.length)], observedAt, refs, {
          type: "content.delta",
          payload: {
            streamKind: message.variant === "text" ? "assistant_text" : "reasoning_text",
            delta: suffix,
          },
        }),
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

  private diverged(
    turnId: AetherTurnId,
    messageId: string,
    source: "live" | "activity" | "seed",
    streamedLength: number,
    receivedLength: number,
  ): AetherTranscriptDivergedError {
    return new AetherTranscriptDivergedError({
      taskId: this.context.taskId,
      turnId,
      messageId,
      source,
      streamedLength,
      receivedLength,
    });
  }

  private event(
    turnId: AetherTurnId | undefined,
    idParts: ReadonlyArray<string>,
    observedAt: string,
    refs: { readonly itemId?: string; readonly providerItemId?: string; readonly requestId?: string },
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
    if (this.warnedOnce.has(key)) return [];
    this.warnedOnce.add(key);
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
  refs: { readonly itemId?: string; readonly providerItemId?: string; readonly requestId?: string } = {},
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

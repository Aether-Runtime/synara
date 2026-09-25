// FILE: aetherApiSchemas.ts
// Purpose: Wire schemas for the Aether REST API: the response fields Synara reads, the error
// bodies it branches on, and the request bodies it sends.
// Layer: Server provider boundary (Aether). Source of truth: Aether's OpenAPI (`aether-api`
// 0.1.0, 2026-09-24).
//
// Decoding rules:
// - Unknown keys are ignored (`Schema.Struct` strips them); Aether adds fields over time.
// - Only fields a caller reads are declared. Each is required exactly when the OpenAPI requires
//   it, so a missing one fails the decode, naming it.
// - An Aether enum that drives the protocol (task `status`, turn `state`, `awaiting_input.kind`,
//   what the client sends next) is strict: an unknown value is a decode error naming the field.
//   A vocabulary Aether grows and Synara only displays (tool `status` and `itemType`, seam
//   `reason`) is open: an unknown value decodes to `{ _tag: "Unknown", raw }`, so one new tool type
//   cannot fail a whole activity page. Enums nobody reads are not decoded at all.
// - IDs are branded here, once; every client method takes the branded type.
// - Nothing is defaulted.
//
// Requests are plain types. Aether validates them and answers 422, so they are not re-checked.

import type { AetherAgentFamily } from "@synara/contracts";
import { NonNegativeInt, PositiveInt } from "@synara/contracts";
import { Schema, SchemaTransformation } from "effect";

const NonEmptyString = Schema.String.check(Schema.isMinLength(1));
const Uuid = Schema.String.check(Schema.isUUID(undefined));

// Path IDs are UUIDs, so an ID decoded here is always safe to put in a URL path.
export const AetherTaskId = Uuid.pipe(Schema.brand("AetherTaskId"));
export type AetherTaskId = typeof AetherTaskId.Type;
export const AetherProjectId = Uuid.pipe(Schema.brand("AetherProjectId"));
export type AetherProjectId = typeof AetherProjectId.Type;
export const AetherWorkspaceId = Uuid.pipe(Schema.brand("AetherWorkspaceId"));
export type AetherWorkspaceId = typeof AetherWorkspaceId.Type;
export const AetherTurnId = Uuid.pipe(Schema.brand("AetherTurnId"));
export type AetherTurnId = typeof AetherTurnId.Type;
export const AetherMessageId = NonEmptyString.pipe(Schema.brand("AetherMessageId"));
export type AetherMessageId = typeof AetherMessageId.Type;
export const AetherToolId = NonEmptyString.pipe(Schema.brand("AetherToolId"));
export type AetherToolId = typeof AetherToolId.Type;

/** A value of an open vocabulary that this client does not know, kept as sent. */
export interface AetherUnknownValue {
  readonly _tag: "Unknown";
  readonly raw: string;
}

/** A known value decodes to its literal; anything else to `AetherUnknownValue`. */
const openVocabulary = <const Known extends ReadonlyArray<string>>(known: Known) =>
  Schema.String.pipe(
    Schema.decodeTo(
      Schema.Union([
        Schema.Literals(known),
        Schema.Struct({ _tag: Schema.Literal("Unknown"), raw: Schema.String }),
      ]),
      SchemaTransformation.transform<Known[number] | AetherUnknownValue, string>({
        decode: (raw) => (known.includes(raw) ? (raw as Known[number]) : { _tag: "Unknown", raw }),
        encode: (value) => (typeof value === "string" ? value : value.raw),
      }),
    ),
  );

// ---------------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------------

/** `GET /profile`: the health check reads `email`. */
export const AetherProfile = Schema.Struct({ email: NonEmptyString });

/** A `GET /projects` entry: a worktree's project is found by its `repo_url`. */
export const AetherProject = Schema.Struct({
  id: AetherProjectId,
  repo_url: Schema.optionalKey(Schema.String),
});
export type AetherProject = typeof AetherProject.Type;
export const AetherProjectList = Schema.Struct({ projects: Schema.Array(AetherProject) });

export const AetherCreateTaskResponse = Schema.Struct({ id: AetherTaskId });
export type AetherCreateTaskResponse = typeof AetherCreateTaskResponse.Type;

/** The created user message's id: the `id` its timeline row carries. */
export const AetherRespondToTaskResponse = Schema.Struct({ message_id: AetherMessageId });
export type AetherRespondToTaskResponse = typeof AetherRespondToTaskResponse.Type;

/**
 * What the task waits for. `input` is kept whole, every key as sent: the respond fence
 * (`expected_input_digest`) is a digest of the complete input.
 */
export const AetherAwaitingInput = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("message") }),
  Schema.Struct({
    kind: Schema.Literals(["questions", "plan"]),
    tool_id: AetherToolId,
    input: Schema.Record(Schema.String, Schema.Unknown),
  }),
]);
export type AetherAwaitingInput = typeof AetherAwaitingInput.Type;

/** `run_context.workspace_id` is the workspace to connect to. */
const RunContext = Schema.Struct({ workspace_id: AetherWorkspaceId });

/** The task on every conversation page, by status. */
export const AetherTask = Schema.Union([
  Schema.Struct({ status: Schema.Literal("queued"), run_context: Schema.NullOr(RunContext) }),
  Schema.Struct({ status: Schema.Literal("processing"), run_context: RunContext }),
  Schema.Struct({
    status: Schema.Literal("awaiting_input"),
    // Null when every queued message was cancelled before a workspace was assigned.
    run_context: Schema.NullOr(RunContext),
    awaiting_input: AetherAwaitingInput,
  }),
  Schema.Struct({
    status: Schema.Literal("errored"),
    run_context: Schema.NullOr(RunContext),
    error: NonEmptyString,
  }),
]);
export type AetherTask = typeof AetherTask.Type;

/**
 * The answer a user message carried to a pending question or plan. `data` is the answer as sent
 * (`{ answers, customAnswers? }` for `ask_user`); ingestion decodes it where it reads it.
 */
export const AetherToolResponseRow = Schema.Struct({
  tool_name: Schema.String,
  data: Schema.Unknown,
});

export const AetherUserMessage = Schema.Struct({
  id: AetherMessageId,
  role: Schema.Literal("user"),
  content: Schema.String,
  /** Present on messages sent with `client_message_id`: tells Synara's turns from others'. */
  clientMessageId: Schema.optionalKey(Schema.String),
  /** Present on the message that answered a question or plan. */
  toolResponse: Schema.optionalKey(AetherToolResponseRow),
});
export type AetherUserMessage = typeof AetherUserMessage.Type;

const assistantRow = <const Variant extends string>(variant: Variant) => ({
  id: AetherMessageId,
  role: Schema.Literal("assistant"),
  variant: Schema.Literal(variant),
});

export const AetherTimelineTool = Schema.Struct({
  id: AetherToolId,
  name: NonEmptyString,
  input: Schema.Record(Schema.String, Schema.Unknown),
  status: openVocabulary([
    "input-streaming",
    "input-available",
    "output-available",
    "output-error",
    "output-denied",
    "approval-responded",
    "approval-requested",
  ]),
  /** `primary` is the call's subject: the command, or the file path. */
  display: Schema.Struct({ label: NonEmptyString, primary: Schema.optionalKey(Schema.String) }),
  itemType: Schema.optionalKey(
    openVocabulary([
      "user_message",
      "assistant_message",
      "reasoning",
      "plan",
      "command_execution",
      "file_change",
      "file_read",
      "mcp_tool_call",
      "dynamic_tool_call",
      "collab_agent_tool_call",
      "web_search",
      "web_fetch",
      "image_view",
      "task_tracking",
      "subagent_invocation",
      "review_entered",
      "review_exited",
      "context_compaction",
      "error",
      "unknown",
    ]),
  ),
  result: Schema.optionalKey(Schema.String),
  error: Schema.optionalKey(Schema.String),
  // Top-level on tool rows (not inside `result`).
  exitCode: Schema.optionalKey(Schema.Int),
  durationMs: Schema.optionalKey(NonNegativeInt),
});
export type AetherTimelineTool = typeof AetherTimelineTool.Type;

/** One row of a turn's activity: user, assistant text, thinking, tool, or seam. */
export const AetherTimelineMessage = Schema.Union([
  AetherUserMessage,
  Schema.Struct({ ...assistantRow("text"), content: Schema.String }),
  Schema.Struct({ ...assistantRow("thinking"), content: Schema.String }),
  Schema.Struct({ ...assistantRow("tool"), tool: AetherTimelineTool }),
  Schema.Struct({
    ...assistantRow("seam"),
    seam: Schema.Struct({ reason: openVocabulary(["teleport", "compaction"]) }),
  }),
]);
export type AetherTimelineMessage = typeof AetherTimelineMessage.Type;

const pageFields = {
  has_more_older: Schema.Boolean,
  /** The `before` cursor for the next older page. */
  oldest_sequence_loaded: Schema.NullOr(NonNegativeInt),
} as const;

/** `GET /tasks/{taskID}/conversation/turns`. */
export const AetherConversationTurnsPage = Schema.Struct({
  task: AetherTask,
  head_revision: NonNegativeInt,
  turns: Schema.Array(
    Schema.Struct({
      id: AetherTurnId,
      root_message: AetherUserMessage,
      members: Schema.Array(AetherUserMessage),
      state: Schema.Literals(["queued", "running", "awaiting_input", "settled", "errored"]),
      /** Moves whenever anything in the turn changes; re-read its activity when it does. */
      revision_sequence: NonNegativeInt,
    }),
  ),
  ...pageFields,
});
export type AetherConversationTurnsPage = typeof AetherConversationTurnsPage.Type;

/** `GET /tasks/{taskID}/conversation/turns/{turnID}/activity`. */
export const AetherTurnActivityPage = Schema.Struct({
  items: Schema.Array(AetherTimelineMessage),
  turn_revision_sequence: NonNegativeInt,
  ...pageFields,
});
export type AetherTurnActivityPage = typeof AetherTurnActivityPage.Type;

/** An open text or thinking message with its full accumulated content (not a delta). */
const AetherLiveMessage = Schema.Struct({
  id: AetherMessageId,
  conversationTurnId: AetherTurnId,
  variant: Schema.Literals(["text", "thinking"]),
  content: Schema.String,
});

/** `GET /tasks/{taskID}/conversation/live`. */
export const AetherLiveConversationSnapshot = Schema.Struct({
  /** Present while an agent run is executing, absent otherwise. */
  activeExecution: Schema.optionalKey(
    Schema.Struct({
      conversationTurnId: AetherTurnId,
      /** The user message the run is answering. */
      processingMessageId: AetherMessageId,
    }),
  ),
  headRevision: NonNegativeInt,
  /** Moves when the conversation was truncated; cached rows past it are gone. */
  resetSequence: NonNegativeInt,
  open: Schema.Array(AetherLiveMessage),
  replacements: Schema.Array(AetherLiveMessage),
});
export type AetherLiveConversationSnapshot = typeof AetherLiveConversationSnapshot.Type;

/** `POST /workspaces/{workspaceID}/connect` 200: a socket to open, or ask again later. */
export const AetherWorkspaceConnect = Schema.Union([
  Schema.Struct({
    state: Schema.Literal("running"),
    transport: Schema.Struct({ websocket_path: NonEmptyString }),
  }),
  Schema.Struct({ state: Schema.Literal("connecting"), retry_after_ms: PositiveInt }),
]);
export type AetherWorkspaceConnect = typeof AetherWorkspaceConnect.Type;

// ---------------------------------------------------------------------------
// Error bodies
// ---------------------------------------------------------------------------

/** The generic error body. Read only for its message. */
export const AetherErrorBody = Schema.Struct({
  error: NonEmptyString,
  code: Schema.optionalKey(Schema.String),
});

/** 402: the workspace runtime was not admitted for the account's plan. */
export const AetherAdmissionErrorBody = Schema.Struct({
  error: NonEmptyString,
  code: Schema.String,
  tier: Schema.String,
  vm_size: Schema.String,
});

const conflict = <const Code extends string>(code: Code) =>
  Schema.Struct({ error: NonEmptyString, code: Schema.Literal(code) });

const IdempotencyKeyReused = conflict("idempotency_key_reused");
const QueuedStopRequiresDiscard = conflict("queued_stop_requires_discard");
const TaskCreatorDeleted = conflict("task_creator_deleted");
const StalePendingInput = Schema.Struct({
  error: NonEmptyString,
  code: Schema.Literal("stale_pending_input"),
  current_tool_id: Schema.optionalKey(AetherToolId),
});
/**
 * A respond conflict naming the kind of input the task waits for. The OpenAPI's respond 409
 * union omits it (and `ErrorResponse` forbids the extra key), but the AetherDriver received and
 * handled it. Unconfirmed against the current server. A body with an `awaiting_input_kind`
 * outside these three decodes as the generic `ErrorResponse` variant.
 */
const AwaitingInputConflict = Schema.Struct({
  error: NonEmptyString,
  code: Schema.optionalKey(Schema.String),
  awaiting_input_kind: Schema.Literals(["message", "questions", "plan"]),
});
const ConnectTransitional = Schema.Struct({
  kind: Schema.Literal("transitional"),
  error: NonEmptyString,
  retry_after_ms: PositiveInt,
});
const ConnectNotRunning = Schema.Struct({
  kind: Schema.Literals(["startable", "not_connectable"]),
  error: NonEmptyString,
});

/** Each endpoint's 409 bodies, as the OpenAPI declares them. The first match wins. */
export const AetherConflictBodies = {
  createTask: IdempotencyKeyReused,
  respondToTask: Schema.Union([
    StalePendingInput,
    TaskCreatorDeleted,
    AwaitingInputConflict,
    AetherErrorBody,
  ]),
  stopTask: Schema.Union([QueuedStopRequiresDiscard, AetherErrorBody]),
  connectWorkspace: Schema.Union([ConnectTransitional, ConnectNotRunning, TaskCreatorDeleted]),
} as const;

/** Any endpoint's decoded 409 body. The generic body is last so it never absorbs another. */
export const AetherConflictBody = Schema.Union([
  IdempotencyKeyReused,
  QueuedStopRequiresDiscard,
  TaskCreatorDeleted,
  StalePendingInput,
  AwaitingInputConflict,
  ConnectTransitional,
  ConnectNotRunning,
  AetherErrorBody,
]);
export type AetherConflictBody = typeof AetherConflictBody.Type;

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

export interface AetherPromptContext {
  readonly attachments?: ReadonlyArray<{
    readonly filename: string;
    readonly mediaType: string;
    /** Base64. */
    readonly data: string;
  }>;
  readonly files?: ReadonlyArray<{
    readonly path: string;
    readonly include: boolean;
    readonly selection?: { readonly startLine: number; readonly endLine: number };
  }>;
}

/** `POST /tasks`. */
export interface AetherCreateTaskRequest {
  readonly project_id: AetherProjectId;
  readonly prompt: string;
  readonly agent_type: AetherAgentFamily;
  readonly model: string;
  readonly interaction_mode: "default" | "plan";
  /**
   * A fresh UUID per task, reused on every retry. A lost 202 retried with the same key returns
   * the same task instead of starting a second VM; the same key with a different request is 409
   * `idempotency_key_reused`.
   */
  readonly idempotency_key: string;
  readonly base_branch?: string;
  readonly reasoning_effort?: string | null;
  readonly context?: AetherPromptContext;
  readonly auto_fix_ci?: boolean;
  readonly auto_fix_pr_comments?: boolean;
  readonly auto_rebase?: boolean;
}

/**
 * An answer to the pending question or plan, fenced by the pending tool id (and optionally the
 * input digest), so Aether refuses a stale answer with 409 `stale_pending_input`.
 */
export type AetherToolResponse = (
  | {
      readonly tool_name: "ask_user";
      readonly data: {
        readonly answers: Readonly<Record<string, ReadonlyArray<number>>>;
        readonly customAnswers?: Readonly<Record<string, string>>;
      };
    }
  | {
      readonly tool_name: "propose_plan";
      readonly data: { readonly approved: boolean; readonly feedback?: string };
    }
) & { readonly expected_tool_id: AetherToolId; readonly expected_input_digest?: string };

/** `POST /tasks/{taskID}/respond`. Steers a running task unless `queue_for_later`. */
export interface AetherRespondToTaskRequest {
  readonly message: string;
  /** A fresh UUID per message, reused on every retry: a retry resolves to the original row. */
  readonly client_message_id: string;
  readonly queue_for_later?: boolean;
  readonly context?: AetherPromptContext;
  readonly interaction_mode?: "default" | "plan";
  readonly reasoning_effort?: string;
  readonly tool_response?: AetherToolResponse;
}

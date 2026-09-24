// FILE: aetherApiSchemas.ts
// Purpose: Wire schemas for the Aether REST API: request bodies, response bodies and the
// structured error bodies the client branches on.
// Layer: Server provider boundary (Aether). Source of truth: Aether's OpenAPI (`aether-api`
// 0.1.0, 2026-09-24), the 13 operations listed in plans/aether-cloud-provider.md step P2.
//
// Decoding rules, shared by every response schema here:
// - Unknown keys are ignored: Aether adds fields over time, and `Schema.Struct` strips them.
// - Each response schema declares the fields Synara reads. A declared field is required exactly
//   when the OpenAPI requires it, so a missing required field fails the decode and names it.
// - Discriminators that pick a variant or decide control flow (task `status`,
//   `awaiting_input.kind`, turn `state`, message `role`/`variant`, seam `reason`, live
//   `variant`, connect `state`) are closed `Literals`: an unknown value fails the decode and
//   names the field instead of being guessed.
// - Descriptive enums on rows and settings (activity item `type`, tool `status`/`itemType`,
//   `deliveryStatus`, `messageEvent.kind`, subagent `status`, PR `state`, agent/model settings)
//   are open strings, so one value Aether adds does not fail the whole task or page it sits in.
//   Each has an exported list of the values this client knows; a consumer narrows with
//   `isKnownAetherValue` and handles (and reports) the rest itself. Nothing maps an unknown
//   value to a known one.
// - Nothing is defaulted. Optional fields stay absent (`optionalKey`), nullable ones stay null.
//
// Request schemas mirror the OpenAPI request bodies in full and are encoded with
// `onExcessProperty: "error"`, so a request is validated before anything is sent.

import { NonNegativeInt, PositiveInt, TrimmedNonEmptyString } from "@synara/contracts";
import { Schema } from "effect";

// ---------------------------------------------------------------------------
// Shared primitives
// ---------------------------------------------------------------------------

const NonEmptyString = Schema.String.check(Schema.isMinLength(1));
/** OpenAPI `pattern: "\\S"`: at least one non-whitespace character. */
const NonBlankString = Schema.String.check(Schema.isPattern(/\S/u));
const SafeNonNegativeInt = NonNegativeInt.check(
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
);
const Uuid = Schema.String.check(Schema.isUUID(undefined));
/** The lowercase UUID pattern Aether declares on `taskID` and `turnID` path parameters. */
const LOWERCASE_UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;
const NIL_UUID = "00000000-0000-0000-0000-000000000000";

export const AetherTaskIdParam = Schema.String.check(Schema.isPattern(LOWERCASE_UUID_PATTERN));
export const AetherTurnIdParam = AetherTaskIdParam.check(
  Schema.makeFilter<string>((value) =>
    value === NIL_UUID ? "The nil UUID is not a turn id." : undefined,
  ),
);
export const AetherProjectIdParam = Uuid;
export const AetherWorkspaceIdParam = Uuid;

/** Whether an open-enum value is one this client knows (see the decoding rules above). */
export function isKnownAetherValue<const V extends string>(
  known: ReadonlyArray<V>,
  value: string,
): value is V {
  return (known as ReadonlyArray<string>).includes(value);
}

export const AETHER_AGENT_TYPES = ["codex", "claude-code", "opencode", "cursor"] as const;
export const AetherAgentType = Schema.Literals(AETHER_AGENT_TYPES);
export type AetherAgentType = typeof AetherAgentType.Type;

export const AETHER_INTERACTION_MODES = ["default", "plan"] as const;
export const AetherInteractionMode = Schema.Literals(AETHER_INTERACTION_MODES);
export type AetherInteractionMode = typeof AetherInteractionMode.Type;

export const AETHER_REASONING_EFFORTS = [
  "ultra",
  "max",
  "xhigh",
  "high",
  "medium",
  "low",
  "ultrathink",
  "ultracode",
  "none",
] as const;
export const AetherReasoningEffort = Schema.Literals(AETHER_REASONING_EFFORTS);
export type AetherReasoningEffort = typeof AetherReasoningEffort.Type;

export const AETHER_VM_SIZES = ["small", "medium", "large"] as const;
export const AetherVmSize = Schema.Literals(AETHER_VM_SIZES);
export type AetherVmSize = typeof AetherVmSize.Type;

// ---------------------------------------------------------------------------
// Profile and projects
// ---------------------------------------------------------------------------

/** `GET /profile`. Only the health check reads it, and only `email`. */
export const AetherProfile = Schema.Struct({ email: TrimmedNonEmptyString });
export type AetherProfile = typeof AetherProfile.Type;

// Settings enums are open on read (known values: AETHER_AGENT_TYPES, AETHER_INTERACTION_MODES,
// AETHER_REASONING_EFFORTS, AETHER_VM_SIZES): a value set in Aether's web app that this client
// does not know must not make every project or task unreadable. Request schemas stay closed, so
// echoing an unknown value back fails that one request, naming the field.
export const AetherProjectTaskDefaults = Schema.Struct({
  agent_type: Schema.String,
  model: NonEmptyString,
  interaction_mode: Schema.String,
  reasoning_effort: Schema.optionalKey(Schema.NullOr(Schema.String)),
  auto_fix_ci: Schema.Boolean,
  auto_fix_pr_comments: Schema.Boolean,
  auto_rebase: Schema.Boolean,
  post_pr_screenshots: Schema.Boolean,
  post_pr_demo_video: Schema.Boolean,
  default_vm_size: Schema.optionalKey(Schema.NullOr(Schema.String)),
});
export type AetherProjectTaskDefaults = typeof AetherProjectTaskDefaults.Type;

/** `GET /projects/{id}` and each `GET /projects` entry. `repo_url` resolves a worktree's project. */
export const AetherProject = Schema.Struct({
  id: NonEmptyString,
  name: NonEmptyString,
  repo_url: Schema.optionalKey(Schema.String),
  default_branch: Schema.optionalKey(NonEmptyString),
  task_defaults: AetherProjectTaskDefaults,
});
export type AetherProject = typeof AetherProject.Type;

export const AetherProjectList = Schema.Struct({ projects: Schema.Array(AetherProject) });
export type AetherProjectList = typeof AetherProjectList.Type;

// ---------------------------------------------------------------------------
// Pending input (awaiting_input) and tool responses
// ---------------------------------------------------------------------------

// The pending input is echoed back as a SHA-256 digest (`expected_input_digest`), which Aether
// computes over the complete input. These schemas therefore keep every key they do not declare
// (`StructWithRest`), at every level, so the decoded value still holds the full input.
const PreservedKeys = [Schema.Record(Schema.String, Schema.Unknown)] as const;

export const AetherAskUserQuestionOption = Schema.StructWithRest(
  Schema.Struct({
    label: NonBlankString,
    description: Schema.optionalKey(Schema.String),
    preview: Schema.optionalKey(Schema.String),
  }),
  PreservedKeys,
);

export const AetherAskUserQuestion = Schema.StructWithRest(
  Schema.Struct({
    question: NonBlankString,
    options: Schema.Array(AetherAskUserQuestionOption),
    header: Schema.optionalKey(Schema.String),
    id: Schema.optionalKey(Schema.String),
    multiSelect: Schema.optionalKey(Schema.NullOr(Schema.Boolean)),
  }),
  PreservedKeys,
);
export type AetherAskUserQuestion = typeof AetherAskUserQuestion.Type;

export const AetherAskUserToolInput = Schema.StructWithRest(
  Schema.Struct({
    questions: Schema.Array(AetherAskUserQuestion).check(Schema.isMinLength(1)),
  }),
  PreservedKeys,
);
export type AetherAskUserToolInput = typeof AetherAskUserToolInput.Type;

export const AetherProposePlanToolInput = Schema.StructWithRest(
  Schema.Struct({ summary: Schema.String, plan: Schema.String }),
  PreservedKeys,
);
export type AetherProposePlanToolInput = typeof AetherProposePlanToolInput.Type;

export const AetherAwaitingInput = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("message") }),
  Schema.Struct({
    kind: Schema.Literal("questions"),
    tool_id: NonEmptyString,
    input: AetherAskUserToolInput,
  }),
  Schema.Struct({
    kind: Schema.Literal("plan"),
    tool_id: NonEmptyString,
    input: AetherProposePlanToolInput,
  }),
]);
export type AetherAwaitingInput = typeof AetherAwaitingInput.Type;
export type AetherAwaitingInputKind = AetherAwaitingInput["kind"];
export const AETHER_AWAITING_INPUT_KINDS = [
  "message",
  "questions",
  "plan",
] as const satisfies ReadonlyArray<AetherAwaitingInputKind>;

const AskUserToolResponseData = Schema.Struct({
  answers: Schema.Record(
    Schema.String,
    Schema.Array(Schema.Int.check(Schema.isGreaterThanOrEqualTo(-1))),
  ),
  customAnswers: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
});
const ProposePlanToolResponseData = Schema.Struct({
  approved: Schema.Boolean,
  feedback: Schema.optionalKey(Schema.String),
});

/** A tool response as it appears on a timeline user message. */
export const AetherToolResponse = Schema.Union([
  Schema.Struct({ tool_name: Schema.Literal("ask_user"), data: AskUserToolResponseData }),
  Schema.Struct({ tool_name: Schema.Literal("propose_plan"), data: ProposePlanToolResponseData }),
]);
export type AetherToolResponse = typeof AetherToolResponse.Type;

const ExpectedToolId = NonBlankString;
const ExpectedInputDigest = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/u));

/**
 * The `tool_response` of `POST /tasks/{taskID}/respond`. The fenced variants carry the pending
 * tool id (and optionally the input digest) so Aether refuses a stale answer with 409
 * `stale_pending_input`. A digest without a tool id matches no variant.
 */
export const AetherRespondToolResponse = Schema.Union([
  Schema.Struct({ tool_name: Schema.Literal("ask_user"), data: AskUserToolResponseData }),
  Schema.Struct({ tool_name: Schema.Literal("propose_plan"), data: ProposePlanToolResponseData }),
  Schema.Struct({
    tool_name: Schema.Literal("ask_user"),
    data: AskUserToolResponseData,
    expected_tool_id: ExpectedToolId,
    expected_input_digest: Schema.optionalKey(ExpectedInputDigest),
  }),
  Schema.Struct({
    tool_name: Schema.Literal("propose_plan"),
    data: ProposePlanToolResponseData,
    expected_tool_id: ExpectedToolId,
    expected_input_digest: Schema.optionalKey(ExpectedInputDigest),
  }),
]);
export type AetherRespondToolResponse = typeof AetherRespondToolResponse.Type;

// ---------------------------------------------------------------------------
// Tasks
// ---------------------------------------------------------------------------

export const AetherTaskRunContext = Schema.Struct({
  workspace_id: NonEmptyString,
  started_at: NonEmptyString,
});
export type AetherTaskRunContext = typeof AetherTaskRunContext.Type;

export const AetherTaskPullRequest = Schema.Struct({
  pull_request_id: NonEmptyString,
  pr_number: PositiveInt,
  html_url: NonEmptyString,
  title: Schema.String,
  /** Open enum; known values: `AETHER_PULL_REQUEST_STATES`. */
  state: Schema.optionalKey(Schema.String),
  branch_name: Schema.optionalKey(NonEmptyString),
});
export const AETHER_PULL_REQUEST_STATES = ["open", "closed", "merged"] as const;
export type AetherTaskPullRequest = typeof AetherTaskPullRequest.Type;

export const AetherTaskActivityItem = Schema.Struct({
  id: NonEmptyString,
  task_id: NonEmptyString,
  created_at: NonEmptyString,
  sequence: SafeNonNegativeInt,
  /** Open enum; known values: `AETHER_TASK_ACTIVITY_TYPES`. */
  type: Schema.String,
  message: NonBlankString,
});
export type AetherTaskActivityItem = typeof AetherTaskActivityItem.Type;
export const AETHER_TASK_ACTIVITY_TYPES = [
  "done",
  "error",
  "question",
  "stop",
  "workspace_lifecycle",
  "workspace_warning",
  "ci_passed",
  "ci_failed",
] as const;

// Fields every task status variant carries. `latest_sequence` no longer exists on the read
// model; `latest_message_revision` is compared for equality only (see the OpenAPI description).
// The settings fields are here because `PUT /tasks/{taskID}` is a full replace: a caller reads
// them back rather than overwriting a change made in Aether's web app.
const taskFields = {
  id: NonEmptyString,
  project_id: NonEmptyString,
  parent_task_id: Schema.NullOr(Schema.String),
  name: NonEmptyString,
  // Open settings enums, as on the project defaults above.
  agent_type: Schema.String,
  model: NonEmptyString,
  interaction_mode: Schema.String,
  reasoning_effort: Schema.optionalKey(Schema.NullOr(Schema.String)),
  auto_fix_ci: Schema.Boolean,
  auto_fix_pr_comments: Schema.Boolean,
  auto_rebase: Schema.Boolean,
  post_pr_screenshots: Schema.Boolean,
  post_pr_demo_video: Schema.Boolean,
  creator_reauth_required: Schema.Boolean,
  base_branch: NonBlankString,
  head_branch: Schema.optionalKey(NonEmptyString),
  primary_pull_request: Schema.NullOr(AetherTaskPullRequest),
  created_at: NonEmptyString,
  archived_at: Schema.optionalKey(Schema.NullOr(Schema.String)),
  latest_message_revision: SafeNonNegativeInt,
} as const;

// Only the `...WithActivity` variants (`GET`/`PUT /tasks/{taskID}`) carry these.
const activityFields = {
  activity_items: Schema.Array(AetherTaskActivityItem),
  latest_workspace_warning: Schema.optionalKey(NonEmptyString),
} as const;

const queuedFields = {
  ...taskFields,
  status: Schema.Literal("queued"),
  run_context: Schema.NullOr(AetherTaskRunContext),
} as const;
const processingFields = {
  ...taskFields,
  status: Schema.Literal("processing"),
  // Always present while processing.
  run_context: AetherTaskRunContext,
} as const;
const awaitingInputFields = {
  ...taskFields,
  status: Schema.Literal("awaiting_input"),
  // Null when every queued message was cancelled before a workspace was assigned.
  run_context: Schema.NullOr(AetherTaskRunContext),
  awaiting_input: AetherAwaitingInput,
} as const;
const erroredFields = {
  ...taskFields,
  status: Schema.Literal("errored"),
  run_context: Schema.NullOr(AetherTaskRunContext),
  error: NonEmptyString,
  completed_at: NonEmptyString,
} as const;

/** The task summary embedded in conversation pages (`TaskQueued` … `TaskErrored`). */
export const AetherTask = Schema.Union([
  Schema.Struct(queuedFields),
  Schema.Struct(processingFields),
  Schema.Struct(awaitingInputFields),
  Schema.Struct(erroredFields),
]);
export type AetherTask = typeof AetherTask.Type;
export type AetherTaskStatus = AetherTask["status"];

/** `GET`/`PUT /tasks/{taskID}` (`Task…WithActivity`). */
export const AetherTaskWithActivity = Schema.Union([
  Schema.Struct({ ...queuedFields, ...activityFields }),
  Schema.Struct({ ...processingFields, ...activityFields }),
  Schema.Struct({ ...awaitingInputFields, ...activityFields }),
  Schema.Struct({ ...erroredFields, ...activityFields }),
]);
export type AetherTaskWithActivity = typeof AetherTaskWithActivity.Type;

export const AetherCreateTaskResponse = Schema.Struct({ id: NonEmptyString, name: NonEmptyString });
export type AetherCreateTaskResponse = typeof AetherCreateTaskResponse.Type;

/** The created user message's id: the same `id` its timeline row carries. */
export const AetherRespondToTaskResponse = Schema.Struct({ message_id: NonEmptyString });
export type AetherRespondToTaskResponse = typeof AetherRespondToTaskResponse.Type;

// ---------------------------------------------------------------------------
// Conversation timeline
// ---------------------------------------------------------------------------

export const AetherFileReference = Schema.Struct({
  path: NonEmptyString,
  include: Schema.Boolean,
  selection: Schema.optionalKey(Schema.Struct({ startLine: PositiveInt, endLine: PositiveInt })),
});
export type AetherFileReference = typeof AetherFileReference.Type;

export const AetherConversationAttachment = Schema.Struct({
  filename: NonEmptyString,
  mediaType: NonEmptyString,
  taskId: NonEmptyString,
  messageId: NonEmptyString,
  index: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0), Schema.isLessThanOrEqualTo(9)),
  sha256: Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/u)),
});
export type AetherConversationAttachment = typeof AetherConversationAttachment.Type;

export const AetherConversationContext = Schema.Struct({
  attachments: Schema.optionalKey(Schema.Array(AetherConversationAttachment)),
  files: Schema.optionalKey(Schema.Array(AetherFileReference)),
});

/**
 * Why a user message exists when a person did not type it (PR comment, CI failure, parent
 * task message, …). `kind` is an open enum (known values: `AETHER_MESSAGE_EVENT_KINDS`); each
 * kind's payload is kept as sent until a consumer renders it.
 */
export const AetherMessageEvent = Schema.StructWithRest(
  Schema.Struct({ kind: NonEmptyString }),
  PreservedKeys,
);
export type AetherMessageEvent = typeof AetherMessageEvent.Type;
export const AETHER_MESSAGE_EVENT_KINDS = [
  "pr_comment",
  "ci_failure",
  "auto_rebase",
  "pr_review",
  "external",
  "github_batch",
  "parent_message",
  "task_signal_batch",
  "child_turn_result",
  "local_parent_message",
  "task_stopped",
] as const;

export const AETHER_DELIVERY_STATUSES = ["queued", "processing", "processed", "bundled"] as const;
export type AetherDeliveryStatus = (typeof AETHER_DELIVERY_STATUSES)[number];

export const AetherUserMessage = Schema.Struct({
  id: NonEmptyString,
  role: Schema.Literal("user"),
  variant: Schema.optionalKey(Schema.Literal("text")),
  content: Schema.String,
  /** Open enum; known values: `AETHER_DELIVERY_STATUSES`. */
  deliveryStatus: Schema.String,
  timestamp: NonEmptyString,
  sequence: SafeNonNegativeInt,
  clientMessageId: Schema.optionalKey(Schema.String),
  processingStartedAt: Schema.optionalKey(NonEmptyString),
  sender_display_name: Schema.optionalKey(NonEmptyString),
  sender_user_id: Schema.optionalKey(Schema.String),
  context: Schema.optionalKey(AetherConversationContext),
  toolResponse: Schema.optionalKey(AetherToolResponse),
  messageEvent: Schema.optionalKey(AetherMessageEvent),
});
export type AetherUserMessage = typeof AetherUserMessage.Type;

const assistantFields = {
  id: NonEmptyString,
  role: Schema.Literal("assistant"),
  timestamp: NonEmptyString,
  sequence: SafeNonNegativeInt,
} as const;

export const AetherTextMessage = Schema.Struct({
  ...assistantFields,
  variant: Schema.Literal("text"),
  content: Schema.String,
});
export type AetherTextMessage = typeof AetherTextMessage.Type;

export const AetherThinkingMessage = Schema.Struct({
  ...assistantFields,
  variant: Schema.Literal("thinking"),
  content: Schema.String,
  isStreaming: Schema.Boolean,
  duration: Schema.optionalKey(Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0))),
});
export type AetherThinkingMessage = typeof AetherThinkingMessage.Type;

export const AETHER_TOOL_STATUSES = [
  "input-streaming",
  "input-available",
  "output-available",
  "output-error",
  "output-denied",
  "approval-responded",
  "approval-requested",
] as const;
export type AetherToolStatus = (typeof AETHER_TOOL_STATUSES)[number];

export const AETHER_CANONICAL_ITEM_TYPES = [
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
] as const;
export type AetherCanonicalItemType = (typeof AETHER_CANONICAL_ITEM_TYPES)[number];

export const AetherToolDisplay = Schema.Struct({
  label: NonEmptyString,
  primary: Schema.optionalKey(NonEmptyString),
  secondary: Schema.optionalKey(NonEmptyString),
});

export const AetherTimelineTool = Schema.Struct({
  id: NonEmptyString,
  name: NonEmptyString,
  input: Schema.Record(Schema.String, Schema.Unknown),
  /** Open enum; known values: `AETHER_TOOL_STATUSES`. One unknown row must not fail the page. */
  status: Schema.String,
  display: AetherToolDisplay,
  /** Open enum; known values: `AETHER_CANONICAL_ITEM_TYPES`. */
  itemType: Schema.optionalKey(Schema.String),
  result: Schema.optionalKey(Schema.String),
  error: Schema.optionalKey(Schema.String),
  // Top-level on tool rows (not inside `result`).
  exitCode: Schema.optionalKey(
    Schema.Int.check(
      Schema.isGreaterThanOrEqualTo(-2147483648),
      Schema.isLessThanOrEqualTo(2147483647),
    ),
  ),
  durationMs: Schema.optionalKey(SafeNonNegativeInt),
});
export type AetherTimelineTool = typeof AetherTimelineTool.Type;

export const AetherToolMessage = Schema.Struct({
  ...assistantFields,
  variant: Schema.Literal("tool"),
  tool: AetherTimelineTool,
});
export type AetherToolMessage = typeof AetherToolMessage.Type;

export const AetherTimelineSeam = Schema.Union([
  Schema.Struct({ reason: Schema.Literal("teleport"), sessionId: NonEmptyString }),
  Schema.Struct({
    reason: Schema.Literal("compaction"),
    sessionId: NonEmptyString,
    boundaryId: NonEmptyString,
    /** Open enum; known values: `manual`, `auto`. */
    trigger: Schema.String,
    preTokens: SafeNonNegativeInt,
    postTokens: Schema.optionalKey(SafeNonNegativeInt),
  }),
]);
export type AetherTimelineSeam = typeof AetherTimelineSeam.Type;

export const AetherSeamMessage = Schema.Struct({
  ...assistantFields,
  variant: Schema.Literal("seam"),
  seam: AetherTimelineSeam,
});
export type AetherSeamMessage = typeof AetherSeamMessage.Type;

/** One row of a turn's activity: user, assistant text, thinking, tool, or seam. */
export const AetherTimelineMessage = Schema.Union([
  AetherUserMessage,
  AetherTextMessage,
  AetherThinkingMessage,
  AetherToolMessage,
  AetherSeamMessage,
]);
export type AetherTimelineMessage = typeof AetherTimelineMessage.Type;

export const AetherConversationTurnState = Schema.Literals([
  "queued",
  "running",
  "awaiting_input",
  "settled",
  "errored",
]);
export type AetherConversationTurnState = typeof AetherConversationTurnState.Type;

export const AetherConversationTurn = Schema.Struct({
  id: NonEmptyString,
  root_message: AetherUserMessage,
  members: Schema.Array(AetherUserMessage),
  latest_agent_message: Schema.NullOr(AetherTextMessage),
  state: AetherConversationTurnState,
  activity: Schema.Struct({
    has_activity: Schema.Boolean,
    item_count: SafeNonNegativeInt,
    failed_tool_count: SafeNonNegativeInt,
    duration_ms: SafeNonNegativeInt,
    has_result_media: Schema.Boolean,
  }),
  /** Moves whenever anything in the turn changes; re-read its activity when it does. */
  revision_sequence: SafeNonNegativeInt,
});
export type AetherConversationTurn = typeof AetherConversationTurn.Type;

/** `GET /tasks/{taskID}/conversation/turns`. */
export const AetherConversationTurnsPage = Schema.Struct({
  task: AetherTask,
  head_revision: SafeNonNegativeInt,
  turns: Schema.Array(AetherConversationTurn),
  active_conversation_turn_id: Schema.NullOr(NonEmptyString),
  processing_started_at: Schema.NullOr(Schema.String),
  oldest_sequence_loaded: Schema.NullOr(SafeNonNegativeInt),
  has_more_older: Schema.Boolean,
});
export type AetherConversationTurnsPage = typeof AetherConversationTurnsPage.Type;

/** `GET /tasks/{taskID}/conversation/turns/{turnID}/activity`. */
export const AetherTurnActivityPage = Schema.Struct({
  task: AetherTask,
  items: Schema.Array(AetherTimelineMessage),
  active_conversation_turn_id: Schema.NullOr(NonEmptyString),
  turn_revision_sequence: SafeNonNegativeInt,
  oldest_sequence_loaded: Schema.NullOr(SafeNonNegativeInt),
  has_more_older: Schema.Boolean,
});
export type AetherTurnActivityPage = typeof AetherTurnActivityPage.Type;

/** An open (still streaming) text or thinking message with its full accumulated content. */
export const AetherLiveMessage = Schema.Struct({
  id: Schema.String,
  processingAttemptId: NonEmptyString,
  conversationTurnId: NonEmptyString,
  variant: Schema.Literals(["text", "thinking"]),
  content: Schema.String,
  sequence: PositiveInt.check(Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)),
  createdAt: NonEmptyString,
});
export type AetherLiveMessage = typeof AetherLiveMessage.Type;

/** `GET /tasks/{taskID}/conversation/live`. */
export const AetherLiveConversationSnapshot = Schema.Struct({
  taskId: NonEmptyString,
  revision: SafeNonNegativeInt,
  headRevision: SafeNonNegativeInt,
  throughSequence: SafeNonNegativeInt,
  /** Moves when the conversation was truncated; cached rows past it are gone. */
  resetSequence: SafeNonNegativeInt,
  open: Schema.Array(AetherLiveMessage),
  replacements: Schema.Array(AetherLiveMessage),
  activeExecution: Schema.optionalKey(
    Schema.Struct({
      conversationTurnId: NonEmptyString,
      processingMessageId: Schema.String,
      processingAttemptId: NonEmptyString,
      workspaceRunId: NonEmptyString,
    }),
  ),
});
export type AetherLiveConversationSnapshot = typeof AetherLiveConversationSnapshot.Type;

// ---------------------------------------------------------------------------
// Subagents (read only)
// ---------------------------------------------------------------------------

export const AetherTaskSubagent = Schema.Struct({
  native_task_id: NonEmptyString,
  provider: Schema.String,
  tool_call_id: NonEmptyString,
  conversation_turn_id: NonEmptyString,
  task_type: Schema.String,
  description: NonEmptyString,
  is_backgrounded: Schema.Boolean,
  ambient: Schema.Boolean,
  /** Open enum; known values: `AETHER_SUBAGENT_STATUSES`. */
  status: Schema.String,
  started_at: NonEmptyString,
  updated_at: NonEmptyString,
  ended_at: Schema.optionalKey(NonEmptyString),
  error: Schema.optionalKey(NonEmptyString),
  subagent_type: Schema.optionalKey(NonEmptyString),
  summary: Schema.optionalKey(NonEmptyString),
  spawn_depth: Schema.optionalKey(PositiveInt),
  usage: Schema.optionalKey(
    Schema.Struct({
      total_tokens: NonNegativeInt,
      tool_uses: NonNegativeInt,
      duration_ms: NonNegativeInt,
    }),
  ),
});
export type AetherTaskSubagent = typeof AetherTaskSubagent.Type;
export const AETHER_SUBAGENT_STATUSES = [
  "pending",
  "running",
  "paused",
  "completed",
  "failed",
  "killed",
  "stopped",
  "stream_closed",
] as const;

export const AetherTaskSubagents = Schema.Struct({ subagents: Schema.Array(AetherTaskSubagent) });
export type AetherTaskSubagents = typeof AetherTaskSubagents.Type;

// ---------------------------------------------------------------------------
// Workspace connect
// ---------------------------------------------------------------------------

export const AetherWorkspaceTransport = Schema.Struct({
  websocket_path: NonEmptyString,
  /** Authorizes port previews for this workspace; not the socket credential. */
  preview_token: Schema.String.check(Schema.isPattern(/^[a-z0-9]{32}$/u)),
});
export type AetherWorkspaceTransport = typeof AetherWorkspaceTransport.Type;

/** `POST /workspaces/{workspaceID}/connect` 200: either a socket to open, or ask again later. */
export const AetherWorkspaceConnect = Schema.Union([
  Schema.Struct({
    workspace_id: NonEmptyString,
    state: Schema.Literal("running"),
    display_state: Schema.String,
    transport: AetherWorkspaceTransport,
  }),
  Schema.Struct({
    workspace_id: NonEmptyString,
    state: Schema.Literal("connecting"),
    display_state: Schema.String,
    retry_after_ms: PositiveInt,
  }),
]);
export type AetherWorkspaceConnect = typeof AetherWorkspaceConnect.Type;

// ---------------------------------------------------------------------------
// Error bodies
// ---------------------------------------------------------------------------

/** The generic error body. Decoded only for its message; the status decides the error. */
export const AetherErrorBody = Schema.Struct({
  error: NonEmptyString,
  code: Schema.optionalKey(Schema.String),
  errors: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        location: Schema.optionalKey(Schema.String),
        message: Schema.optionalKey(Schema.String),
      }),
    ),
  ),
});
export type AetherErrorBody = typeof AetherErrorBody.Type;

/** 402, and one 403 variant: the workspace runtime was not admitted (plan tier, VM size). */
export const AetherAdmissionErrorBody = Schema.Struct({
  error: NonEmptyString,
  code: Schema.String,
  tier: Schema.String,
  vm_size: Schema.String,
});

export const AETHER_ORG_AUTHORIZATION_CODES = [
  "org_membership_required",
  "org_role_insufficient",
  "org_suspended",
  "account_deleting",
] as const;
export const AetherOrgAuthorizationErrorBody = Schema.Struct({
  error: NonEmptyString,
  code: Schema.Literals(AETHER_ORG_AUTHORIZATION_CODES),
});

export const AetherTaskCreatorDeletedBody = Schema.Struct({
  error: NonEmptyString,
  code: Schema.Literal("task_creator_deleted"),
});
export const AetherStalePendingInputBody = Schema.Struct({
  error: NonEmptyString,
  code: Schema.Literal("stale_pending_input"),
  current_tool_id: Schema.optionalKey(NonEmptyString),
});
export const AetherIdempotencyKeyReusedBody = Schema.Struct({
  error: NonEmptyString,
  code: Schema.Literal("idempotency_key_reused"),
});
export const AetherQueuedStopRequiresDiscardBody = Schema.Struct({
  error: NonEmptyString,
  code: Schema.Literal("queued_stop_requires_discard"),
});
/**
 * A respond conflict naming the kind of input the task is waiting for (a response that does
 * not fit the pending input). The OpenAPI's respond 409 union omits this body, and its
 * `ErrorResponse` forbids the extra key; the earlier Aether client (the AetherDriver) received
 * and handled it, so it is decoded whenever `awaiting_input_kind` is present. Unconfirmed
 * against the current server.
 */
export const AetherAwaitingInputConflictBody = Schema.Struct({
  error: NonEmptyString,
  code: Schema.optionalKey(Schema.String),
  awaiting_input_kind: Schema.Literals(AETHER_AWAITING_INPUT_KINDS),
});

export const AetherConnectConflictBody = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("transitional"),
    error: NonEmptyString,
    workspace_id: NonEmptyString,
    retry_after_ms: PositiveInt,
  }),
  Schema.Struct({
    kind: Schema.Literal("startable"),
    error: NonEmptyString,
    workspace_id: NonEmptyString,
  }),
  Schema.Struct({
    kind: Schema.Literal("not_connectable"),
    error: NonEmptyString,
    workspace_id: NonEmptyString,
    display_state: Schema.String,
  }),
]);

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

export const AetherPromptContext = Schema.Struct({
  attachments: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({ filename: NonEmptyString, mediaType: NonEmptyString, data: NonEmptyString }),
    ).check(Schema.isMaxLength(10)),
  ),
  files: Schema.optionalKey(Schema.Array(AetherFileReference)),
});
export type AetherPromptContext = typeof AetherPromptContext.Type;

/** `POST /tasks`. Omitted `auto_fix_*` / `post_pr_*` inherit the project default. */
export const AetherCreateTaskRequest = Schema.Struct({
  project_id: Uuid,
  prompt: NonEmptyString,
  agent_type: AetherAgentType,
  model: NonEmptyString,
  interaction_mode: AetherInteractionMode,
  base_branch: Schema.optionalKey(NonEmptyString),
  context: Schema.optionalKey(AetherPromptContext),
  /**
   * Client-generated UUID. A retry with the same key and an equivalent request returns the
   * original task instead of creating a second one; a different request with the key is 409
   * `idempotency_key_reused`.
   */
  idempotency_key: Schema.optionalKey(Uuid),
  reasoning_effort: Schema.optionalKey(Schema.NullOr(AetherReasoningEffort)),
  vm_size: Schema.optionalKey(AetherVmSize),
  auto_fix_ci: Schema.optionalKey(Schema.Boolean),
  auto_fix_pr_comments: Schema.optionalKey(Schema.Boolean),
  auto_rebase: Schema.optionalKey(Schema.Boolean),
  post_pr_screenshots: Schema.optionalKey(Schema.Boolean),
  post_pr_demo_video: Schema.optionalKey(Schema.Boolean),
});
export type AetherCreateTaskRequest = typeof AetherCreateTaskRequest.Type;

/** `PUT /tasks/{taskID}`: a full replace. `reasoning_effort` is required and may be null. */
export const AetherUpdateTaskRequest = Schema.Struct({
  agent_type: AetherAgentType,
  model: NonEmptyString,
  interaction_mode: AetherInteractionMode,
  reasoning_effort: Schema.NullOr(AetherReasoningEffort),
  auto_fix_ci: Schema.Boolean,
  auto_fix_pr_comments: Schema.Boolean,
  auto_rebase: Schema.Boolean,
  post_pr_screenshots: Schema.optionalKey(Schema.Boolean),
  post_pr_demo_video: Schema.optionalKey(Schema.Boolean),
});
export type AetherUpdateTaskRequest = typeof AetherUpdateTaskRequest.Type;

/**
 * `POST /tasks/{taskID}/respond`. A respond to a running task steers it; `queue_for_later`
 * queues the message instead. Retrying with the same `client_message_id` resolves to the
 * original message.
 */
export const AetherRespondToTaskRequest = Schema.Struct({
  message: NonEmptyString,
  client_message_id: Schema.optionalKey(Uuid),
  queue_for_later: Schema.optionalKey(Schema.Boolean),
  context: Schema.optionalKey(AetherPromptContext),
  interaction_mode: Schema.optionalKey(AetherInteractionMode),
  reasoning_effort: Schema.optionalKey(AetherReasoningEffort),
  tool_response: Schema.optionalKey(AetherRespondToolResponse),
});
export type AetherRespondToTaskRequest = typeof AetherRespondToTaskRequest.Type;

export const AetherStopTaskRequest = Schema.Struct({ discard_queued_messages: Schema.Boolean });
export type AetherStopTaskRequest = typeof AetherStopTaskRequest.Type;

export const AetherRemoveFromQueueRequest = Schema.Struct({ message_id: NonEmptyString });
export type AetherRemoveFromQueueRequest = typeof AetherRemoveFromQueueRequest.Type;

export const AETHER_PAGE_LIMIT_MAX = 200;

/** `before` / `limit` paging for turns and turn activity. */
export const AetherPageQuery = Schema.Struct({
  before: Schema.optionalKey(SafeNonNegativeInt),
  limit: Schema.optionalKey(
    Schema.Int.check(
      Schema.isGreaterThanOrEqualTo(1),
      Schema.isLessThanOrEqualTo(AETHER_PAGE_LIMIT_MAX),
    ),
  ),
});
export type AetherPageQuery = typeof AetherPageQuery.Type;

/** At most 200 previously observed open message ids, sent as one JSON array. */
export const AetherLiveMessageIds = Schema.Array(Schema.String).check(
  Schema.isMaxLength(AETHER_PAGE_LIMIT_MAX),
);
export const AETHER_LIVE_MESSAGE_IDS_MAX_LENGTH = 32_768;

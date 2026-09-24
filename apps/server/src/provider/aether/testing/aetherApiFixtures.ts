// FILE: aetherApiFixtures.ts
// Purpose: Aether REST payloads shaped exactly like the OpenAPI schemas (every required field,
// including the ones Synara does not read), for the client tests and later adapter tests.
// Layer: Test support.

export const TASK_ID = "6a3e1f0c-2b4d-4e8f-9a1b-3c5d7e9f1a2b";
export const PROJECT_ID = "0d1e2f3a-4b5c-4d6e-8f70-8192a3b4c5d6";
export const TURN_ID = "7b8c9d0e-1f2a-4b3c-8d4e-5f6a7b8c9d0e";
export const WORKSPACE_ID = "c1d2e3f4-a5b6-4c7d-8e9f-0a1b2c3d4e5f";
export const PREVIEW_TOKEN = "0123456789abcdefghijklmnopqrstuv";
const TIMESTAMP = "2026-09-24T10:00:00Z";

type Json = Record<string, unknown>;

export const profileBody = (): Json => ({
  id: "user_01",
  email: "dev@example.com",
  display_name: "Dev",
  onboarding_completed: true,
  created_at: TIMESTAMP,
  updated_at: TIMESTAMP,
});

export const projectBody = (overrides: Json = {}): Json => ({
  id: PROJECT_ID,
  name: "synara",
  description: "The Synara repository",
  repo_url: "https://github.com/Aether-Runtime/synara",
  default_branch: "main",
  installation_id: 42,
  mobile_testing_mode: "off",
  pr_review_agent_profile_id: null,
  pr_review_auto_trigger: "disabled",
  pr_review_comment_trigger_enabled: false,
  pr_review_mode: "review_only",
  pr_review_post_demo_video: false,
  pr_review_post_screenshots: false,
  pr_review_prompt: "Review this pull request.",
  pr_review_respond_to_comments_enabled: false,
  task_defaults: {
    agent_type: "claude-code",
    model: "claude-opus-5-5",
    interaction_mode: "default",
    reasoning_effort: "high",
    auto_fix_ci: false,
    auto_fix_pr_comments: false,
    auto_rebase: false,
    post_pr_screenshots: false,
    post_pr_demo_video: false,
    default_vm_size: null,
  },
  created_at: TIMESTAMP,
  updated_at: TIMESTAMP,
  ...overrides,
});

const pullRequestBody = (): Json => ({
  pull_request_id: "9e8d7c6b-5a4f-4e3d-8c2b-1a0f9e8d7c6b",
  pr_number: 7,
  html_url: "https://github.com/Aether-Runtime/synara/pull/7",
  title: "Add the Aether API client",
  checks_passed: 3,
  checks_failed: 0,
  checks_total: 3,
  commits: 2,
  lines_changed: 120,
  additions: 100,
  deletions: 20,
  ci_status: "passed",
  state: "open",
  branch_name: "aether/p2",
});

export const runContextBody = (): Json => ({ workspace_id: WORKSPACE_ID, started_at: TIMESTAMP });

export type TaskStatusFixture = "queued" | "processing" | "awaiting_input" | "errored";

/** `TaskQueued` … `TaskErrored` (the conversation pages' task, without activity). */
export const taskSummaryBody = (status: TaskStatusFixture, overrides: Json = {}): Json => ({
  id: TASK_ID,
  project_id: PROJECT_ID,
  project: {
    state: "active",
    name: "synara",
    repo_url: "https://github.com/Aether-Runtime/synara",
    installation_id: 42,
  },
  parent_task_id: null,
  user_id: "user_01",
  name: "Add the Aether API client",
  agent_type: "claude-code",
  model: "claude-opus-5-5",
  interaction_mode: "default",
  reasoning_effort: "high",
  auto_fix_ci: false,
  auto_fix_pr_comments: false,
  auto_rebase: false,
  post_pr_screenshots: false,
  post_pr_demo_video: false,
  status,
  display_status: status === "errored" ? "errored" : "running",
  github_status: null,
  pull_requests: [pullRequestBody()],
  primary_pull_request: pullRequestBody(),
  review_subject_pull_request: null,
  pr_review_summary: null,
  campaign: null,
  creator_reauth_required: false,
  usage: { tokens_in: 1200, tokens_out: 340, vm_uptime_seconds: 60 },
  runtime_timing: { state: "active", active_started_at: TIMESTAMP },
  hardware: {
    cpu_kind: "shared",
    cpus: 2,
    memory_mb: 4096,
    volume_size_gb: 20,
    gpu_kind: null,
  },
  source: "cli",
  created_at: TIMESTAMP,
  feed_updated_at: TIMESTAMP,
  base_branch: "main",
  head_branch: "aether/p2",
  latest_message_revision: 17,
  run_context: runContextBody(),
  ...(status === "awaiting_input"
    ? {
        awaiting_input: {
          kind: "questions",
          tool_id: "toolu_01",
          input: {
            questions: [
              {
                question: "Which transport?",
                header: "Transport",
                options: [{ label: "WebSocket", description: "Live", extra_option_key: 1 }],
                multiSelect: false,
                extra_question_key: "kept",
              },
            ],
            extra_input_key: { nested: [1, 2] },
          },
        },
      }
    : {}),
  ...(status === "errored" ? { error: "The agent crashed.", completed_at: TIMESTAMP } : {}),
  ...overrides,
});

/** `Task…WithActivity` (`GET`/`PUT /tasks/{taskID}`). */
export const taskBody = (status: TaskStatusFixture, overrides: Json = {}): Json => ({
  ...taskSummaryBody(status),
  activity_items: [
    {
      id: "act_01",
      task_id: TASK_ID,
      created_at: TIMESTAMP,
      sequence: 3,
      type: "workspace_lifecycle",
      message: "Workspace started.",
      event_id: "evt_01",
    },
  ],
  agent_segments: [],
  runtime_intervals: [],
  latest_workspace_warning: "Disk is 90% full.",
  ...overrides,
});

export const userMessageBody = (overrides: Json = {}): Json => ({
  id: "msg_user_01",
  role: "user",
  variant: "text",
  content: "Build the client.",
  deliveryStatus: "processed",
  timestamp: TIMESTAMP,
  sequence: 1,
  clientMessageId: "4e5f6a7b-8c9d-4e0f-9a1b-2c3d4e5f6a7b",
  sender_display_name: "Dev",
  ...overrides,
});

export const textMessageBody = (overrides: Json = {}): Json => ({
  id: "msg_text_01",
  role: "assistant",
  variant: "text",
  timestamp: TIMESTAMP,
  sequence: 4,
  content: "Done.",
  ...overrides,
});

export const thinkingMessageBody = (): Json => ({
  id: "msg_thinking_01",
  role: "assistant",
  variant: "thinking",
  timestamp: TIMESTAMP,
  sequence: 2,
  content: "Planning the schemas.",
  isStreaming: false,
  duration: 1.5,
});

export const toolMessageBody = (toolOverrides: Json = {}): Json => ({
  id: "msg_tool_01",
  role: "assistant",
  variant: "tool",
  timestamp: TIMESTAMP,
  sequence: 3,
  tool: {
    id: "toolu_02",
    name: "Bash",
    input: { command: "bun run test" },
    status: "output-available",
    display: { label: "Run tests", primary: "bun run test" },
    itemType: "command_execution",
    provider: "claude-code",
    result: "ok",
    exitCode: 0,
    durationMs: 5120,
    ...toolOverrides,
  },
});

export const seamMessageBody = (): Json => ({
  id: "msg_seam_01",
  role: "assistant",
  variant: "seam",
  timestamp: TIMESTAMP,
  sequence: 5,
  seam: {
    reason: "compaction",
    sessionId: "session_01",
    boundaryId: "boundary_01",
    trigger: "auto",
    preTokens: 180_000,
    postTokens: 20_000,
  },
});

export const turnBody = (overrides: Json = {}): Json => ({
  id: TURN_ID,
  root_message: userMessageBody(),
  members: [],
  latest_agent_message: textMessageBody(),
  state: "settled",
  activity: {
    has_activity: true,
    item_count: 4,
    failed_tool_count: 0,
    duration_ms: 9000,
    has_result_media: false,
  },
  revision_sequence: 9,
  ...overrides,
});

export const turnsPageBody = (overrides: Json = {}): Json => ({
  task: taskSummaryBody("processing"),
  head_revision: 17,
  turns: [turnBody()],
  active_conversation_turn_id: TURN_ID,
  processing_started_at: TIMESTAMP,
  oldest_sequence_loaded: 1,
  has_more_older: false,
  ...overrides,
});

export const activityPageBody = (overrides: Json = {}): Json => ({
  task: taskSummaryBody("processing"),
  items: [
    userMessageBody(),
    thinkingMessageBody(),
    toolMessageBody(),
    textMessageBody(),
    seamMessageBody(),
  ],
  active_conversation_turn_id: TURN_ID,
  turn_revision_sequence: 9,
  oldest_sequence_loaded: 1,
  has_more_older: false,
  ...overrides,
});

export const liveMessageBody = (overrides: Json = {}): Json => ({
  id: "msg_live_01",
  processingAttemptId: "5a6b7c8d-9e0f-4a1b-8c2d-3e4f5a6b7c8d",
  conversationTurnId: TURN_ID,
  variant: "text",
  content: "Accumulated so far",
  sequence: 6,
  createdAt: TIMESTAMP,
  ...overrides,
});

export const liveSnapshotBody = (overrides: Json = {}): Json => ({
  taskId: TASK_ID,
  revision: 18,
  headRevision: 17,
  throughSequence: 6,
  resetSequence: 0,
  open: [liveMessageBody()],
  replacements: [],
  activeExecution: {
    conversationTurnId: TURN_ID,
    processingMessageId: "msg_user_01",
    processingAttemptId: "5a6b7c8d-9e0f-4a1b-8c2d-3e4f5a6b7c8d",
    workspaceRunId: "6b7c8d9e-0f1a-4b2c-8d3e-4f5a6b7c8d9e",
  },
  ...overrides,
});

export const subagentsBody = (): Json => ({
  subagents: [
    {
      native_task_id: "native_01",
      provider: "claude-code",
      tool_call_id: "toolu_03",
      conversation_turn_id: TURN_ID,
      task_type: "local_agent",
      description: "Explore the repository",
      is_backgrounded: false,
      ambient: false,
      status: "completed",
      started_at: TIMESTAMP,
      updated_at: TIMESTAMP,
      ended_at: TIMESTAMP,
      usage: { total_tokens: 900, tool_uses: 4, duration_ms: 3000 },
    },
  ],
});

export const connectRunningBody = (): Json => ({
  workspace_id: WORKSPACE_ID,
  state: "running",
  display_state: "ready",
  last_started_at: TIMESTAMP,
  transport: { websocket_path: `/workspaces/${WORKSPACE_ID}/ws`, preview_token: PREVIEW_TOKEN },
});

export const connectConnectingBody = (): Json => ({
  workspace_id: WORKSPACE_ID,
  state: "connecting",
  display_state: "starting",
  retry_after_ms: 1500,
  operation: {
    id: "op_01",
    type: "start",
    status: "running",
    created_at: TIMESTAMP,
    updated_at: TIMESTAMP,
  },
});

export const errorBody = (error: string, code?: string): Json => ({
  error,
  ...(code === undefined ? {} : { code }),
});

export const admissionBody = (): Json => ({
  error: "Your plan does not include large workspaces.",
  code: "vm_size_not_allowed",
  tier: "free",
  vm_size: "large",
});

export const orgBody = (code = "org_membership_required"): Json => ({
  error: "You are not a member of this organization.",
  code,
});

/** A deep copy without the key at `path` (e.g. `["task", "status"]`). */
export function withoutKey(body: Json, path: ReadonlyArray<string | number>): Json {
  const copy = structuredClone(body);
  let node: unknown = copy;
  for (const key of path.slice(0, -1)) node = (node as Record<string | number, unknown>)[key];
  delete (node as Record<string | number, unknown>)[path[path.length - 1] as string | number];
  return copy;
}

// FILE: aetherApiFixtures.ts
// Purpose: Aether REST payloads with the fields the client decodes, for client, socket and
// adapter tests. A tolerance test adds unknown keys to all of them.
// Layer: Test support.

import {
  AetherMessageId,
  AetherProjectId,
  AetherTaskId,
  AetherToolId,
  AetherTurnId,
  AetherWorkspaceId,
} from "../aetherApiSchemas.ts";

export const TASK_ID = AetherTaskId.makeUnsafe("6a3e1f0c-2b4d-4e8f-9a1b-3c5d7e9f1a2b");
export const PROJECT_ID = AetherProjectId.makeUnsafe("0d1e2f3a-4b5c-4d6e-8f70-8192a3b4c5d6");
export const TURN_ID = AetherTurnId.makeUnsafe("7b8c9d0e-1f2a-4b3c-8d4e-5f6a7b8c9d0e");
export const WORKSPACE_ID = AetherWorkspaceId.makeUnsafe("c1d2e3f4-a5b6-4c7d-8e9f-0a1b2c3d4e5f");
export const MESSAGE_ID = AetherMessageId.makeUnsafe("msg_user_01");
export const TOOL_ID = AetherToolId.makeUnsafe("toolu_01");

type Json = Record<string, unknown>;

export const profileBody = (): Json => ({ email: "dev@example.com" });

export const projectBody = (): Json => ({
  id: PROJECT_ID,
  repo_url: "https://github.com/Aether-Runtime/synara",
});

export type TaskStatusFixture = "queued" | "processing" | "awaiting_input" | "errored";

export const taskBody = (status: TaskStatusFixture, extra: Json = {}): Json => ({
  status,
  run_context: { workspace_id: WORKSPACE_ID },
  ...(status === "awaiting_input"
    ? {
        awaiting_input: {
          kind: "questions",
          tool_id: TOOL_ID,
          input: { questions: [{ question: "Which transport?", options: [] }], extra: [1, 2] },
        },
      }
    : {}),
  ...(status === "errored" ? { error: "The agent crashed." } : {}),
  ...extra,
});

export const userMessageBody = (extra: Json = {}): Json => ({
  id: MESSAGE_ID,
  role: "user",
  content: "Build the client.",
  clientMessageId: "4e5f6a7b-8c9d-4e0f-9a1b-2c3d4e5f6a7b",
  ...extra,
});

export const toolRowBody = (tool: Json = {}): Json => ({
  id: "msg_tool_01",
  role: "assistant",
  variant: "tool",
  tool: {
    id: "toolu_02",
    name: "Bash",
    input: { command: "bun run test" },
    status: "output-available",
    display: { label: "Run tests" },
    itemType: "command_execution",
    result: "ok",
    exitCode: 0,
    durationMs: 5120,
    ...tool,
  },
});

export const turnBody = (extra: Json = {}): Json => ({
  id: TURN_ID,
  root_message: userMessageBody(),
  members: [],
  state: "settled",
  revision_sequence: 9,
  ...extra,
});

const page = { has_more_older: false, oldest_sequence_loaded: 1 };

export const turnsPageBody = (extra: Json = {}): Json => ({
  task: taskBody("processing"),
  head_revision: 17,
  turns: [turnBody()],
  ...page,
  ...extra,
});

export const activityPageBody = (extra: Json = {}): Json => ({
  items: [
    userMessageBody(),
    { id: "msg_thinking_01", role: "assistant", variant: "thinking", content: "Planning." },
    toolRowBody(),
    { id: "msg_text_01", role: "assistant", variant: "text", content: "Done." },
    { id: "msg_seam_01", role: "assistant", variant: "seam", seam: { reason: "compaction" } },
  ],
  turn_revision_sequence: 9,
  ...page,
  ...extra,
});

export const liveSnapshotBody = (extra: Json = {}): Json => ({
  headRevision: 17,
  resetSequence: 0,
  open: [{ id: "msg_live_01", conversationTurnId: TURN_ID, variant: "text", content: "So far" }],
  replacements: [],
  ...extra,
});

export const connectRunningBody = (): Json => ({
  state: "running",
  transport: { websocket_path: `/workspaces/${WORKSPACE_ID}/ws` },
});

/** A deep copy without the key at `path` (e.g. `["turns", 0, "state"]`). */
export function withoutKey(body: Json, path: ReadonlyArray<string | number>): Json {
  const copy = structuredClone(body);
  let node: unknown = copy;
  for (const key of path.slice(0, -1)) node = (node as Record<string | number, unknown>)[key];
  delete (node as Record<string | number, unknown>)[path[path.length - 1] as string | number];
  return copy;
}

import { ProviderRuntimeEvent, ThreadId, TurnId } from "@synara/contracts";
import { Result, Schema } from "effect";
import { describe, expect, it } from "vitest";

import {
  AetherConversationTurnsPage,
  AetherLiveConversationSnapshot,
  AetherTurnActivityPage,
  AetherTurnId,
} from "./aetherApiSchemas.ts";
import {
  AetherConversationIngest,
  AetherTranscriptDivergedError,
  type AetherIngestOutput,
  type AetherIngestSeed,
} from "./aetherConversationIngest.ts";
import { TASK_ID, TURN_ID, taskBody, userMessageBody } from "./testing/aetherApiFixtures.ts";

type Json = Record<string, unknown>;

const AT = "2026-09-25T10:00:00.000Z";
const OTHER_TURN_ID = AetherTurnId.makeUnsafe("9c8b7a6f-5e4d-4c3b-8a29-18f7e6d5c4b3");

function makeIngest(seed?: AetherIngestSeed) {
  return new AetherConversationIngest(
    {
      taskId: TASK_ID,
      threadId: ThreadId.makeUnsafe("thread-1"),
      turnIdFor: (turnId) => TurnId.makeUnsafe(`synara-${turnId}`),
    },
    seed,
  );
}

const liveBody = (extra: Json = {}): Json => ({
  headRevision: 10,
  resetSequence: 0,
  open: [],
  replacements: [],
  ...extra,
});
const message = (id: string, content: string, variant = "text", turnId: string = TURN_ID) => ({
  id,
  conversationTurnId: turnId,
  variant,
  content,
});
const turn = (extra: Json = {}): Json => ({
  id: TURN_ID,
  root_message: userMessageBody({ id: "root-1" }),
  members: [],
  state: "running",
  revision_sequence: 1,
  ...extra,
});
const turnsBody = (task: Json, turns: ReadonlyArray<Json>, head = 10): Json => ({
  task,
  head_revision: head,
  turns,
  has_more_older: false,
  oldest_sequence_loaded: 1,
});
const toolRow = (id: string, tool: Json = {}): Json => ({
  id: `row-${id}`,
  role: "assistant",
  variant: "tool",
  tool: {
    id,
    name: "Bash",
    input: { command: "bun run test" },
    status: "input-available",
    display: { label: "Run command", primary: "bun run test" },
    itemType: "command_execution",
    ...tool,
  },
});
const textRow = (id: string, content: string, variant = "text"): Json => ({
  id,
  role: "assistant",
  variant,
  content,
});
const activityBody = (items: ReadonlyArray<Json>, revision = 1, extra: Json = {}): Json => ({
  items,
  turn_revision_sequence: revision,
  has_more_older: false,
  oldest_sequence_loaded: null,
  ...extra,
});
const processingTask = (extra: Json = {}) => taskBody("processing", extra);
const idleTask = () =>
  taskBody("processing", { status: "awaiting_input", awaiting_input: { kind: "message" } });

function live(ingest: AetherConversationIngest, body: Json) {
  const step = ingest.applyLive(Schema.decodeUnknownSync(AetherLiveConversationSnapshot)(body), AT);
  if (Result.isFailure(step)) throw step.failure;
  return step.success;
}

/** One refresh: the turns page, the activity of every turn that needs it, then the finish. */
function refresh(
  ingest: AetherConversationIngest,
  page: Json,
  activity: Readonly<Record<string, Json>> = {},
): AetherIngestOutput[] {
  const outputs = [
    ...ingest.applyTurns(Schema.decodeUnknownSync(AetherConversationTurnsPage)(page)),
  ];
  for (const turnId of ingest.turnsNeedingActivity()) {
    const body = activity[turnId];
    if (body === undefined) continue;
    const decoded = Schema.decodeUnknownSync(AetherTurnActivityPage)(body);
    const applied = ingest.applyActivity(turnId, decoded.items, decoded.turn_revision_sequence, AT);
    if (Result.isFailure(applied)) throw applied.failure;
    outputs.push(...applied.success);
  }
  outputs.push(...ingest.finishRefresh(AT));
  return outputs;
}

const events = (outputs: ReadonlyArray<AetherIngestOutput>) =>
  outputs.flatMap((output) => (output._tag === "Event" ? [output.event] : []));
const signals = (outputs: ReadonlyArray<AetherIngestOutput>) =>
  outputs.filter((output) => output._tag !== "Event");

function expectValidEvents(outputs: ReadonlyArray<AetherIngestOutput>) {
  for (const event of events(outputs)) {
    const encoded = Schema.encodeUnknownSync(ProviderRuntimeEvent)(event);
    expect(Schema.decodeUnknownSync(ProviderRuntimeEvent)(encoded)).toEqual(event);
  }
}

describe("AetherConversationIngest: text", () => {
  it("streams only the new suffix of accumulated content, then completes on close", () => {
    const ingest = makeIngest();
    const first = live(ingest, liveBody({ open: [message("m1", "Hel")] })).outputs;
    const second = live(ingest, liveBody({ open: [message("m1", "Hello")] })).outputs;
    const repeat = live(ingest, liveBody({ open: [message("m1", "Hello")] })).outputs;
    const closed = live(ingest, liveBody({ replacements: [message("m1", "Hello!")] })).outputs;

    expect(events(first).map((event) => [event.type, event.payload])).toEqual([
      ["content.delta", { streamKind: "assistant_text", delta: "Hel" }],
    ]);
    expect(events(second).map((event) => event.payload)).toEqual([
      { streamKind: "assistant_text", delta: "lo" },
    ]);
    expect(repeat).toEqual([]);
    expect(events(closed).map((event) => [event.type, event.payload])).toEqual([
      ["content.delta", { streamKind: "assistant_text", delta: "!" }],
      ["item.completed", { itemType: "assistant_message", status: "completed", detail: "Hello!" }],
    ]);
    expect(events([...first, ...second, ...closed]).map((event) => event.eventId)).toEqual([
      `aether:${TASK_ID}:${TURN_ID}:m1:delta:3`,
      `aether:${TASK_ID}:${TURN_ID}:m1:delta:5`,
      `aether:${TASK_ID}:${TURN_ID}:m1:delta:6`,
      `aether:${TASK_ID}:${TURN_ID}:m1:completed`,
    ]);
    expect(ingest.openMessageIds()).toEqual([]);
    expectValidEvents(closed);
  });

  it("completes a message first seen closed without streaming it", () => {
    const ingest = makeIngest();
    const outputs = live(ingest, liveBody({ replacements: [message("m1", "All done.")] }));
    expect(events(outputs.outputs).map((event) => [event.type, event.itemId])).toEqual([
      ["item.completed", "m1"],
    ]);
  });

  it("does not repeat a closed message the activity reads again", () => {
    const ingest = makeIngest();
    live(ingest, liveBody({ open: [message("m1", "Hi")] }));
    live(ingest, liveBody({ replacements: [message("m1", "Hi.")] }));
    const outputs = refresh(ingest, turnsBody(processingTask(), [turn()]), {
      [TURN_ID]: activityBody([textRow("m1", "Hi.")]),
    });
    expect(events(outputs)).toEqual([]);
  });

  it("leaves a message the live endpoint still streams to the live endpoint", () => {
    const ingest = makeIngest();
    live(ingest, liveBody({ open: [message("m1", "Hi")] }));
    const outputs = refresh(ingest, turnsBody(processingTask(), [turn()]), {
      [TURN_ID]: activityBody([textRow("m1", "Hi")]),
    });
    expect(events(outputs)).toEqual([]);
  });

  it("streams thinking as reasoning and drops an empty thinking block", () => {
    const ingest = makeIngest();
    const outputs = live(
      ingest,
      liveBody({
        replacements: [message("t-empty", "", "thinking")],
        open: [message("t1", "Weighing options", "thinking")],
      }),
    ).outputs;
    expect(events(outputs).map((event) => [event.itemId, event.payload])).toEqual([
      ["t1", { streamKind: "reasoning_text", delta: "Weighing options" }],
    ]);
    const closed = live(
      ingest,
      liveBody({ replacements: [message("t1", "Weighing options", "thinking")] }),
    );
    expect(events(closed.outputs).map((event) => event.payload)).toEqual([
      { itemType: "reasoning", status: "completed", detail: "Weighing options" },
    ]);
  });

  it("fails with a typed error when accumulated text stops extending what was streamed", () => {
    const ingest = makeIngest();
    live(ingest, liveBody({ open: [message("m1", "Hello")] }));
    const step = ingest.applyLive(
      Schema.decodeUnknownSync(AetherLiveConversationSnapshot)(
        liveBody({ open: [message("m1", "Help")] }),
      ),
      AT,
    );
    expect(Result.isFailure(step)).toBe(true);
    if (Result.isSuccess(step)) return;
    expect(step.failure).toBeInstanceOf(AetherTranscriptDivergedError);
    expect(step.failure).toMatchObject({
      messageId: "m1",
      source: "live",
      streamedLength: 5,
      receivedLength: 4,
    });
  });

  it("fails when a completed message comes back different", () => {
    const ingest = makeIngest();
    live(ingest, liveBody({ replacements: [message("m1", "Final.")] }));
    ingest.applyTurns(
      Schema.decodeUnknownSync(AetherConversationTurnsPage)(turnsBody(processingTask(), [turn()])),
    );
    const applied = ingest.applyActivity(
      TURN_ID,
      Schema.decodeUnknownSync(AetherTurnActivityPage)(activityBody([textRow("m1", "Final!")]))
        .items,
      1,
      AT,
    );
    expect(Result.isFailure(applied) && applied.failure.source).toBe("activity");
  });

  it("resumes a stream after the length Synara already holds", () => {
    const ingest = makeIngest({ streamedLengthByMessageId: new Map([["m1", 3]]) });
    const outputs = live(ingest, liveBody({ open: [message("m1", "Hello")] })).outputs;
    expect(events(outputs).map((event) => [event.eventId, event.payload])).toEqual([
      [`aether:${TASK_ID}:${TURN_ID}:m1:delta:5`, { streamKind: "assistant_text", delta: "lo" }],
    ]);
  });
});

describe("AetherConversationIngest: tool rows", () => {
  function applyRows(
    ingest: AetherConversationIngest,
    rows: ReadonlyArray<Json>,
    revision: number,
  ) {
    return refresh(ingest, turnsBody(processingTask(), [turn({ revision_sequence: revision })]), {
      [TURN_ID]: activityBody(rows, revision),
    });
  }

  it("starts, updates and completes a tool row once per change", () => {
    const ingest = makeIngest();
    const started = applyRows(
      ingest,
      [toolRow("t1", { input: {}, display: { label: "Run command" } })],
      1,
    );
    const unchanged = applyRows(
      ingest,
      [toolRow("t1", { input: {}, display: { label: "Run command" } })],
      2,
    );
    const updated = applyRows(ingest, [toolRow("t1")], 3);
    const completed = applyRows(
      ingest,
      [toolRow("t1", { status: "output-available", result: "ok", exitCode: 0, durationMs: 5120 })],
      4,
    );
    const afterwards = applyRows(
      ingest,
      [toolRow("t1", { status: "output-available", result: "ok (again)", exitCode: 0 })],
      5,
    );

    expect(events(started).map((event) => [event.type, event.payload])).toEqual([
      [
        "item.started",
        {
          itemType: "command_execution",
          status: "inProgress",
          title: "Run command",
          data: {
            toolCallId: "t1",
            callId: "t1",
            toolName: "Bash",
            input: {},
            aetherStatus: "input-available",
          },
        },
      ],
    ]);
    expect(events(unchanged)).toEqual([]);
    expect(events(updated).map((event) => [event.type, event.payload])).toEqual([
      ["item.updated", expect.objectContaining({ detail: "bun run test", status: "inProgress" })],
    ]);
    expect(events(updated)[0]?.eventId).toMatch(
      new RegExp(`^aether:${TASK_ID}:${TURN_ID}:row-t1:input-available:[0-9a-f]{16}$`),
    );
    expect(events(completed).map((event) => [event.type, event.eventId, event.payload])).toEqual([
      [
        "item.completed",
        `aether:${TASK_ID}:${TURN_ID}:row-t1:output-available`,
        expect.objectContaining({
          status: "completed",
          data: expect.objectContaining({ result: "ok", exitCode: 0, durationMs: 5120 }),
        }),
      ],
    ]);
    expect(events(afterwards)).toEqual([]);
    expectValidEvents([...started, ...updated, ...completed]);
  });

  it("completes a row first seen finished without starting it", () => {
    const ingest = makeIngest();
    const outputs = applyRows(ingest, [toolRow("t1", { status: "output-available" })], 1);
    expect(events(outputs).map((event) => event.type)).toEqual(["item.completed"]);
  });

  it("maps failed and denied calls, with the error as detail when there is no subject", () => {
    const ingest = makeIngest();
    const outputs = applyRows(
      ingest,
      [
        toolRow("t1", {
          status: "output-error",
          error: "exit 1",
          display: { label: "Run command" },
        }),
        toolRow("t2", { status: "output-denied" }),
      ],
      1,
    );
    expect(
      events(outputs).map((event) =>
        event.type === "item.completed" ? [event.payload.status, event.payload.detail] : [],
      ),
    ).toEqual([
      ["failed", "exit 1"],
      ["declined", "bun run test"],
    ]);
  });

  it("maps Aether item types onto Synara's canonical ones", () => {
    const ingest = makeIngest();
    const bare = toolRow("bare");
    delete (bare.tool as Json).itemType;
    const outputs = applyRows(
      ingest,
      [
        toolRow("read", { itemType: "file_read" }),
        toolRow("fetch", { itemType: "web_fetch" }),
        toolRow("agent", { itemType: "subagent_invocation" }),
        toolRow("edit", { itemType: "file_change" }),
        toolRow("mcp", { itemType: "mcp_tool_call" }),
        bare,
      ],
      1,
    );
    expect(
      events(outputs).map((event) =>
        event.type === "item.started" ? [event.itemId, event.payload.itemType] : [],
      ),
    ).toEqual([
      ["row-read", "dynamic_tool_call"],
      ["row-fetch", "dynamic_tool_call"],
      ["row-agent", "collab_agent_tool_call"],
      ["row-edit", "file_change"],
      ["row-mcp", "mcp_tool_call"],
      ["row-bare", "dynamic_tool_call"],
    ]);
  });

  it("shows an unknown tool type as a generic row carrying the raw type, and warns once", () => {
    const ingest = makeIngest();
    const outputs = applyRows(
      ingest,
      [
        toolRow("t1", { itemType: "hologram_render" }),
        toolRow("t2", { itemType: "hologram_render" }),
      ],
      1,
    );
    expect(events(outputs).map((event) => [event.type, event.payload])).toEqual([
      [
        "runtime.warning",
        {
          message:
            "Aether ran a tool type Synara does not support yet (hologram_render); it is shown as a generic row.",
          detail: { toolName: "Bash", itemType: "hologram_render" },
        },
      ],
      [
        "item.started",
        expect.objectContaining({
          itemType: "unknown",
          data: expect.objectContaining({ aetherItemType: "hologram_render" }),
        }),
      ],
      ["item.started", expect.objectContaining({ itemType: "unknown" })],
    ]);
    expectValidEvents(outputs);
  });

  it("warns about an unknown tool status and keeps the row in progress", () => {
    const ingest = makeIngest();
    const outputs = applyRows(ingest, [toolRow("t1", { status: "teleporting" })], 1);
    expect(events(outputs).map((event) => [event.type, event.payload])).toEqual([
      [
        "runtime.warning",
        expect.objectContaining({
          message:
            "Aether reported a tool status Synara does not recognize (teleporting); the row stays in progress.",
        }),
      ],
      ["item.started", expect.objectContaining({ status: "inProgress" })],
    ]);
  });

  it("turns task-tracking calls into task list updates", () => {
    const ingest = makeIngest();
    const todos = applyRows(
      ingest,
      [
        toolRow("todo", {
          name: "TodoWrite",
          itemType: "task_tracking",
          input: {
            todos: [
              { content: "Find the button", activeForm: "Finding the button", status: "completed" },
              {
                content: "Change the label",
                activeForm: "Changing the label",
                status: "in_progress",
              },
            ],
          },
        }),
      ],
      1,
    );
    const plan = applyRows(
      ingest,
      [
        toolRow("plan", {
          name: "update_plan",
          itemType: "task_tracking",
          input: { explanation: "Two steps.", plan: [{ step: "Edit", status: "pending" }] },
        }),
      ],
      2,
    );
    expect(events(todos).map((event) => [event.type, event.payload])).toEqual([
      [
        "turn.tasks.updated",
        {
          tasks: [
            { task: "Find the button", status: "completed" },
            { task: "Changing the label", status: "inProgress" },
          ],
        },
      ],
    ]);
    expect(events(plan).map((event) => event.payload)).toEqual([
      { explanation: "Two steps.", tasks: [{ task: "Edit", status: "pending" }] },
    ]);
    expectValidEvents([...todos, ...plan]);
  });

  it("waits for a task list's input, and warns only when a finished call is unreadable", () => {
    const ingest = makeIngest();
    const streaming = applyRows(
      ingest,
      [toolRow("todo", { itemType: "task_tracking", input: {} })],
      1,
    );
    const unreadable = applyRows(
      ingest,
      [
        toolRow("todo", {
          itemType: "task_tracking",
          status: "output-available",
          input: { steps: 2 },
        }),
      ],
      2,
    );
    expect(events(streaming)).toEqual([]);
    expect(events(unreadable).map((event) => [event.type, event.payload])).toEqual([
      [
        "runtime.warning",
        {
          message: "Aether updated a task list that Synara cannot read.",
          detail: { toolName: "Bash", input: { steps: 2 } },
        },
      ],
    ]);
  });

  it("maps compaction seams to a row and other seams to a visible warning", () => {
    const ingest = makeIngest();
    const outputs = applyRows(
      ingest,
      [
        { id: "seam-1", role: "assistant", variant: "seam", seam: { reason: "compaction" } },
        { id: "seam-2", role: "assistant", variant: "seam", seam: { reason: "teleport" } },
        { id: "seam-3", role: "assistant", variant: "seam", seam: { reason: "rewind" } },
      ],
      1,
    );
    expect(events(outputs).map((event) => [event.type, event.payload])).toEqual([
      [
        "item.completed",
        { itemType: "context_compaction", status: "completed", title: "Context compacted" },
      ],
      [
        "runtime.warning",
        {
          message: "Aether marked a conversation seam (teleport).",
          detail: { reason: "teleport" },
        },
      ],
      [
        "runtime.warning",
        {
          message: "Aether marked a conversation seam Synara does not recognize (rewind).",
          detail: { reason: "rewind" },
        },
      ],
    ]);
  });
});

describe("AetherConversationIngest: settlement", () => {
  it("does not settle a turn whose activity has not been read at its revision", () => {
    const ingest = makeIngest();
    const outputs = refresh(ingest, turnsBody(idleTask(), [turn({ state: "settled" })]));
    expect(signals(outputs).map((output) => output._tag)).toEqual(["TurnOrigin"]);
  });

  it("settles once the task is idle, and only once", () => {
    const ingest = makeIngest();
    const page = turnsBody(idleTask(), [turn({ state: "settled" })]);
    const first = refresh(ingest, page, { [TURN_ID]: activityBody([]) });
    const again = refresh(ingest, page, { [TURN_ID]: activityBody([]) });
    expect(signals(first)).toEqual([
      {
        _tag: "TurnOrigin",
        turnId: TURN_ID,
        rootMessageId: "root-1",
        clientMessageId: expect.any(String),
      },
      { _tag: "TurnSettled", turnId: TURN_ID, runMessageId: "root-1", state: "settled" },
    ]);
    expect(signals(again)).toEqual([]);
  });

  it("waits while a settled turn's task is queued again for that turn", () => {
    const ingest = makeIngest();
    const outputs = refresh(ingest, turnsBody(taskBody("queued"), [turn({ state: "settled" })]), {
      [TURN_ID]: activityBody([]),
    });
    expect(signals(outputs).some((output) => output._tag === "TurnSettled")).toBe(false);
  });

  it("settles a turn when the task's queued work belongs to the next turn", () => {
    const ingest = makeIngest();
    const outputs = refresh(
      ingest,
      turnsBody(taskBody("queued"), [
        turn({ state: "settled" }),
        turn({
          id: OTHER_TURN_ID,
          root_message: userMessageBody({ id: "root-2" }),
          state: "queued",
        }),
      ]),
      { [TURN_ID]: activityBody([]), [OTHER_TURN_ID]: activityBody([]) },
    );
    expect(signals(outputs).filter((output) => output._tag === "TurnSettled")).toEqual([
      { _tag: "TurnSettled", turnId: TURN_ID, runMessageId: "root-1", state: "settled" },
    ]);
  });

  it("carries the task's error on an errored settlement", () => {
    const ingest = makeIngest();
    const outputs = refresh(ingest, turnsBody(taskBody("errored"), [turn({ state: "errored" })]), {
      [TURN_ID]: activityBody([]),
    });
    expect(signals(outputs).filter((output) => output._tag === "TurnSettled")).toEqual([
      {
        _tag: "TurnSettled",
        turnId: TURN_ID,
        runMessageId: "root-1",
        state: "errored",
        errorMessage: "The agent crashed.",
      },
    ]);
  });

  it("settles a proposed plan, then settles again once the approval's run finishes", () => {
    const ingest = makeIngest();
    const planTask = taskBody("processing", {
      status: "awaiting_input",
      awaiting_input: { kind: "plan", tool_id: "toolu_plan", input: { plan: "1. Edit\n2. Test" } },
    });
    const proposed = refresh(
      ingest,
      turnsBody(planTask, [turn({ state: "awaiting_input", revision_sequence: 1 })]),
      {
        [TURN_ID]: activityBody(
          [toolRow("toolu_plan", { name: "propose_plan", status: "approval-requested" })],
          1,
        ),
      },
    );
    const approval = {
      ...userMessageBody({ id: "approval-1" }),
      toolResponse: { tool_name: "propose_plan", data: { approved: true } },
    };
    const resumed = refresh(
      ingest,
      turnsBody(processingTask(), [turn({ state: "running", revision_sequence: 2 })]),
      {
        [TURN_ID]: activityBody([approval], 2),
      },
    );
    const finished = refresh(
      ingest,
      turnsBody(idleTask(), [turn({ state: "settled", revision_sequence: 3 })]),
      {
        [TURN_ID]: activityBody([approval], 3),
      },
    );

    expect(
      proposed.map((output) => (output._tag === "Event" ? output.event.type : output._tag)),
    ).toEqual(["TurnOrigin", "PendingInput", "turn.proposed.completed", "TurnSettled"]);
    expect(events(proposed)[0]).toMatchObject({
      requestId: "toolu_plan",
      payload: { planMarkdown: "1. Edit\n2. Test" },
    });
    expect(signals(proposed).at(-1)).toEqual({
      _tag: "TurnSettled",
      turnId: TURN_ID,
      runMessageId: "root-1",
      state: "awaiting_input",
      kind: "plan",
    });
    expect(signals(resumed)).toEqual([
      { _tag: "PendingInputResolved", turnId: TURN_ID, toolId: "toolu_plan", kind: "plan" },
    ]);
    expect(signals(finished)).toEqual([
      { _tag: "TurnSettled", turnId: TURN_ID, runMessageId: "approval-1", state: "settled" },
    ]);
    expectValidEvents(proposed);
  });

  it("warns when a question cannot be shown, and still hands over the pending input", () => {
    const ingest = makeIngest();
    const outputs = refresh(
      ingest,
      turnsBody(taskBody("awaiting_input"), [turn({ state: "awaiting_input" })]),
      {
        [TURN_ID]: activityBody([]),
      },
    );
    expect(
      outputs.map((output) => (output._tag === "Event" ? output.event.type : output._tag)),
    ).toEqual(["TurnOrigin", "PendingInput", "runtime.warning"]);
    expect(events(outputs)[0]?.payload).toMatchObject({
      message: "Aether is waiting on a question that Synara cannot display.",
    });
  });

  it("resolves a question answered without a readable answer with no answers", () => {
    const ingest = makeIngest();
    const question = {
      kind: "questions",
      tool_id: "toolu_q",
      input: {
        questions: [
          { header: "Pick", question: "Which?", options: [{ label: "A", description: "The A." }] },
        ],
      },
    };
    refresh(
      ingest,
      turnsBody(taskBody("processing", { status: "awaiting_input", awaiting_input: question }), [
        turn({ state: "awaiting_input" }),
      ]),
      { [TURN_ID]: activityBody([]) },
    );
    const resolved = refresh(
      ingest,
      turnsBody(processingTask(), [turn({ revision_sequence: 2 })]),
      {
        [TURN_ID]: activityBody([], 2),
      },
    );
    expect(events(resolved).map((event) => [event.type, event.payload])).toEqual([
      ["user-input.resolved", { answers: {} }],
    ]);
  });
});

describe("AetherConversationIngest: polling state", () => {
  it("asks for the turns page on the first tick, when the head moves, and until it is read", () => {
    const ingest = makeIngest();
    expect(live(ingest, liveBody({ headRevision: 5 })).refreshTurns).toBe(true);
    expect(live(ingest, liveBody({ headRevision: 5 })).refreshTurns).toBe(true);
    ingest.applyTurns(
      Schema.decodeUnknownSync(AetherConversationTurnsPage)(turnsBody(processingTask(), [])),
    );
    expect(live(ingest, liveBody({ headRevision: 5 })).refreshTurns).toBe(false);
    expect(live(ingest, liveBody({ headRevision: 6 })).refreshTurns).toBe(true);
  });

  it("counts activity read after a turns page as current, even when its revision trails", () => {
    const ingest = makeIngest();
    const outputs = refresh(
      ingest,
      turnsBody(idleTask(), [turn({ state: "settled", revision_sequence: 5 })]),
      { [TURN_ID]: activityBody([], 4) },
    );
    expect(ingest.turnsNeedingActivity()).toEqual([]);
    expect(signals(outputs).filter((output) => output._tag === "TurnSettled")).toHaveLength(1);
  });

  it("re-reads everything after the conversation is truncated", () => {
    const ingest = makeIngest();
    live(ingest, liveBody());
    refresh(ingest, turnsBody(processingTask(), [turn()]), { [TURN_ID]: activityBody([]) });
    expect(ingest.turnsNeedingActivity()).toEqual([]);

    const step = live(ingest, liveBody({ resetSequence: 4 }));
    expect(step.refreshTurns).toBe(true);
    expect(ingest.turnsNeedingActivity()).toEqual([TURN_ID]);
    expect(
      step.outputs.map((output) => (output._tag === "Event" ? output.event.type : output)),
    ).toEqual([{ _tag: "ConversationReset", resetSequence: 4 }, "runtime.warning"]);
  });

  it("keeps polling while work is visible and stops once the task is idle", () => {
    const ingest = makeIngest();
    expect(ingest.shouldKeepPolling()).toBe(true);
    live(
      ingest,
      liveBody({ activeExecution: { conversationTurnId: TURN_ID, processingMessageId: "root-1" } }),
    );
    refresh(ingest, turnsBody(idleTask(), [turn({ state: "settled" })]), {
      [TURN_ID]: activityBody([]),
    });
    expect(ingest.shouldKeepPolling()).toBe(true);
    live(ingest, liveBody());
    expect(ingest.shouldKeepPolling()).toBe(false);
    refresh(ingest, turnsBody(taskBody("awaiting_input"), [turn({ state: "awaiting_input" })]));
    expect(ingest.shouldKeepPolling()).toBe(true);
    refresh(ingest, turnsBody(taskBody("errored"), [turn({ state: "errored" })]));
    expect(ingest.shouldKeepPolling()).toBe(false);
  });

  it("pages back through a turn it never read, and only as far as needed afterwards", () => {
    const ingest = makeIngest();
    ingest.applyTurns(
      Schema.decodeUnknownSync(AetherConversationTurnsPage)(turnsBody(processingTask(), [turn()])),
    );
    const page = (items: ReadonlyArray<Json>) =>
      Schema.decodeUnknownSync(AetherTurnActivityPage)(
        activityBody(items, 1, { has_more_older: true, oldest_sequence_loaded: 40 }),
      );
    const newest = page([toolRow("t2")]);
    expect(ingest.needsOlderActivity(TURN_ID, newest, new Set(["row-t2"]))).toBe(true);

    const applied = ingest.applyActivity(
      TURN_ID,
      [...page([toolRow("t1")]).items, ...newest.items],
      1,
      AT,
    );
    expect(Result.isSuccess(applied)).toBe(true);
    // Both rows are known but both still run: the older one must be read again.
    expect(ingest.needsOlderActivity(TURN_ID, newest, new Set(["row-t2"]))).toBe(true);
    expect(ingest.needsOlderActivity(TURN_ID, newest, new Set(["row-t2", "row-t1"]))).toBe(false);
    // A page that starts with a row never seen means more new rows lie behind it.
    expect(ingest.needsOlderActivity(TURN_ID, page([toolRow("t3")]), new Set(["row-t3"]))).toBe(
      true,
    );
  });
});

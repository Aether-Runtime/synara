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
  AetherPendingInputError,
  AetherTranscriptDivergedError,
  type AetherIngestOutput,
  type AetherIngestResume,
  type AetherRunKey,
} from "./aetherConversationIngest.ts";
import {
  TASK_ID,
  TOOL_ID,
  TURN_ID,
  taskBody,
  userMessageBody,
} from "./testing/aetherApiFixtures.ts";

type Json = Record<string, unknown>;

const AT = "2026-09-25T10:00:00.000Z";
const OTHER_TURN_ID = AetherTurnId.makeUnsafe("9c8b7a6f-5e4d-4c3b-8a29-18f7e6d5c4b3");

function makeIngest(resume: Partial<AetherIngestResume> = {}) {
  return new AetherConversationIngest(
    {
      taskId: TASK_ID,
      threadId: ThreadId.makeUnsafe("thread-1"),
      turnIdFor: (turnId) => TurnId.makeUnsafe(`synara-${turnId}`),
    },
    { after: null, emittedText: new Map(), ...resume },
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
const askRow = (toolId: string) =>
  toolRow(toolId, { name: "ask_user", status: "input-available", itemType: "dynamic_tool_call" });
const answerRow = (id: string, data: unknown): Json => ({
  ...userMessageBody({ id }),
  toolResponse: { tool_name: "ask_user", data },
});
const processingTask = (extra: Json = {}) => taskBody("processing", extra);
const idleTask = () =>
  taskBody("processing", { status: "awaiting_input", awaiting_input: { kind: "message" } });
const askingTask = (toolId: string) =>
  taskBody("processing", {
    status: "awaiting_input",
    awaiting_input: {
      kind: "questions",
      tool_id: toolId,
      input: {
        questions: [
          {
            header: "Pick",
            question: "Which one?",
            options: [
              { label: "A", description: "The A." },
              { label: "B", description: "The B." },
            ],
          },
        ],
      },
    },
  });

function live(ingest: AetherConversationIngest, body: Json) {
  const step = ingest.applyLive(Schema.decodeUnknownSync(AetherLiveConversationSnapshot)(body), AT);
  if (Result.isFailure(step)) throw step.failure;
  return step.success;
}

function finish(ingest: AetherConversationIngest) {
  const finished = ingest.finishRefresh(AT);
  if (Result.isFailure(finished)) throw finished.failure;
  return finished.success;
}

/** One refresh: the turns page, the activity of every turn that needs it, then the finish. */
function refreshResult(
  ingest: AetherConversationIngest,
  page: Json,
  activity: Readonly<Record<string, Json>> = {},
): Result.Result<AetherIngestOutput[], AetherPendingInputError> {
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
  const finished = ingest.finishRefresh(AT);
  if (Result.isFailure(finished)) return Result.fail(finished.failure);
  return Result.succeed([...outputs, ...finished.success]);
}

function refresh(
  ingest: AetherConversationIngest,
  page: Json,
  activity: Readonly<Record<string, Json>> = {},
): AetherIngestOutput[] {
  const refreshed = refreshResult(ingest, page, activity);
  if (Result.isFailure(refreshed)) throw refreshed.failure;
  return refreshed.success;
}

function refreshFailure(
  ingest: AetherConversationIngest,
  page: Json,
  activity: Readonly<Record<string, Json>>,
): AetherPendingInputError {
  const refreshed = refreshResult(ingest, page, activity);
  if (Result.isSuccess(refreshed)) throw new Error("the refresh succeeded");
  return refreshed.failure;
}

const events = (outputs: ReadonlyArray<AetherIngestOutput>) =>
  outputs.flatMap((output) => (output._tag === "Event" ? [output.event] : []));
const signals = (outputs: ReadonlyArray<AetherIngestOutput>) =>
  outputs.filter((output) => output._tag !== "Event");
const settles = (outputs: ReadonlyArray<AetherIngestOutput>) =>
  outputs.filter((output) => output._tag === "TurnSettled");
const tags = (outputs: ReadonlyArray<AetherIngestOutput>) =>
  outputs.map((output) => (output._tag === "Event" ? output.event.type : output._tag));

function expectValidEvents(outputs: ReadonlyArray<AetherIngestOutput>) {
  for (const event of events(outputs)) {
    const encoded = Schema.encodeUnknownSync(ProviderRuntimeEvent)(event);
    expect(Schema.decodeUnknownSync(ProviderRuntimeEvent)(encoded)).toEqual(event);
  }
}

describe("AetherConversationIngest: text", () => {
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

  it("continues text across a restart from what Synara holds: still open, or closed while down", () => {
    // Before the restart Synara streamed "Hello wor" of m1 and "Hel" of m2.
    const before = live(
      makeIngest(),
      liveBody({ open: [message("m1", "Hello wor"), message("m2", "Hel")] }),
    ).outputs;
    // After it, m1 is still open and m2 closed while Synara was down (only activity shows it).
    const ingest = makeIngest({
      emittedText: new Map([
        ["m1", 9],
        ["m2", 3],
      ]),
    });
    const after = [
      ...live(ingest, liveBody({ open: [message("m1", "Hello world")] })).outputs,
      ...refresh(ingest, turnsBody(processingTask(), [turn()]), {
        [TURN_ID]: activityBody([textRow("m2", "Hello there."), textRow("m1", "Hello world")]),
      }),
    ];

    expect(events(after).map((event) => [event.eventId, event.payload])).toEqual([
      [`aether:${TASK_ID}:${TURN_ID}:m1:delta:11`, { streamKind: "assistant_text", delta: "ld" }],
      [
        `aether:${TASK_ID}:${TURN_ID}:m2:delta:12`,
        { streamKind: "assistant_text", delta: "lo there." },
      ],
      [
        `aether:${TASK_ID}:${TURN_ID}:m2:completed`,
        { itemType: "assistant_message", status: "completed", detail: "Hello there." },
      ],
    ]);
    // Nothing repeats and nothing is missing.
    const text = (messageId: string) =>
      events([...before, ...after])
        .flatMap((event) =>
          event.type === "content.delta" && event.itemId === messageId ? [event.payload.delta] : [],
        )
        .join("");
    expect([text("m1"), text("m2")]).toEqual(["Hello world", "Hello there."]);
  });

  it("fails loudly when Synara holds more of a message than the server text has", () => {
    const ingest = makeIngest({ emittedText: new Map([["m1", 20]]) });
    const step = ingest.applyLive(
      Schema.decodeUnknownSync(AetherLiveConversationSnapshot)(
        liveBody({ open: [message("m1", "Hello world")] }),
      ),
      AT,
    );
    expect(Result.isFailure(step) && step.failure).toMatchObject({
      _tag: "AetherTranscriptDivergedError",
      messageId: "m1",
      source: "resume",
      streamedLength: 20,
      receivedLength: 11,
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

  it("turns Claude's task-tracking calls into task list updates", () => {
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
    expectValidEvents(todos);
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

  it("hands over the turn's origin, and carries the task's error on an errored settlement", () => {
    const ingest = makeIngest();
    const outputs = refresh(ingest, turnsBody(taskBody("errored"), [turn({ state: "errored" })]), {
      [TURN_ID]: activityBody([]),
    });
    expect(signals(outputs)).toEqual([
      {
        _tag: "TurnOrigin",
        turnId: TURN_ID,
        rootMessageId: "root-1",
        rootMessageText: "Build the client.",
        clientMessageId: "4e5f6a7b-8c9d-4e0f-9a1b-2c3d4e5f6a7b",
      },
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

  it("does not settle a turn while one of its messages is still open", () => {
    const ingest = makeIngest();
    live(ingest, liveBody({ open: [message("m1", "Done")] }));
    const early = refresh(ingest, turnsBody(idleTask(), [turn({ state: "settled" })]), {
      [TURN_ID]: activityBody([textRow("m1", "Done")]),
    });
    expect(settles(early)).toEqual([]);
    expect(ingest.shouldKeepPolling()).toBe(true);
    // The reply closes on the next tick: its last text comes first, then the settle.
    const closed = live(ingest, liveBody({ replacements: [message("m1", "Done.")] })).outputs;
    expect(tags([...closed, ...finish(ingest)])).toEqual([
      "content.delta",
      "item.completed",
      "TurnSettled",
    ]);
  });

  it("ignores a turns page older than the live head, and reads the turns again", () => {
    const ingest = makeIngest();
    const page = (head: number) => turnsBody(idleTask(), [turn({ state: "settled" })], head);
    expect(live(ingest, liveBody({ headRevision: 12 })).refreshTurns).toBe(true);
    expect(refresh(ingest, page(11), { [TURN_ID]: activityBody([]) })).toEqual([]);
    expect(live(ingest, liveBody({ headRevision: 12 })).refreshTurns).toBe(true);
    expect(tags(refresh(ingest, page(12), { [TURN_ID]: activityBody([]) }))).toEqual([
      "TurnOrigin",
      "TurnSettled",
    ]);
  });

  it("reads a turn again while its activity trails the turns page, and settles only after", () => {
    const ingest = makeIngest();
    const page = turnsBody(idleTask(), [turn({ state: "settled", revision_sequence: 5 })]);
    expect(settles(refresh(ingest, page, { [TURN_ID]: activityBody([], 4) }))).toEqual([]);
    expect(ingest.turnsNeedingActivity()).toEqual([TURN_ID]);
    const caughtUp = refresh(ingest, page, {
      [TURN_ID]: activityBody([textRow("m1", "Done.")], 5),
    });
    expect(tags(caughtUp)).toEqual(["item.completed", "TurnSettled"]);
  });

  it("signals only the runs after the resume watermark, by page order, not revision", () => {
    // As on real tasks: turns are listed oldest first, and revisions are not monotonic across them.
    const first = turn({ state: "settled", revision_sequence: 7 });
    const second = turn({
      id: OTHER_TURN_ID,
      root_message: userMessageBody({ id: "root-2" }),
      state: "settled",
      revision_sequence: 2,
    });
    const history = turnsBody(idleTask(), [first, second]);
    const activity = { [TURN_ID]: activityBody([], 7), [OTHER_TURN_ID]: activityBody([], 2) };
    const settledRuns = (after: AetherRunKey | null = null) =>
      settles(refresh(makeIngest({ after }), history, activity)).map((settle) =>
        settle._tag === "TurnSettled" ? settle.runMessageId : undefined,
      );

    expect(settledRuns()).toEqual(["root-1", "root-2"]);
    expect(settledRuns({ turnId: TURN_ID, runMessageId: "root-1" })).toEqual(["root-2"]);
    expect(settledRuns({ turnId: OTHER_TURN_ID, runMessageId: "root-2" })).toEqual([]);
    // A watermark whose turn is gone (truncated past it) covers every turn on the page.
    const gone = AetherTurnId.makeUnsafe("0f1e2d3c-4b5a-4968-8776-a5b4c3d2e1f0");
    expect(settledRuns({ turnId: gone, runMessageId: "root-9" })).toEqual([]);

    // A new run in the watermark's turn (an approved plan) still settles.
    const ingest = makeIngest({ after: { turnId: OTHER_TURN_ID, runMessageId: "root-2" } });
    refresh(ingest, history, activity);
    expect(ingest.shouldKeepPolling()).toBe(false);
    const approved = refresh(
      ingest,
      turnsBody(idleTask(), [first, { ...second, revision_sequence: 3 }]),
      { [OTHER_TURN_ID]: activityBody([userMessageBody({ id: "approval-2" })], 3) },
    );
    expect(settles(approved)).toEqual([
      { _tag: "TurnSettled", turnId: OTHER_TURN_ID, runMessageId: "approval-2", state: "settled" },
    ]);
  });
});

describe("AetherConversationIngest: pending input", () => {
  const waitingTurn = (revision = 1) =>
    turn({ state: "awaiting_input", revision_sequence: revision });

  it("warns when a question cannot be shown, and still hands over the pending input", () => {
    const ingest = makeIngest();
    const outputs = refresh(
      ingest,
      turnsBody(taskBody("awaiting_input"), [turn({ state: "awaiting_input" })]),
      { [TURN_ID]: activityBody([askRow(TOOL_ID)]) },
    );
    expect(tags(outputs)).toEqual(["TurnOrigin", "PendingInput", "runtime.warning"]);
    expect(events(outputs)[0]?.payload).toMatchObject({
      message: "Aether is waiting on a question that Synara cannot display.",
      detail: { toolId: TOOL_ID, error: expect.stringContaining("header") },
    });
  });

  it("resolves each question with its own answer, and never reuses one", () => {
    const ingest = makeIngest();
    const first = refresh(ingest, turnsBody(askingTask("q1"), [waitingTurn()]), {
      [TURN_ID]: activityBody([askRow("q1")]),
    });
    const answered = [askRow("q1"), answerRow("a1", { answers: { "0": [1] } }), askRow("q2")];
    const second = refresh(ingest, turnsBody(askingTask("q2"), [waitingTurn(2)]), {
      [TURN_ID]: activityBody(answered, 2),
    });
    // The task stops waiting on the second question with no answer row (it was stopped).
    const stopped = refresh(
      ingest,
      turnsBody(idleTask(), [turn({ state: "settled", revision_sequence: 3 })]),
      { [TURN_ID]: activityBody(answered, 3) },
    );

    expect(tags(first)).toEqual(["TurnOrigin", "PendingInput", "user-input.requested"]);
    expect(tags(second)).toEqual([
      "user-input.resolved",
      "PendingInputResolved",
      "PendingInput",
      "user-input.requested",
    ]);
    expect(events(second)[0]).toMatchObject({
      requestId: "q1",
      payload: { answers: { "0": "B" } },
    });
    expect(tags(stopped)).toEqual(["runtime.warning", "PendingInputResolved", "TurnSettled"]);
    expect(events(stopped)[0]?.payload).toEqual({
      message: "Aether stopped waiting on a question without an answer.",
      detail: { toolId: "q2" },
    });
    expectValidEvents([...first, ...second, ...stopped]);
  });

  it('resolves a custom ("Other") answer, selected with option -1', () => {
    const ingest = makeIngest();
    refresh(ingest, turnsBody(askingTask("q1"), [waitingTurn()]), {
      [TURN_ID]: activityBody([askRow("q1")]),
    });
    const answer = answerRow("a1", { answers: { "0": [-1] }, customAnswers: { "0": " C " } });
    const resolved = refresh(
      ingest,
      turnsBody(processingTask(), [turn({ revision_sequence: 2 })]),
      {
        [TURN_ID]: activityBody([askRow("q1"), answer], 2),
      },
    );
    expect(events(resolved).map((event) => event.payload)).toEqual([{ answers: { "0": "C" } }]);
  });

  it.each([
    [{ answers: { "0": [7] } }, "question 0 has no option 7"],
    [{ answers: { "0": [-1] } }, "question 0 selects a custom answer but has none"],
    [{ answers: { "5": [0] } }, "the answer names question 5"],
    [{ answers: { "0": [0, 1] } }, "question 0 takes one answer and got 2"],
    [{ choices: [1] }, `["answers"]`],
  ])("fails loudly on an answer it cannot read: %j", (data, detail) => {
    const ingest = makeIngest();
    refresh(ingest, turnsBody(askingTask("q1"), [waitingTurn()]), {
      [TURN_ID]: activityBody([askRow("q1")]),
    });
    const failure = refreshFailure(
      ingest,
      turnsBody(processingTask(), [turn({ revision_sequence: 2 })]),
      {
        [TURN_ID]: activityBody([askRow("q1"), answerRow("a1", data)], 2),
      },
    );
    expect(failure).toBeInstanceOf(AetherPendingInputError);
    expect(failure.message).toContain(detail);
  });

  it("surfaces a question once the turn that asked it is read, and fails if none did", () => {
    const ingest = makeIngest();
    const page = turnsBody(askingTask("q1"), [waitingTurn(2)]);
    expect(tags(refresh(ingest, page, { [TURN_ID]: activityBody([], 1) }))).toEqual(["TurnOrigin"]);
    expect(refreshFailure(ingest, page, { [TURN_ID]: activityBody([], 2) })).toMatchObject({
      _tag: "AetherPendingInputError",
      toolId: "q1",
    });
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

  it("re-reads everything after the conversation is truncated", () => {
    const ingest = makeIngest();
    live(ingest, liveBody());
    refresh(ingest, turnsBody(processingTask(), [turn()]), { [TURN_ID]: activityBody([]) });
    expect(ingest.turnsNeedingActivity()).toEqual([]);

    const step = live(ingest, liveBody({ resetSequence: 4 }));
    expect(step.refreshTurns).toBe(true);
    expect(ingest.turnsNeedingActivity()).toEqual([TURN_ID]);
    expect(step.outputs).toEqual([{ _tag: "ConversationReset", resetSequence: 4 }]);
  });

  it("keeps polling until every turn has ended and been signalled, even if the task is idle", () => {
    const ingest = makeIngest();
    expect(ingest.shouldKeepPolling()).toBe(true);
    live(
      ingest,
      liveBody({ activeExecution: { conversationTurnId: TURN_ID, processingMessageId: "root-1" } }),
    );
    refresh(ingest, turnsBody(idleTask(), [turn({ state: "running" })]), {
      [TURN_ID]: activityBody([]),
    });
    expect(ingest.shouldKeepPolling()).toBe(true);
    // Nothing executes and the task reads idle, but the turn has not ended yet.
    live(ingest, liveBody());
    expect(ingest.shouldKeepPolling()).toBe(true);
    refresh(ingest, turnsBody(idleTask(), [turn({ state: "settled", revision_sequence: 2 })]), {
      [TURN_ID]: activityBody([], 2),
    });
    expect(ingest.shouldKeepPolling()).toBe(false);
  });

  it("idles once a question or plan waiting on the user is surfaced, not before", () => {
    const waiting = turn({ state: "awaiting_input", revision_sequence: 2 });
    const question = makeIngest();
    live(question, liveBody());
    refresh(question, turnsBody(askingTask("q1"), [waiting]), { [TURN_ID]: activityBody([], 1) });
    // No turn has been seen making the call yet: keep reading.
    expect(question.shouldKeepPolling()).toBe(true);
    refresh(question, turnsBody(askingTask("q1"), [waiting]), {
      [TURN_ID]: activityBody([toolRow("q1", { name: "ask_user" })], 2),
    });
    expect(question.shouldKeepPolling()).toBe(false);

    const plan = makeIngest();
    live(plan, liveBody());
    const planTask = taskBody("processing", {
      status: "awaiting_input",
      awaiting_input: { kind: "plan", tool_id: "toolu_plan", input: { plan: "1. Edit" } },
    });
    const proposed = refresh(plan, turnsBody(planTask, [waiting]), {
      [TURN_ID]: activityBody([toolRow("toolu_plan", { name: "propose_plan" })], 2),
    });
    expect(tags(proposed).at(-1)).toBe("TurnSettled");
    expect(plan.shouldKeepPolling()).toBe(false);
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
    expect(ingest.needsOlderActivity(TURN_ID, page([toolRow("t4")]), new Set(["row-t4"]))).toBe(
      true,
    );
  });
});

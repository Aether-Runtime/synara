import { ThreadId, TurnId } from "@synara/contracts";
import { Clock, Effect, Schema } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vitest";

import {
  AetherApiDecodeError,
  AetherApiHttpError,
  AetherApiTransportError,
  type AetherApiError,
} from "./aetherApiErrors.ts";
import {
  AetherConversationTurnsPage,
  AetherLiveConversationSnapshot,
  AetherTurnActivityPage,
} from "./aetherApiSchemas.ts";
import {
  AETHER_ACTIVITY_MAX_PAGES,
  AETHER_POLL_AWAITING_INTERVAL_MS,
  AETHER_POLL_INTERVAL_MS,
  AETHER_POLL_OUTAGE_LIMIT_MS,
  AETHER_POLL_PULL_REQUEST_INTERVAL_MS,
  AETHER_POLL_WAKE_GRACE_MS,
  AETHER_TURNS_PAGE_LIMIT,
  makeAetherConversationPoller,
  type AetherConversationPoller,
  type AetherConversationReader,
  type AetherPollerOutput,
} from "./aetherConversationPoller.ts";
import { aetherThreadPullRequest } from "./aetherPullRequest.ts";
import {
  pullRequestBody,
  TASK_ID,
  TURN_ID,
  taskBody,
  userMessageBody,
} from "./testing/aetherApiFixtures.ts";

type Json = Record<string, unknown>;

interface Call {
  readonly at: number;
  readonly kind: "live" | "turns" | "activity";
  readonly detail?: unknown;
}

interface Script {
  /** `at` is the test clock when the read is made. */
  readonly live: (call: number, at: number) => Json | AetherApiError;
  readonly turns?: (call: number, at: number) => Json | AetherApiError;
  readonly activity?: (call: number, before: number | undefined) => Json | AetherApiError;
}

const liveBody = (extra: Json = {}): Json => ({
  headRevision: 1,
  resetSequence: 0,
  open: [],
  replacements: [],
  ...extra,
});
const executing = { conversationTurnId: TURN_ID, processingMessageId: "root-1" };
const turnsBody = (task: Json, state = "running", revision = 1): Json => ({
  task,
  head_revision: revision,
  turns: [
    {
      id: TURN_ID,
      root_message: userMessageBody({ id: "root-1" }),
      members: [],
      state,
      revision_sequence: revision,
    },
  ],
  has_more_older: false,
  oldest_sequence_loaded: 1,
});
const idleTask = () =>
  taskBody("processing", { status: "awaiting_input", awaiting_input: { kind: "message" } });
const activityBody = (items: ReadonlyArray<Json> = [], extra: Json = {}): Json => ({
  items,
  turn_revision_sequence: 1,
  has_more_older: false,
  oldest_sequence_loaded: null,
  ...extra,
});
const toolRow = (id: string): Json => ({
  id,
  role: "assistant",
  variant: "tool",
  tool: {
    id,
    name: "Bash",
    input: {},
    status: "output-available",
    display: { label: "Run command" },
    itemType: "command_execution",
  },
});

function scriptedReader(script: Script, calls: Call[]): AetherConversationReader {
  const counts = { live: 0, turns: 0, activity: 0 };
  const answer = <A>(
    schema: Schema.Top & { readonly DecodingServices: never },
    reply: Json | AetherApiError,
  ) =>
    reply instanceof Error
      ? Effect.fail(reply as AetherApiError)
      : Effect.succeed(Schema.decodeUnknownSync(schema)(reply) as A);
  return {
    getLiveConversation: (_task, messageIds) =>
      Effect.gen(function* () {
        const at = yield* Clock.currentTimeMillis;
        calls.push({ at, kind: "live", detail: messageIds });
        return yield* answer(AetherLiveConversationSnapshot, script.live(counts.live++, at));
      }),
    getConversationTurns: (_task, page) =>
      Effect.gen(function* () {
        const at = yield* Clock.currentTimeMillis;
        calls.push({ at, kind: "turns", detail: page?.limit });
        const reply = script.turns?.(counts.turns++, at) ?? turnsBody(taskBody("processing"));
        return yield* answer(AetherConversationTurnsPage, reply);
      }),
    getTurnActivity: (_task, _turn, page) =>
      Effect.gen(function* () {
        calls.push({ at: yield* Clock.currentTimeMillis, kind: "activity", detail: page?.before });
        const reply = script.activity?.(counts.activity++, page?.before) ?? activityBody();
        return yield* answer(AetherTurnActivityPage, reply);
      }),
  } as AetherConversationReader;
}

const flush = Effect.promise(() => new Promise<void>((resolve) => setImmediate(resolve)));

/** Wakes the poller, advances the clock `seconds` times by one tick, and returns what it did. */
async function run(
  script: Script,
  options: {
    readonly seconds: number;
    readonly wakeAgainAt?: number;
    /** What the consumer does with an output, as the adapter would. */
    readonly onEmit?: (
      output: AetherPollerOutput,
      poller: AetherConversationPoller,
    ) => Effect.Effect<void>;
  },
) {
  const calls: Call[] = [];
  const outputs: AetherPollerOutput[] = [];
  await Effect.gen(function* () {
    const poller: AetherConversationPoller = yield* makeAetherConversationPoller({
      client: scriptedReader(script, calls),
      context: {
        taskId: TASK_ID,
        threadId: ThreadId.makeUnsafe("thread-1"),
        turnIdFor: (turnId) => TurnId.makeUnsafe(`synara-${turnId}`),
      },
      resume: { after: null, emittedText: new Map() },
      emit: (output) =>
        Effect.suspend(() => {
          outputs.push(output);
          return options.onEmit?.(output, poller) ?? Effect.void;
        }),
    });
    yield* poller.wake;
    for (let second = 0; ; second += 1) {
      for (let round = 0; round < 4; round += 1) yield* flush;
      if (second === options.wakeAgainAt) {
        yield* poller.wake;
        for (let round = 0; round < 4; round += 1) yield* flush;
      }
      if (second === options.seconds) break;
      yield* TestClock.adjust(AETHER_POLL_INTERVAL_MS);
    }
  }).pipe(Effect.scoped, Effect.provide(TestClock.layer()), Effect.runPromise);
  return { calls, outputs };
}

const transportError = () =>
  new AetherApiTransportError({
    endpoint: "GET /tasks/{taskID}/conversation/live",
    cause: new Error("ECONNRESET"),
  });
const liveCallTimes = (calls: ReadonlyArray<Call>) =>
  calls.filter((call) => call.kind === "live").map((call) => call.at);
const warnings = (outputs: ReadonlyArray<AetherPollerOutput>) =>
  outputs.flatMap((output) =>
    output._tag === "Event" && output.event.type === "runtime.warning" ? [output.event] : [],
  );
const stops = (outputs: ReadonlyArray<AetherPollerOutput>) =>
  outputs.flatMap((output) => (output._tag === "PollingStopped" ? [output.error] : []));
/** The conversation's outputs, without the task hand-overs a changed turns read adds. */
const conversation = (outputs: ReadonlyArray<AetherPollerOutput>) =>
  outputs.filter((output) => output._tag !== "TaskUpdated");
const summary = (output: AetherPollerOutput) =>
  output._tag === "Event"
    ? [output.event.type, output.event.itemId].filter(Boolean).join(" ")
    : output._tag === "PollingStopped"
      ? output.error._tag
      : output._tag;

/** The boot phases a task's status line walks through, one every two seconds. */
const statusAt = (at: number) =>
  at < 2_000 ? "Booting workspace" : at < 4_000 ? "Preparing workspace" : "Workspace ready";

describe("makeAetherConversationPoller", () => {
  it("backs off on transport failures with one warning, then stops after the outage limit", async () => {
    const { calls, outputs } = await run({ live: () => transportError() }, { seconds: 200 });
    expect(liveCallTimes(calls)).toEqual([
      0, 1000, 3000, 7000, 15000, 25000, 35000, 45000, 55000, 65000, 75000, 85000, 95000, 105000,
      115000, 125000,
    ]);
    expect(warnings(outputs).map((event) => [event.eventId, event.payload])).toEqual([
      [
        `aether:${TASK_ID}:task:poll-degraded:0`,
        {
          message: "Synara lost contact with Aether and is retrying.",
          detail: transportError().message,
        },
      ],
    ]);
    const [stopped] = stops(outputs);
    expect(stops(outputs)).toHaveLength(1);
    expect(stopped?._tag).toBe("AetherPollOutageError");
    expect(stopped).toMatchObject({ taskId: TASK_ID, outageMs: 125000 });
    expect(125000).toBeGreaterThanOrEqual(AETHER_POLL_OUTAGE_LIMIT_MS);
  });

  it("recovers from a short outage and warns again only for a new one", async () => {
    const failing = new Set([1, 2, 6]);
    const { calls, outputs } = await run(
      {
        live: (call) =>
          failing.has(call) ? transportError() : liveBody({ activeExecution: executing }),
      },
      { seconds: 12 },
    );
    // Backoff restarts at one second after each recovery.
    expect(liveCallTimes(calls)).toEqual([
      0, 1000, 2000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 11000, 12000,
    ]);
    expect(warnings(outputs).map((event) => event.eventId)).toEqual([
      `aether:${TASK_ID}:task:poll-degraded:1000`,
      `aether:${TASK_ID}:task:poll-degraded:7000`,
    ]);
    expect(stops(outputs)).toEqual([]);
  });

  it("stops at once on an error that retrying cannot fix", async () => {
    const notFound = new AetherApiHttpError({
      endpoint: "GET /tasks/{taskID}/conversation/live",
      status: 404,
    });
    const { calls, outputs } = await run({ live: () => notFound }, { seconds: 10 });
    expect(liveCallTimes(calls)).toEqual([0]);
    expect(stops(outputs)).toEqual([notFound]);
    expect(warnings(outputs)).toEqual([]);
  });

  it("stops with a typed error when streamed text diverges, and again on wake", async () => {
    const texts = ["Hello", "Help"];
    const { outputs } = await run(
      {
        live: (call) =>
          liveBody({
            activeExecution: executing,
            open: [
              {
                id: "m1",
                conversationTurnId: TURN_ID,
                variant: "text",
                content: texts[Math.min(call, 1)],
              },
            ],
          }),
      },
      { seconds: 3, wakeAgainAt: 2 },
    );
    // The failed ticks change nothing: the wake reads the same text and fails the same way.
    expect(conversation(outputs).map(summary)).toEqual([
      "content.delta m1",
      "TurnOrigin",
      "AetherTranscriptDivergedError",
      "AetherTranscriptDivergedError",
    ]);
  });

  it("changes nothing on a failed tick, so a wake emits that tick's outputs exactly once", async () => {
    const decodeError = new AetherApiDecodeError({
      endpoint: "GET /tasks/{taskID}/conversation/turns/{turnID}/activity",
      status: 200,
      detail: 'Expected an array at ["items"]',
      cause: null,
    });
    const text = (content: string) => ({
      id: "m1",
      conversationTurnId: TURN_ID,
      variant: "text",
      content,
    });
    const { calls, outputs } = await run(
      {
        // The second tick sees the conversation truncated (resetSequence 1), then its activity
        // read fails to decode. The wake at 2 s reads the same state again.
        live: (call) =>
          call === 0
            ? liveBody({ activeExecution: executing, open: [text("Hel")] })
            : liveBody({
                activeExecution: executing,
                headRevision: 2,
                resetSequence: 1,
                open: [text("Hello")],
              }),
        turns: (call) => turnsBody(taskBody("processing"), "running", call === 0 ? 1 : 2),
        activity: (call) =>
          call === 1
            ? decodeError
            : activityBody([toolRow(call === 0 ? "t0" : "t1")], {
                turn_revision_sequence: call === 0 ? 1 : 2,
              }),
      },
      { seconds: 3, wakeAgainAt: 2 },
    );
    expect(liveCallTimes(calls)).toEqual([0, 1000, 2000, 3000]);
    expect(conversation(outputs).map(summary)).toEqual([
      "content.delta m1",
      "TurnOrigin",
      "item.completed t0",
      "AetherApiDecodeError",
      "ConversationReset",
      "content.delta m1",
      "item.completed t1",
    ]);
  });

  it("after a wake, keeps polling an idle-looking task for the grace period only", async () => {
    const { calls } = await run(
      {
        live: () => liveBody(),
        turns: () => turnsBody(idleTask(), "settled"),
      },
      { seconds: 60 },
    );
    const times = liveCallTimes(calls);
    expect(times[0]).toBe(0);
    expect(times.at(-1)).toBe(AETHER_POLL_WAKE_GRACE_MS);
    expect(times).toHaveLength(AETHER_POLL_WAKE_GRACE_MS / AETHER_POLL_INTERVAL_MS + 1);
  });

  it("while a question waits, reads every 15 s and notices an answer given elsewhere", async () => {
    // Answered in Aether's own UI at 50 s, after the wake's grace period ended.
    const answeredAt = 50_000;
    const askRow = {
      ...toolRow("q1"),
      tool: {
        id: "q1",
        name: "ask_user",
        input: {},
        status: "input-available",
        display: { label: "Ask" },
      },
    };
    const question = { questions: [{ question: "Which color?", options: [{ label: "Blue" }] }] };
    const { calls, outputs } = await run(
      {
        live: (_call, at) => liveBody({ headRevision: at >= answeredAt ? 2 : 1 }),
        turns: (_call, at) =>
          at >= answeredAt
            ? turnsBody(idleTask(), "settled", 2)
            : turnsBody(
                taskBody("processing", {
                  status: "awaiting_input",
                  awaiting_input: { kind: "questions", tool_id: "q1", input: question },
                }),
                "awaiting_input",
                1,
              ),
        activity: (call) =>
          call === 0
            ? activityBody([askRow])
            : activityBody(
                [
                  askRow,
                  userMessageBody({
                    id: "answer-1",
                    toolResponse: { tool_name: "ask_user", data: { answers: { "0": [0] } } },
                  }),
                ],
                { turn_revision_sequence: 2 },
              ),
      },
      { seconds: 120 },
    );
    const times = liveCallTimes(calls);
    // Every second through the wake's grace period, then every 15 s until the answer shows.
    expect(times.slice(-3)).toEqual([
      AETHER_POLL_WAKE_GRACE_MS,
      AETHER_POLL_WAKE_GRACE_MS + AETHER_POLL_AWAITING_INTERVAL_MS,
      AETHER_POLL_WAKE_GRACE_MS + 2 * AETHER_POLL_AWAITING_INTERVAL_MS,
    ]);
    expect(times.at(-1)! - answeredAt).toBeLessThanOrEqual(AETHER_POLL_AWAITING_INTERVAL_MS);
    expect(conversation(outputs).map(summary)).toEqual([
      "TurnOrigin",
      "PendingInput",
      "user-input.requested",
      "user-input.resolved",
      "PendingInputResolved",
      "TurnSettled",
    ]);
  });

  it("hands over the task each time a turns read shows it changed", async () => {
    const { outputs } = await run(
      {
        live: (_call, at) => liveBody({ headRevision: Math.floor(at / 1_000) + 1 }),
        turns: (_call, at) =>
          turnsBody(
            taskBody("processing", {
              latest_status_message: statusAt(at),
              latest_status_source: "infra",
            }),
            "running",
            Math.floor(at / 1_000) + 1,
          ),
      },
      { seconds: 6 },
    );
    const tasks = outputs.flatMap((output) =>
      output._tag === "TaskUpdated" ? [output.task.latest_status_message] : [],
    );
    expect(tasks).toEqual(["Booting workspace", "Preparing workspace", "Workspace ready"]);
  });

  it("while the task is queued, reads the turns page in place of /live: one read a tick", async () => {
    // As recorded on a cold start: the head does not move while the workspace boots, only the
    // task's status line does; the run starting moves it.
    const bootedAt = 4_000;
    const { calls, outputs } = await run(
      {
        live: (_call, at) =>
          at < bootedAt
            ? liveBody({ headRevision: 1 })
            : liveBody({ headRevision: 2, activeExecution: executing }),
        turns: (_call, at) =>
          at < bootedAt
            ? turnsBody(
                taskBody("queued", {
                  latest_status_message: statusAt(at),
                  latest_status_source: "infra",
                }),
                "queued",
                1,
              )
            : turnsBody(
                taskBody("processing", {
                  latest_status_message: "Workspace ready",
                  latest_status_source: "infra",
                }),
                "running",
                2,
              ),
        activity: (call) => activityBody([], { turn_revision_sequence: call === 0 ? 1 : 2 }),
      },
      { seconds: 6 },
    );
    const kinds = calls.map(
      (call) =>
        `${call.at / 1_000}:${call.kind}${call.kind === "turns" ? `/${String(call.detail)}` : ""}`,
    );
    expect(kinds).toEqual([
      "0:live",
      `0:turns/${AETHER_TURNS_PAGE_LIMIT}`,
      "0:activity",
      // One turn is enough for the task and the head, until the head moves.
      "1:turns/1",
      "2:turns/1",
      "3:turns/1",
      "4:turns/1",
      `4:turns/${AETHER_TURNS_PAGE_LIMIT}`,
      "4:activity",
      // Once it runs, /live again, and the turns page only when the head moves.
      "5:live",
      "6:live",
    ]);
    const statuses = outputs.flatMap((output) =>
      output._tag === "TaskUpdated" ? [output.task.latest_status_message] : [],
    );
    expect(statuses).toEqual(["Booting workspace", "Preparing workspace", "Workspace ready"]);
  });

  it("after the turn, reads the task every minute while its pull request's CI or review runs", async () => {
    const settledAt = 150_000;
    const pullRequestAt = (at: number) => ({
      ...pullRequestBody({ ci_status: at < settledAt ? "pending" : "passed" }),
    });
    const { calls, outputs } = await run(
      {
        live: () => liveBody(),
        turns: (_call, at) =>
          turnsBody({ ...idleTask(), primary_pull_request: pullRequestAt(at) }, "settled"),
      },
      { seconds: 400 },
    );
    // Once the wake's grace ends, one read a minute until CI settles, none after: a one-turn
    // page, which carries the task and the head.
    expect(
      calls
        .filter((call) => call.at > AETHER_POLL_WAKE_GRACE_MS)
        .map((call) => [call.at, `${call.kind}/${String(call.detail)}`]),
    ).toEqual([
      [AETHER_POLL_WAKE_GRACE_MS + AETHER_POLL_PULL_REQUEST_INTERVAL_MS, "turns/1"],
      [AETHER_POLL_WAKE_GRACE_MS + 2 * AETHER_POLL_PULL_REQUEST_INTERVAL_MS, "turns/1"],
    ]);
    expect(settledAt).toBe(AETHER_POLL_WAKE_GRACE_MS + 2 * AETHER_POLL_PULL_REQUEST_INTERVAL_MS);
    const ci = outputs.flatMap((output) => {
      if (output._tag !== "TaskUpdated") return [];
      const report = aetherThreadPullRequest(output.task);
      return [report._tag === "Reported" ? report.pullRequest?.checks?.status : report.detail];
    });
    expect(ci).toEqual(["pending", "passed"]);
  });

  it("hands over the newer task the consumer read while settling, and watches its pull request", async () => {
    // The tick's turns read shows no pull request yet; the adapter's mirror, settling the
    // turn, reads the task again and finds the one the agent just opened, its CI running.
    const newer = Schema.decodeUnknownSync(AetherConversationTurnsPage)(
      turnsBody(
        { ...idleTask(), primary_pull_request: pullRequestBody({ ci_status: "pending" }) },
        "settled",
      ),
    );
    let noted = false;
    const { calls, outputs } = await run(
      {
        live: () => liveBody(),
        turns: () =>
          turnsBody(
            noted
              ? { ...idleTask(), primary_pull_request: pullRequestBody({ ci_status: "pending" }) }
              : idleTask(),
            "settled",
          ),
      },
      {
        seconds: 120,
        onEmit: (output, poller) => {
          if (output._tag !== "TurnSettled") return Effect.void;
          noted = true;
          return poller.noteTask(newer);
        },
      },
    );
    const tasks = outputs.flatMap((output) => {
      if (output._tag !== "TaskUpdated") return [];
      const report = aetherThreadPullRequest(output.task);
      return [report._tag === "Reported" ? (report.pullRequest?.checks?.status ?? "none") : "?"];
    });
    // Never the tick's older read after the newer one.
    expect(tasks).toEqual(["pending"]);
    // Its CI runs, so the loop keeps reading the task once a minute.
    expect(
      calls.filter((call) => call.at > AETHER_POLL_WAKE_GRACE_MS).map((call) => call.at),
    ).toEqual([AETHER_POLL_WAKE_GRACE_MS + AETHER_POLL_PULL_REQUEST_INTERVAL_MS]);
  });

  it("reads a turn's older activity pages and applies the rows oldest first", async () => {
    const { calls, outputs } = await run(
      {
        live: () => liveBody({ activeExecution: executing }),
        activity: (_call, before) =>
          before === undefined
            ? activityBody([toolRow("t2")], { has_more_older: true, oldest_sequence_loaded: 40 })
            : activityBody([toolRow("t1")]),
      },
      { seconds: 0 },
    );
    expect(calls.filter((call) => call.kind === "activity").map((call) => call.detail)).toEqual([
      undefined,
      40,
    ]);
    expect(
      outputs.flatMap((output) => (output._tag === "Event" ? [output.event.itemId] : [])),
    ).toEqual(["t1", "t2"]);
  });

  it("stops when a turn's activity does not end within the page limit", async () => {
    const { calls, outputs } = await run(
      {
        live: () => liveBody({ activeExecution: executing }),
        activity: (call) =>
          activityBody([toolRow(`t${call}`)], {
            has_more_older: true,
            oldest_sequence_loaded: 1000 - call,
          }),
      },
      { seconds: 0 },
    );
    expect(calls.filter((call) => call.kind === "activity")).toHaveLength(
      AETHER_ACTIVITY_MAX_PAGES,
    );
    expect(stops(outputs)).toMatchObject([
      { _tag: "AetherActivityPagingLimitError", pages: AETHER_ACTIVITY_MAX_PAGES },
    ]);
  });
});

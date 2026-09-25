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
  AETHER_POLL_INTERVAL_MS,
  AETHER_POLL_OUTAGE_LIMIT_MS,
  AETHER_POLL_WAKE_GRACE_MS,
  makeAetherConversationPoller,
  type AetherConversationReader,
  type AetherPollerOutput,
} from "./aetherConversationPoller.ts";
import { TASK_ID, TURN_ID, taskBody, userMessageBody } from "./testing/aetherApiFixtures.ts";

type Json = Record<string, unknown>;

interface Call {
  readonly at: number;
  readonly kind: "live" | "turns" | "activity";
  readonly detail?: unknown;
}

interface Script {
  readonly live: (call: number) => Json | AetherApiError;
  readonly turns?: (call: number) => Json | AetherApiError;
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
        calls.push({ at: yield* Clock.currentTimeMillis, kind: "live", detail: messageIds });
        return yield* answer(AetherLiveConversationSnapshot, script.live(counts.live++));
      }),
    getConversationTurns: () =>
      Effect.gen(function* () {
        calls.push({ at: yield* Clock.currentTimeMillis, kind: "turns" });
        const reply = script.turns?.(counts.turns++) ?? turnsBody(taskBody("processing"));
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
  options: { readonly seconds: number; readonly wakeAgainAt?: number },
) {
  const calls: Call[] = [];
  const outputs: AetherPollerOutput[] = [];
  await Effect.gen(function* () {
    const poller = yield* makeAetherConversationPoller({
      client: scriptedReader(script, calls),
      context: {
        taskId: TASK_ID,
        threadId: ThreadId.makeUnsafe("thread-1"),
        turnIdFor: (turnId) => TurnId.makeUnsafe(`synara-${turnId}`),
      },
      resume: { after: null, emittedText: new Map() },
      emit: (output) => Effect.sync(() => void outputs.push(output)),
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
const summary = (output: AetherPollerOutput) =>
  output._tag === "Event"
    ? [output.event.type, output.event.itemId].filter(Boolean).join(" ")
    : output._tag === "PollingStopped"
      ? output.error._tag
      : output._tag;

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
    expect(outputs.map(summary)).toEqual([
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
    expect(outputs.map(summary)).toEqual([
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

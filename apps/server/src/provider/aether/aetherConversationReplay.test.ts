// Replays the recorded Aether traffic (testing/recordings) through the real REST client and the
// poll loop, on a test clock, and checks the events and signals it produces.

import { AetherApiBaseUrl, ProviderRuntimeEvent, ThreadId, TurnId } from "@synara/contracts";
import { Effect, Schema } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vitest";

import { makeAetherApiClient } from "../Layers/AetherApiClient.ts";
import { AetherTaskId } from "./aetherApiSchemas.ts";
import {
  AETHER_POLL_INTERVAL_MS,
  makeAetherConversationPoller,
  type AetherPollerOutput,
} from "./aetherConversationPoller.ts";
import { loadRecording, replayTransport, type ReplayTransport } from "./testing/aetherRecording.ts";

const API_BASE_URL = AetherApiBaseUrl.makeUnsafe("https://api.runaether.dev");
const THREAD_ID = ThreadId.makeUnsafe("thread-aether-replay");

const CLAUDE_TASK = "47d30b95-4103-43e7-a0f3-5e353792716e";
const CLAUDE_TURN = "8a117558-1d50-40f0-ab8f-579a346732f2";
const CLAUDE_ANSWER = "1a8748cd-0473-4351-a542-b8c97e2f4e97";
const ASK_TOOL_ID = "toolu_014ppzUGtM2uoSgVDQ2KGm7X";
const TEXT_ID =
  "message:e7d7476e-98b6-584d-b21f-8a8176fa2fcc:text:35eb7848fba9d7b5b0b8fc9018869f649becdf547bc87b315d6a5bef7fbf07ba";

const flush = Effect.promise(() => new Promise<void>((resolve) => setImmediate(resolve)));

/** Lets the loop do everything it can without the clock moving. */
const settle = (transport: ReplayTransport) =>
  Effect.gen(function* () {
    let quietRounds = 0;
    let seen = -1;
    while (quietRounds < 3) {
      yield* flush;
      const count = transport.requested().length;
      if (count === seen && transport.inFlight() === 0) {
        quietRounds += 1;
      } else {
        quietRounds = 0;
        seen = count;
      }
    }
  });

interface Replay {
  readonly transport: ReplayTransport;
  readonly outputs: ReadonlyArray<AetherPollerOutput>;
}

/** Wakes the poller once, then advances the clock a tick at a time for `seconds`. */
async function replayThroughPoller(seconds = 120) {
  const transport = replayTransport(loadRecording("claude-question-edit-shell"));
  const outputs: AetherPollerOutput[] = [];
  await Effect.gen(function* () {
    const client = makeAetherApiClient({
      apiBaseUrl: Effect.succeed(API_BASE_URL),
      readApiKey: Effect.succeed("aether-replay-key"),
      request: transport.request,
    });
    const poller = yield* makeAetherConversationPoller({
      client,
      context: {
        taskId: AetherTaskId.makeUnsafe(CLAUDE_TASK),
        threadId: THREAD_ID,
        turnIdFor: (turnId) => TurnId.makeUnsafe(`synara-${turnId}`),
      },
      emit: (output) => Effect.sync(() => void outputs.push(output)),
    });
    yield* poller.wake;
    yield* settle(transport);
    for (let second = 0; second < seconds; second += 1) {
      yield* TestClock.adjust(AETHER_POLL_INTERVAL_MS);
      yield* settle(transport);
    }
  }).pipe(Effect.scoped, Effect.provide(TestClock.layer()), Effect.runPromise);
  return { transport, outputs } satisfies Replay;
}

const eventsOf = (outputs: ReadonlyArray<AetherPollerOutput>) =>
  outputs.flatMap((output) => (output._tag === "Event" ? [output.event] : []));

describe("Aether replay: question, file edit, shell command, long reply (Claude Code)", () => {
  it("issues exactly the recorded requests, one at a time, and goes idle once settled", async () => {
    const { transport, outputs } = await replayThroughPoller();
    // Every recorded request, in order, and none after the last: the loop went idle.
    expect(transport.mismatches()).toEqual([]);
    expect(transport.remaining()).toEqual([]);
    expect(transport.requested()).toHaveLength(57);
    expect(transport.maxInFlight()).toBe(1);
    expect(outputs.filter((output) => output._tag === "PollingStopped")).toEqual([]);
    expect(eventsOf(outputs).filter((event) => event.type === "runtime.warning")).toEqual([]);
  });

  it("produces the recorded output sequence", async () => {
    const { outputs } = await replayThroughPoller();
    const summary = outputs.map((output) =>
      output._tag === "Event"
        ? [output.event.type, output.event.itemId].filter(Boolean).join(" ")
        : output._tag,
    );
    expect(summary).toEqual([
      "TurnOrigin",
      "PendingInput",
      "user-input.requested",
      "user-input.resolved",
      "PendingInputResolved",
      "item.started toolu_01LqzJk9fKdyQMiEfXUs5Eeb",
      "item.completed toolu_01LqzJk9fKdyQMiEfXUs5Eeb",
      "item.started toolu_01LZHLGuwFLNuVgMbxuKcRhQ",
      "item.completed toolu_01LZHLGuwFLNuVgMbxuKcRhQ",
      "item.started toolu_018ofRoXAuUWMhxvqmRjFKYN",
      "item.completed toolu_018ofRoXAuUWMhxvqmRjFKYN",
      "item.started toolu_018MwGK76HNubHQMt98x6DU7",
      "item.completed toolu_018MwGK76HNubHQMt98x6DU7",
      "item.completed toolu_01X2w96QgTAChBvY6We8rcmK",
      ...Array.from({ length: 12 }, () => `content.delta ${TEXT_ID}`),
      `item.completed ${TEXT_ID}`,
      "TurnSettled",
    ]);
  });

  it("produces identical outputs when replayed twice", async () => {
    const first = await replayThroughPoller();
    const second = await replayThroughPoller();
    expect(second.outputs).toEqual(first.outputs);
  });

  it("emits valid runtime events with unique, deterministic ids", async () => {
    const { outputs } = await replayThroughPoller();
    const events = eventsOf(outputs);
    for (const event of events) {
      const encoded = Schema.encodeUnknownSync(ProviderRuntimeEvent)(event);
      expect(Schema.decodeUnknownSync(ProviderRuntimeEvent)(encoded)).toEqual(event);
      expect(event.eventId.startsWith(`aether:${CLAUDE_TASK}:`)).toBe(true);
    }
    const ids = events.map((event) => event.eventId);
    expect(new Set(ids).size).toBe(ids.length);
  });

  it("settles the turn exactly once, on the answer's run, after its last event", async () => {
    const { outputs } = await replayThroughPoller();
    const settled = outputs.filter((output) => output._tag === "TurnSettled");
    expect(settled).toEqual([
      { _tag: "TurnSettled", turnId: CLAUDE_TURN, runMessageId: CLAUDE_ANSWER, state: "settled" },
    ]);
    const settledAt = outputs.findIndex((output) => output._tag === "TurnSettled");
    const lastEventAt = outputs.findLastIndex((output) => output._tag === "Event");
    expect(settledAt).toBeGreaterThan(lastEventAt);
    expect(outputs.filter((output) => output._tag === "TurnOrigin")).toEqual([
      {
        _tag: "TurnOrigin",
        turnId: CLAUDE_TURN,
        rootMessageId: "afe3ef02-9374-4f5a-ba1b-fce213c93f31",
        rootMessageText: expect.stringMatching(/^This task generates API fixtures/),
      },
    ]);
  });

  it("surfaces the question, then resolves it with the chosen label after the answer", async () => {
    const { outputs } = await replayThroughPoller();
    const requestedAt = outputs.findIndex(
      (output) => output._tag === "Event" && output.event.type === "user-input.requested",
    );
    const resolvedAt = outputs.findIndex(
      (output) => output._tag === "Event" && output.event.type === "user-input.resolved",
    );
    const firstToolAt = outputs.findIndex(
      (output) => output._tag === "Event" && output.event.type.startsWith("item."),
    );
    expect(requestedAt).toBeGreaterThanOrEqual(0);
    expect(resolvedAt).toBeGreaterThan(requestedAt);
    // The agent's work after the answer comes after the resolution.
    expect(firstToolAt).toBeGreaterThan(resolvedAt);

    const requested = outputs[requestedAt];
    const resolved = outputs[resolvedAt];
    if (requested?._tag !== "Event" || resolved?._tag !== "Event") throw new Error("unreachable");
    expect(requested.event).toMatchObject({
      type: "user-input.requested",
      requestId: ASK_TOOL_ID,
      turnId: `synara-${CLAUDE_TURN}`,
      payload: {
        questions: [
          {
            id: "0",
            header: "Button label",
            question: "Which label should the checkout submit button use?",
            options: [
              {
                label: "Pay now",
                description: 'Use "Pay now" as the checkout submit button label.',
              },
              {
                label: "Place order",
                description: 'Use "Place order" as the checkout submit button label.',
              },
            ],
            multiSelect: false,
          },
        ],
      },
    });
    expect(resolved.event).toMatchObject({
      type: "user-input.resolved",
      requestId: ASK_TOOL_ID,
      payload: { answers: { "0": "Place order" } },
    });

    const pending = outputs.filter(
      (output) => output._tag === "PendingInput" || output._tag === "PendingInputResolved",
    );
    expect(pending).toEqual([
      {
        _tag: "PendingInput",
        turnId: CLAUDE_TURN,
        toolId: ASK_TOOL_ID,
        kind: "questions",
        input: {
          questions: [
            {
              header: "Button label",
              options: [
                {
                  label: "Pay now",
                  description: 'Use "Pay now" as the checkout submit button label.',
                },
                {
                  label: "Place order",
                  description: 'Use "Place order" as the checkout submit button label.',
                },
              ],
              question: "Which label should the checkout submit button use?",
              multiSelect: false,
            },
          ],
        },
      },
      { _tag: "PendingInputResolved", turnId: CLAUDE_TURN, toolId: ASK_TOOL_ID, kind: "questions" },
    ]);
    // The question tool is surfaced as a question, never as a tool row.
    expect(
      eventsOf(outputs).some(
        (event) => event.type.startsWith("item.") && event.itemId === ASK_TOOL_ID,
      ),
    ).toBe(false);
  });

  it("maps the shell commands, file read and file edit to completed tool rows", async () => {
    const { outputs } = await replayThroughPoller();
    const completed = eventsOf(outputs).flatMap((event) =>
      event.type === "item.completed" && event.payload.itemType !== "assistant_message"
        ? [[event.itemId, event.payload.itemType, event.payload.status, event.payload.detail]]
        : [],
    );
    const form = "/home/coder/project/web/src/components/CheckoutForm.tsx";
    expect(completed).toEqual([
      ["toolu_01LqzJk9fKdyQMiEfXUs5Eeb", "command_execution", "completed", expect.any(String)],
      ["toolu_01LZHLGuwFLNuVgMbxuKcRhQ", "command_execution", "completed", expect.any(String)],
      ["toolu_018ofRoXAuUWMhxvqmRjFKYN", "dynamic_tool_call", "completed", form],
      ["toolu_018MwGK76HNubHQMt98x6DU7", "file_change", "completed", form],
      ["toolu_01X2w96QgTAChBvY6We8rcmK", "command_execution", "completed", "git diff --stat"],
    ]);
    const diff = eventsOf(outputs).find(
      (event) =>
        event.type === "item.completed" && event.itemId === "toolu_01X2w96QgTAChBvY6We8rcmK",
    );
    expect(diff).toMatchObject({
      payload: {
        data: {
          toolName: "Bash",
          durationMs: 239,
          result:
            " web/src/components/CheckoutForm.tsx | 2 +-\n 1 file changed, 1 insertion(+), 1 deletion(-)",
        },
      },
    });
  });

  it("streams the reply as suffix deltas that add up to the final text", async () => {
    const { outputs } = await replayThroughPoller();
    const events = eventsOf(outputs);
    const deltas = events.flatMap((event) =>
      event.type === "content.delta" && event.itemId === TEXT_ID ? [event.payload] : [],
    );
    // One delta per live tick that grew the message: eleven open snapshots, then the close.
    expect(deltas).toHaveLength(12);
    expect(deltas.every((delta) => delta.streamKind === "assistant_text")).toBe(true);
    const completion = events.find(
      (event) => event.type === "item.completed" && event.itemId === TEXT_ID,
    );
    if (completion?.type !== "item.completed") throw new Error("the reply never completed");
    expect(deltas.map((delta) => delta.delta).join("")).toBe(completion.payload.detail);
    expect(completion.payload.detail).toMatch(/^The checkout submit button now says "Place order"/);
    // The empty thinking block streams nothing and completes nothing.
    expect(events.some((event) => event.itemId?.includes(":thinking:"))).toBe(false);
  });
});

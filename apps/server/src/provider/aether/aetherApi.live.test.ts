// Read-only smoke test against a real Aether API. Skipped unless both are set:
//   AETHER_LIVE_API_KEY       an Aether API key
//   AETHER_LIVE_API_BASE_URL  e.g. https://api.runaether.dev
// Optional:
//   AETHER_LIVE_TASK_ID       an existing task: reads its turns, one turn's activity, and `live`
//
// Run from apps/server: bun run test src/provider/aether/aetherApi.live.test.ts
// It never creates, answers, stops or connects anything.

import { AetherApiBaseUrl } from "@synara/contracts";
import { Cause, Effect, Exit, Schema } from "effect";
import { describe, expect, it } from "vitest";

import { makeAetherApiClient } from "../Layers/AetherApiClient.ts";
import { AetherTaskId } from "./aetherApiSchemas.ts";

const apiKey = process.env.AETHER_LIVE_API_KEY?.trim() ?? "";
const apiBaseUrl = process.env.AETHER_LIVE_API_BASE_URL?.trim() ?? "";
const taskId = process.env.AETHER_LIVE_TASK_ID?.trim() ?? "";

async function run<A, E>(effect: Effect.Effect<A, E>): Promise<A> {
  const exit = await Effect.runPromiseExit(effect);
  if (Exit.isFailure(exit)) throw new Error(Cause.pretty(exit.cause));
  return exit.value;
}

describe.skipIf(apiKey === "" || apiBaseUrl === "")("Aether API (live, read-only)", () => {
  const client = makeAetherApiClient({
    apiBaseUrl: Effect.sync(() => Schema.decodeUnknownSync(AetherApiBaseUrl)(apiBaseUrl)),
    readApiKey: Effect.succeed(apiKey),
  });

  it("reads the profile", async () => {
    expect((await run(client.getProfile())).email).not.toBe("");
  });

  it("lists the projects", async () => {
    expect(Array.isArray(await run(client.listProjects()))).toBe(true);
  });

  it.skipIf(taskId === "")("reads a task's turns, one turn's activity, and live", async () => {
    const task = Schema.decodeUnknownSync(AetherTaskId)(taskId);
    const page = await run(client.getConversationTurns(task, { limit: 20 }));
    const turn = page.turns[0];
    if (turn === undefined) throw new Error("The task has no turn to read the activity of.");
    await run(client.getTurnActivity(task, turn.id, { limit: 50 }));
    const live = await run(client.getLiveConversation(task, []));
    expect(live.headRevision).toBeGreaterThanOrEqual(0);
  });
});

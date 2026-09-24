// Read-only smoke test against a real Aether API. Skipped unless both are set:
//   AETHER_LIVE_API_KEY       an Aether API key
//   AETHER_LIVE_API_BASE_URL  e.g. https://api.runaether.dev
// Optional:
//   AETHER_LIVE_TASK_ID       a task to read turns, one turn's activity, and the live snapshot of
//
// Run from apps/server: bun run test src/provider/aether/aetherApi.live.test.ts
// It never creates, answers, stops or updates anything.

import { AetherApiBaseUrl } from "@synara/contracts";
import { Cause, Effect, Exit, Schema } from "effect";
import { describe, expect, it } from "vitest";

import { makeAetherApiClient } from "./aetherApiClient.ts";

const apiKey = process.env.AETHER_LIVE_API_KEY?.trim() ?? "";
const apiBaseUrl = process.env.AETHER_LIVE_API_BASE_URL?.trim() ?? "";
const taskId = process.env.AETHER_LIVE_TASK_ID?.trim() ?? "";

const liveDescribe = apiKey !== "" && apiBaseUrl !== "" ? describe : describe.skip;

async function run<A, E>(effect: Effect.Effect<A, E>): Promise<A> {
  const exit = await Effect.runPromiseExit(effect);
  if (Exit.isFailure(exit)) throw new Error(Cause.pretty(exit.cause));
  return exit.value;
}

liveDescribe("Aether API (live, read-only)", () => {
  const client = makeAetherApiClient({
    connection: Effect.sync(() => ({
      apiBaseUrl: Schema.decodeUnknownSync(AetherApiBaseUrl)(apiBaseUrl),
      apiKey,
    })),
  });

  it("reads the profile", async () => {
    const profile = await run(client.getProfile());
    expect(profile.email.length).toBeGreaterThan(0);
  });

  it("lists the projects", async () => {
    const projects = await run(client.listProjects());
    expect(Array.isArray(projects)).toBe(true);
  });

  it.skipIf(taskId === "")(
    "reads a task's turns, one turn's activity, and its live snapshot",
    async () => {
      const page = await run(client.getConversationTurns(taskId, { limit: 20 }));
      expect(page.task.id).toBe(taskId);

      const turn = page.turns[0];
      if (turn !== undefined) {
        const activity = await run(client.getTurnActivity(taskId, turn.id, { limit: 50 }));
        expect(activity.task.id).toBe(taskId);
      }

      const observed = page.turns.flatMap((entry) =>
        entry.latest_agent_message === null ? [] : [entry.latest_agent_message.id],
      );
      const live = await run(client.getLiveConversation(taskId, observed.slice(0, 200)));
      expect(live.taskId).toBe(taskId);
    },
  );
});

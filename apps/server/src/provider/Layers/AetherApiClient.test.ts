import * as Http from "node:http";
import type { AddressInfo } from "node:net";

import { AetherApiBaseUrl } from "@synara/contracts";
import {
  OutboundHttpError,
  type OutboundHttpRequest,
  type OutboundHttpResponse,
} from "@synara/shared/outboundHttp";
import { OutboundPolicyError } from "@synara/shared/outboundHttpPolicy";
import { Cause, Effect, Exit, Layer, Option } from "effect";
import { describe, expect, it } from "vitest";

import { ProviderCredentials } from "../../providerCredentials.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import type { AetherApiError } from "../aether/aetherApiErrors.ts";
import type {
  AetherCreateTaskRequest,
  AetherRespondToTaskRequest,
} from "../aether/aetherApiSchemas.ts";
import {
  activityPageBody,
  connectRunningBody,
  liveSnapshotBody,
  MESSAGE_ID,
  PROJECT_ID,
  profileBody,
  projectBody,
  TASK_ID,
  taskBody,
  TOOL_ID,
  toolRowBody,
  turnBody,
  TURN_ID,
  turnsPageBody,
  withoutKey,
  WORKSPACE_ID,
} from "../aether/testing/aetherApiFixtures.ts";
import { AetherApiClient, type AetherApiClientShape } from "../Services/AetherApiClient.ts";
import {
  AETHER_API_TIMEOUT_MS,
  AetherApiClientLive,
  makeAetherApiClient,
  type AetherHttpRequest,
} from "./AetherApiClient.ts";

const API_BASE_URL = AetherApiBaseUrl.makeUnsafe("https://api.runaether.dev");
const API_KEY = "aether-secret";

type Json = Record<string, unknown>;
type Reply = OutboundHttpResponse | Error;

function json(status: number, body: unknown): OutboundHttpResponse {
  return raw(status, body === undefined ? "" : JSON.stringify(body));
}

function raw(status: number, text: string): OutboundHttpResponse {
  return { status, headers: new Headers(), body: new TextEncoder().encode(text), url: "" };
}

function clientWith(reply: Reply | ((request: OutboundHttpRequest) => Reply)) {
  const requests: OutboundHttpRequest[] = [];
  const request: AetherHttpRequest = async (input) => {
    requests.push(input);
    const result = typeof reply === "function" ? reply(input) : reply;
    if (result instanceof Error) throw result;
    return result;
  };
  const client = makeAetherApiClient({
    apiBaseUrl: Effect.succeed(API_BASE_URL),
    readApiKey: Effect.succeed(API_KEY),
    request,
  });
  return { client, requests };
}

async function succeed<A>(effect: Effect.Effect<A, AetherApiError>): Promise<A> {
  const exit = await Effect.runPromiseExit(effect);
  if (Exit.isFailure(exit)) throw new Error(Cause.pretty(exit.cause));
  return exit.value;
}

async function fail(effect: Effect.Effect<unknown, AetherApiError>): Promise<AetherApiError> {
  const exit = await Effect.runPromiseExit(effect);
  if (Exit.isSuccess(exit)) throw new Error("expected the call to fail");
  const error = Cause.findErrorOption(exit.cause);
  if (Option.isNone(error)) throw new Error(Cause.pretty(exit.cause));
  return error.value;
}

/** Adds an unknown key to every object in a payload, as a newer server would. */
function withAddedFields(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(withAddedFields);
  if (typeof value !== "object" || value === null) return value;
  const out: Json = { added_by_a_newer_server: { nested: [1, "two"] } };
  for (const [key, child] of Object.entries(value)) out[key] = withAddedFields(child);
  return out;
}

const createRequest: AetherCreateTaskRequest = {
  project_id: PROJECT_ID,
  prompt: "Build the Aether API client.",
  agent_type: "claude-code",
  model: "claude-opus-5-5",
  interaction_mode: "default",
  idempotency_key: "2c4e6a8b-0d1f-4a3c-9e5b-7d9f1b3d5f7a",
  base_branch: "main",
};

const respondRequest: AetherRespondToTaskRequest = {
  message: "Use the WebSocket.",
  client_message_id: "8f7e6d5c-4b3a-4c2d-9e1f-0a9b8c7d6e5f",
  queue_for_later: true,
  tool_response: {
    tool_name: "ask_user",
    data: { answers: { "0": [0] } },
    expected_tool_id: TOOL_ID,
    expected_input_digest: "a".repeat(64),
  },
};

const TASK_URL = `${API_BASE_URL}/tasks/${TASK_ID}`;

const operations: ReadonlyArray<{
  readonly name: string;
  readonly method: string;
  readonly url: string;
  readonly body?: unknown;
  readonly call: (client: AetherApiClientShape) => Effect.Effect<unknown, AetherApiError>;
  readonly ok: readonly [number, Json | undefined];
}> = [
  {
    name: "getProfile",
    method: "GET",
    url: `${API_BASE_URL}/profile`,
    call: (client) => client.getProfile(),
    ok: [200, profileBody()],
  },
  {
    name: "listProjects",
    method: "GET",
    url: `${API_BASE_URL}/projects`,
    call: (client) => client.listProjects(),
    ok: [200, { projects: [projectBody()] }],
  },
  {
    name: "createTask",
    method: "POST",
    url: `${API_BASE_URL}/tasks`,
    body: createRequest,
    call: (client) => client.createTask(createRequest),
    ok: [202, { id: TASK_ID, name: "Add the Aether API client" }],
  },
  {
    name: "respondToTask",
    method: "POST",
    url: `${TASK_URL}/respond`,
    body: respondRequest,
    call: (client) => client.respondToTask(TASK_ID, respondRequest),
    ok: [202, { message_id: "msg_user_02" }],
  },
  {
    name: "stopTask",
    method: "POST",
    url: `${TASK_URL}/stop`,
    body: { discard_queued_messages: true },
    call: (client) => client.stopTask(TASK_ID, { discard_queued_messages: true }),
    ok: [200, undefined],
  },
  {
    name: "removeFromQueue",
    method: "POST",
    url: `${TASK_URL}/remove-from-queue`,
    body: { message_id: MESSAGE_ID },
    call: (client) => client.removeFromQueue(TASK_ID, MESSAGE_ID),
    ok: [200, undefined],
  },
  {
    name: "getConversationTurns",
    method: "GET",
    url: `${TASK_URL}/conversation/turns?before=40&limit=50`,
    call: (client) => client.getConversationTurns(TASK_ID, { before: 40, limit: 50 }),
    ok: [200, turnsPageBody()],
  },
  {
    name: "getTurnActivity",
    method: "GET",
    url: `${TASK_URL}/conversation/turns/${TURN_ID}/activity`,
    call: (client) => client.getTurnActivity(TASK_ID, TURN_ID),
    ok: [200, activityPageBody()],
  },
  {
    name: "getLiveConversation",
    method: "GET",
    url: `${TASK_URL}/conversation/live?messageIds=${encodeURIComponent(`["${MESSAGE_ID}"]`)}`,
    call: (client) => client.getLiveConversation(TASK_ID, [MESSAGE_ID]),
    ok: [200, liveSnapshotBody()],
  },
  {
    name: "connectWorkspace",
    method: "POST",
    url: `${API_BASE_URL}/workspaces/${WORKSPACE_ID}/connect?start=false`,
    call: (client) => client.connectWorkspace(WORKSPACE_ID, { start: false }),
    ok: [200, connectRunningBody()],
  },
];

describe("AetherApiClient", () => {
  for (const operation of operations) {
    it(`${operation.name}: sends the request once, pinned to the origin, and decodes the answer`, async () => {
      const [status, body] = operation.ok;
      const { client, requests } = clientWith(json(status, body));

      await succeed(operation.call(client));

      expect(requests).toHaveLength(1);
      const [request] = requests;
      expect(request?.method).toBe(operation.method);
      expect(String(request?.url)).toBe(operation.url);
      expect(request?.body === undefined ? undefined : JSON.parse(String(request.body))).toEqual(
        operation.body,
      );
      const headers = new Headers(request?.headers);
      expect(headers.get("authorization")).toBe(`Bearer ${API_KEY}`);
      expect(headers.get("content-type")).toBe(
        operation.body === undefined ? null : "application/json",
      );
      expect(request?.policy).toMatchObject({
        allowedOrigins: [API_BASE_URL],
        timeoutMs: AETHER_API_TIMEOUT_MS,
        maxRedirects: 0,
        requirePublicAddress: true,
        allowLoopbackHttp: false,
      });
      expect(request?.signal).toBeInstanceOf(AbortSignal);
    });
  }

  it("tolerates fields a newer server adds at any depth, on every operation", async () => {
    for (const operation of operations) {
      const [status, body] = operation.ok;
      await succeed(operation.call(clientWith(json(status, withAddedFields(body))).client));
    }
  });

  it("decodes the task, its workspace and branded ids from the turns page", async () => {
    const page = await succeed(
      clientWith(
        json(200, turnsPageBody({ task: taskBody("awaiting_input") })),
      ).client.getConversationTurns(TASK_ID),
    );

    expect(page.task).toEqual({
      status: "awaiting_input",
      run_context: { workspace_id: WORKSPACE_ID },
      // The input is kept whole, unknown keys included: the respond fence digests all of it.
      awaiting_input: {
        kind: "questions",
        tool_id: TOOL_ID,
        input: { questions: [{ question: "Which transport?", options: [] }], extra: [1, 2] },
      },
    });
    expect(page.turns[0]).toMatchObject({ id: TURN_ID, state: "settled", revision_sequence: 9 });
  });

  it("decodes activity rows, with exitCode and durationMs on tool rows", async () => {
    const activity = await succeed(
      clientWith(json(200, activityPageBody())).client.getTurnActivity(TASK_ID, TURN_ID),
    );

    expect(activity.items.map((item) => ("variant" in item ? item.variant : item.role))).toEqual([
      "user",
      "thinking",
      "tool",
      "text",
      "seam",
    ]);
    expect(activity.items[2]).toMatchObject({
      tool: {
        status: "output-available",
        itemType: "command_execution",
        exitCode: 0,
        durationMs: 5120,
      },
    });
  });

  it("rejects a missing field or a malformed id, naming it", async () => {
    const cases: ReadonlyArray<
      readonly [
        (client: AetherApiClientShape) => Effect.Effect<unknown, AetherApiError>,
        Json,
        string,
      ]
    > = [
      [
        (client) => client.getConversationTurns(TASK_ID),
        withoutKey(turnsPageBody(), ["turns", 0, "revision_sequence"]),
        '["turns"][0]["revision_sequence"]',
      ],
      [(client) => client.createTask(createRequest), { id: "not-a-uuid" }, '["id"]'],
    ];
    for (const [call, body, named] of cases) {
      const error = await fail(call(clientWith(json(200, body)).client));
      expect(error._tag).toBe("AetherApiDecodeError");
      expect(error._tag === "AetherApiDecodeError" && error.detail).toContain(named);
    }
    expect(await fail(clientWith(raw(200, "<html>")).client.getProfile())).toMatchObject({
      _tag: "AetherApiDecodeError",
      detail: "The body is not valid JSON.",
    });
  });

  it("decodes a tool type, tool status or seam reason it does not know as Unknown, keeping the page", async () => {
    const activity = await succeed(
      clientWith(
        json(
          200,
          activityPageBody({
            items: [
              toolRowBody({ itemType: "notebook_edit", status: "queued" }),
              { id: "msg_seam_02", role: "assistant", variant: "seam", seam: { reason: "fork" } },
              toolRowBody(),
            ],
          }),
        ),
      ).client.getTurnActivity(TASK_ID, TURN_ID),
    );

    expect(activity.items[0]).toMatchObject({
      tool: {
        itemType: { _tag: "Unknown", raw: "notebook_edit" },
        status: { _tag: "Unknown", raw: "queued" },
      },
    });
    expect(activity.items[1]).toMatchObject({ seam: { reason: { _tag: "Unknown", raw: "fork" } } });
    // Known values still decode to their literal.
    expect(activity.items[2]).toMatchObject({
      tool: { itemType: "command_execution", status: "output-available" },
    });
  });

  it("still fails on a turn state it does not know, naming the field", async () => {
    const error = await fail(
      clientWith(
        json(200, turnsPageBody({ turns: [turnBody({ state: "paused" })] })),
      ).client.getConversationTurns(TASK_ID),
    );

    expect(error._tag).toBe("AetherApiDecodeError");
    expect(error._tag === "AetherApiDecodeError" && error.detail).toContain(
      'got "paused"\n  at ["turns"][0]["state"]',
    );
  });

  it("maps other statuses to one HTTP error, keeping a readable server message", async () => {
    for (const status of [401, 403, 404, 422, 429, 500]) {
      const error = await fail(clientWith(json(status, { error: "nope" })).client.getProfile());
      expect(error).toMatchObject({ _tag: "AetherApiHttpError", status, serverMessage: "nope" });
    }
    const gateway = await fail(clientWith(raw(502, "<html>")).client.getProfile());
    expect(gateway).toMatchObject({ _tag: "AetherApiHttpError", status: 502 });
    expect(gateway).not.toHaveProperty("serverMessage");
    // A 409 where the endpoint declares none.
    expect(
      await fail(clientWith(json(409, { error: "x" })).client.getConversationTurns(TASK_ID)),
    ).toMatchObject({ _tag: "AetherApiHttpError", status: 409 });
  });

  it("decodes a 402 into its admission fields, and fails when they are missing", async () => {
    const admission = { error: "No large VMs", code: "vm_size", tier: "free", vm_size: "large" };

    expect(
      await fail(clientWith(json(402, admission)).client.createTask(createRequest)),
    ).toMatchObject({
      _tag: "AetherApiPaymentRequiredError",
      code: "vm_size",
      tier: "free",
      vmSize: "large",
      serverMessage: "No large VMs",
    });
    const missing = await fail(
      clientWith(json(402, withoutKey(admission, ["vm_size"]))).client.createTask(createRequest),
    );
    expect(missing).toMatchObject({ _tag: "AetherApiDecodeError", status: 402 });
  });

  it("decodes each endpoint's 409 body as sent", async () => {
    const cases: ReadonlyArray<
      readonly [(client: AetherApiClientShape) => Effect.Effect<unknown, AetherApiError>, Json]
    > = [
      [
        (client) => client.createTask(createRequest),
        { error: "reused", code: "idempotency_key_reused" },
      ],
      [
        (client) => client.respondToTask(TASK_ID, respondRequest),
        { error: "moved on", code: "stale_pending_input", current_tool_id: "toolu_09" },
      ],
      [
        (client) => client.respondToTask(TASK_ID, respondRequest),
        { error: "gone", code: "task_creator_deleted" },
      ],
      [
        (client) => client.respondToTask(TASK_ID, respondRequest),
        { error: "answer the questions", awaiting_input_kind: "questions" },
      ],
      [
        (client) => client.respondToTask(TASK_ID, respondRequest),
        { error: "busy", code: "task_busy" },
      ],
      [
        (client) => client.stopTask(TASK_ID, { discard_queued_messages: false }),
        { error: "discard first", code: "queued_stop_requires_discard" },
      ],
      [
        (client) => client.connectWorkspace(WORKSPACE_ID, { start: false }),
        { kind: "transitional", error: "suspending", retry_after_ms: 2000 },
      ],
      [
        (client) => client.connectWorkspace(WORKSPACE_ID, { start: false }),
        { kind: "startable", error: "stopped" },
      ],
      [
        (client) => client.connectWorkspace(WORKSPACE_ID, { start: false }),
        { error: "gone", code: "task_creator_deleted" },
      ],
    ];
    for (const [call, body] of cases) {
      const error = await fail(call(clientWith(json(409, body)).client));
      expect(error._tag).toBe("AetherApiConflictError");
      expect(error._tag === "AetherApiConflictError" && error.body).toEqual(body);
    }

    const unknownKind = await fail(
      clientWith(json(409, { kind: "migrating", error: "moving" })).client.connectWorkspace(
        WORKSPACE_ID,
        { start: false },
      ),
    );
    expect(unknownKind).toMatchObject({ _tag: "AetherApiDecodeError", status: 409 });
    const otherCreateBody = await fail(
      clientWith(json(409, { error: "x", code: "other" })).client.createTask(createRequest),
    );
    expect(otherCreateBody).toMatchObject({ _tag: "AetherApiDecodeError", status: 409 });
  });

  it("maps a timeout and other transport failures, keeping the cause", async () => {
    const timeout = new OutboundHttpError("timeout", "deadline");
    expect(await fail(clientWith(timeout).client.getProfile())).toMatchObject({
      _tag: "AetherApiTimeoutError",
      timeoutMs: AETHER_API_TIMEOUT_MS,
    });
    for (const cause of [
      new OutboundPolicyError("private-address", "private"),
      new Error("ECONNRESET"),
    ]) {
      expect(await fail(clientWith(cause).client.getProfile())).toMatchObject({
        _tag: "AetherApiTransportError",
        endpoint: "GET /profile",
        cause,
      });
    }
  });

  it("sends nothing without a key, and says why", async () => {
    const requests: unknown[] = [];
    const client = (readApiKey: Effect.Effect<string | null, unknown>) =>
      makeAetherApiClient({
        apiBaseUrl: Effect.succeed(API_BASE_URL),
        readApiKey,
        request: async (input) => {
          requests.push(input);
          return json(200, profileBody());
        },
      });

    expect(await fail(client(Effect.succeed(null)).getProfile())).toMatchObject({
      _tag: "AetherApiNotConfiguredError",
      reason: "missing-api-key",
    });
    expect(await fail(client(Effect.fail(new Error("EACCES"))).getProfile())).toMatchObject({
      _tag: "AetherApiNotConfiguredError",
      reason: "unreadable-api-key",
    });
    expect(requests).toEqual([]);
  });

  it("live layer: reads the base URL from settings and the key for each request", async () => {
    const seen: Array<string | undefined> = [];
    const server = Http.createServer((request, response) => {
      seen.push(request.headers.authorization);
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify(profileBody()));
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const { port } = server.address() as AddressInfo;
    let secret: string | null = "first-key";
    try {
      await Effect.runPromise(
        Effect.gen(function* () {
          const client = yield* AetherApiClient;
          yield* client.getProfile();
          secret = "rotated-key";
          yield* client.getProfile();
        }).pipe(
          Effect.provide(
            AetherApiClientLive.pipe(
              Layer.provide(
                Layer.merge(
                  Layer.succeed(ProviderCredentials, {
                    getSecret: () => Effect.sync(() => secret),
                    replaceSecret: () => Effect.void,
                    isSecretConfigured: () => Effect.succeed(true),
                  }),
                  ServerSettingsService.layerTest({
                    providers: {
                      aether: {
                        apiBaseUrl: AetherApiBaseUrl.makeUnsafe(`http://127.0.0.1:${port}`),
                      },
                    },
                  }),
                ),
              ),
            ),
          ),
        ),
      );
    } finally {
      await new Promise((resolve) => server.close(resolve));
    }

    expect(seen).toEqual(["Bearer first-key", "Bearer rotated-key"]);
  });
});

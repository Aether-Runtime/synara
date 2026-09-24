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
import {
  AetherApiClient,
  makeAetherApiClient,
  makeAetherApiClientLive,
  resolveAetherApiConnection,
  type AetherApiClientShape,
  type AetherHttpRequest,
} from "./aetherApiClient.ts";
import { AetherApiNotConfiguredError, type AetherApiError } from "./aetherApiErrors.ts";
import type { AetherCreateTaskRequest, AetherRespondToTaskRequest } from "./aetherApiSchemas.ts";
import {
  activityPageBody,
  admissionBody,
  connectConnectingBody,
  connectRunningBody,
  errorBody,
  liveMessageBody,
  liveSnapshotBody,
  orgBody,
  PROJECT_ID,
  profileBody,
  projectBody,
  subagentsBody,
  TASK_ID,
  taskBody,
  toolMessageBody,
  turnBody,
  TURN_ID,
  turnsPageBody,
  WORKSPACE_ID,
  withoutKey,
} from "./testing/aetherApiFixtures.ts";

const API_BASE_URL = AetherApiBaseUrl.makeUnsafe("https://api.runaether.dev");
const API_KEY = "aether-secret";

type Json = Record<string, unknown>;
type Reply = OutboundHttpResponse | Error;

function json(status: number, body: unknown): OutboundHttpResponse {
  return raw(status, body === undefined ? "" : JSON.stringify(body));
}

function raw(status: number, text: string, contentType = "application/json"): OutboundHttpResponse {
  return {
    status,
    headers: new Headers({ "content-type": contentType }),
    body: new TextEncoder().encode(text),
    url: "",
  };
}

function fakeHttp(respond: (request: OutboundHttpRequest) => Reply | Promise<Reply>) {
  const requests: OutboundHttpRequest[] = [];
  const request: AetherHttpRequest = async (input) => {
    requests.push(input);
    const result = await respond(input);
    if (result instanceof Error) throw result;
    return result;
  };
  return { request, requests };
}

function clientWith(reply: Reply | ((request: OutboundHttpRequest) => Reply | Promise<Reply>)) {
  const http = fakeHttp(typeof reply === "function" ? reply : () => reply);
  const client = makeAetherApiClient({
    connection: Effect.succeed({ apiBaseUrl: API_BASE_URL, apiKey: API_KEY }),
    request: http.request,
  });
  return { client, requests: http.requests };
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
  if (Option.isNone(error))
    throw new Error(`expected a typed failure: ${Cause.pretty(exit.cause)}`);
  return error.value;
}

/** Adds an unknown key to every object in a payload, as a newer server would. */
function withAddedFields(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(withAddedFields);
  if (typeof value !== "object" || value === null) return value;
  const out: Record<string, unknown> = { added_by_a_newer_server: { nested: [1, "two"] } };
  for (const [key, child] of Object.entries(value)) out[key] = withAddedFields(child);
  return out;
}

const createRequest: AetherCreateTaskRequest = {
  project_id: PROJECT_ID,
  prompt: "Build the Aether API client.",
  agent_type: "claude-code",
  model: "claude-opus-5-5",
  interaction_mode: "default",
  base_branch: "main",
  reasoning_effort: "high",
  idempotency_key: "2c4e6a8b-0d1f-4a3c-9e5b-7d9f1b3d5f7a",
  auto_fix_ci: false,
  auto_fix_pr_comments: false,
  auto_rebase: false,
};

const respondRequest: AetherRespondToTaskRequest = {
  message: "Use the WebSocket.",
  client_message_id: "8f7e6d5c-4b3a-4c2d-9e1f-0a9b8c7d6e5f",
  queue_for_later: true,
  tool_response: {
    tool_name: "ask_user",
    data: { answers: { "0": [0] }, customAnswers: { "1": "Other" } },
    expected_tool_id: "toolu_01",
    expected_input_digest: "a".repeat(64),
  },
};

const updateRequest = {
  agent_type: "claude-code",
  model: "claude-sonnet-5",
  interaction_mode: "plan",
  reasoning_effort: null,
  auto_fix_ci: false,
  auto_fix_pr_comments: false,
  auto_rebase: false,
} as const;

interface Operation {
  readonly name: string;
  readonly method: string;
  readonly url: string;
  readonly call: (client: AetherApiClientShape) => Effect.Effect<unknown, AetherApiError>;
  /** 2xx status and body; `undefined` body for routes that answer without one. */
  readonly ok: readonly [number, Json | undefined];
  /** A required response key to delete, and the path the decode error must name. */
  readonly required?: { readonly path: ReadonlyArray<string | number>; readonly named: string };
}

const TASK_URL = `${API_BASE_URL}/tasks/${TASK_ID}`;

const operations: ReadonlyArray<Operation> = [
  {
    name: "getProfile",
    method: "GET",
    url: `${API_BASE_URL}/profile`,
    call: (client) => client.getProfile(),
    ok: [200, profileBody()],
    required: { path: ["email"], named: '["email"]' },
  },
  {
    name: "listProjects",
    method: "GET",
    url: `${API_BASE_URL}/projects`,
    call: (client) => client.listProjects(),
    ok: [200, { projects: [projectBody()] }],
    required: {
      path: ["projects", 0, "task_defaults"],
      named: '["projects"]["0"]["task_defaults"]',
    },
  },
  {
    name: "getProject",
    method: "GET",
    url: `${API_BASE_URL}/projects/${PROJECT_ID}`,
    call: (client) => client.getProject(PROJECT_ID),
    ok: [200, projectBody()],
    required: { path: ["id"], named: '["id"]' },
  },
  {
    name: "createTask",
    method: "POST",
    url: `${API_BASE_URL}/tasks`,
    call: (client) => client.createTask(createRequest),
    ok: [202, { id: TASK_ID, name: "Add the Aether API client" }],
    required: { path: ["name"], named: '["name"]' },
  },
  {
    name: "getTask",
    method: "GET",
    url: TASK_URL,
    call: (client) => client.getTask(TASK_ID),
    ok: [200, taskBody("processing")],
    required: { path: ["latest_message_revision"], named: '["latest_message_revision"]' },
  },
  {
    name: "updateTask",
    method: "PUT",
    url: TASK_URL,
    call: (client) => client.updateTask(TASK_ID, updateRequest),
    ok: [200, taskBody("processing")],
    // Always present while processing.
    required: { path: ["run_context"], named: '["run_context"]' },
  },
  {
    name: "respondToTask",
    method: "POST",
    url: `${TASK_URL}/respond`,
    call: (client) => client.respondToTask(TASK_ID, respondRequest),
    ok: [202, { message_id: "msg_user_02" }],
    required: { path: ["message_id"], named: '["message_id"]' },
  },
  {
    name: "stopTask",
    method: "POST",
    url: `${TASK_URL}/stop`,
    call: (client) => client.stopTask(TASK_ID, { discard_queued_messages: true }),
    ok: [200, undefined],
  },
  {
    name: "removeFromQueue",
    method: "POST",
    url: `${TASK_URL}/remove-from-queue`,
    call: (client) => client.removeFromQueue(TASK_ID, { message_id: "msg_user_03" }),
    ok: [200, undefined],
  },
  {
    name: "getConversationTurns",
    method: "GET",
    url: `${TASK_URL}/conversation/turns?before=40&limit=50`,
    call: (client) => client.getConversationTurns(TASK_ID, { before: 40, limit: 50 }),
    ok: [200, turnsPageBody()],
    required: {
      path: ["turns", 0, "revision_sequence"],
      named: '["turns"]["0"]["revision_sequence"]',
    },
  },
  {
    name: "getTurnActivity",
    method: "GET",
    url: `${TASK_URL}/conversation/turns/${TURN_ID}/activity?limit=200`,
    call: (client) => client.getTurnActivity(TASK_ID, TURN_ID, { limit: 200 }),
    ok: [200, activityPageBody()],
    required: { path: ["items", 2, "tool", "status"], named: '["items"]["2"]["tool"]["status"]' },
  },
  {
    name: "getLiveConversation",
    method: "GET",
    url: `${TASK_URL}/conversation/live?messageIds=${encodeURIComponent('["msg_live_01","msg_text_01"]')}`,
    call: (client) => client.getLiveConversation(TASK_ID, ["msg_live_01", "msg_text_01"]),
    ok: [200, liveSnapshotBody()],
    required: { path: ["resetSequence"], named: '["resetSequence"]' },
  },
  {
    name: "listSubagents",
    method: "GET",
    url: `${TASK_URL}/subagents`,
    call: (client) => client.listSubagents(TASK_ID),
    ok: [200, subagentsBody()],
    required: { path: ["subagents", 0, "status"], named: '["subagents"]["0"]["status"]' },
  },
  {
    name: "connectWorkspace",
    method: "POST",
    url: `${API_BASE_URL}/workspaces/${WORKSPACE_ID}/connect?start=false`,
    call: (client) => client.connectWorkspace(WORKSPACE_ID, { start: false }),
    ok: [200, connectRunningBody()],
    required: {
      path: ["transport", "websocket_path"],
      named: '["transport"]["websocket_path"]',
    },
  },
];

describe("Aether API client: every operation", () => {
  for (const operation of operations) {
    describe(operation.name, () => {
      it("sends the request to the pinned origin with the bearer key and decodes the answer", async () => {
        const [status, body] = operation.ok;
        const { client, requests } = clientWith(json(status, body));

        await succeed(operation.call(client));

        expect(requests).toHaveLength(1);
        const [request] = requests;
        expect(request?.method).toBe(operation.method);
        expect(String(request?.url)).toBe(operation.url);
        const headers = new Headers(request?.headers);
        expect(headers.get("authorization")).toBe(`Bearer ${API_KEY}`);
        expect(headers.get("accept")).toBe("application/json");
        expect(headers.get("content-type")).toBe(
          request?.body === undefined ? null : "application/json",
        );
        expect(request?.policy).toMatchObject({
          allowedOrigins: [API_BASE_URL],
          maxRedirects: 0,
          requirePublicAddress: true,
          allowLoopbackHttp: false,
        });
        expect(request?.signal).toBeInstanceOf(AbortSignal);
      });

      it("tolerates fields a newer server adds at any depth", async () => {
        const [status, body] = operation.ok;
        const { client } = clientWith(json(status, withAddedFields(body)));

        await succeed(operation.call(client));
      });

      if (operation.required !== undefined) {
        const required = operation.required;
        it(`rejects a response without a required field, naming ${required.named}`, async () => {
          const [status, body] = operation.ok;
          const { client } = clientWith(json(status, withoutKey(body ?? {}, required.path)));

          const error = await fail(operation.call(client));

          expect(error).toMatchObject({ _tag: "AetherApiDecodeError", status });
          expect(error._tag === "AetherApiDecodeError" && error.detail).toBe(
            `Missing key at ${required.named}`,
          );
        });
      }

      it("maps every error status to its typed error", async () => {
        const cases: ReadonlyArray<readonly [Reply, Json]> = [
          [json(401, errorBody("invalid api key")), { _tag: "AetherApiUnauthenticatedError" }],
          [
            json(402, admissionBody()),
            {
              _tag: "AetherApiPaymentRequiredError",
              code: "vm_size_not_allowed",
              tier: "free",
              vmSize: "large",
            },
          ],
          [
            json(403, orgBody("org_suspended")),
            { _tag: "AetherApiForbiddenError", reason: { kind: "org", code: "org_suspended" } },
          ],
          [json(404, errorBody("task not found")), { _tag: "AetherApiNotFoundError" }],
          [json(408, errorBody("slow upload")), { _tag: "AetherApiRequestTimeoutError" }],
          [json(413, errorBody("too large")), { _tag: "AetherApiPayloadTooLargeError" }],
          [json(400, errorBody("bad request")), { _tag: "AetherApiValidationError", status: 400 }],
          [
            json(422, {
              error: "validation failed",
              errors: [{ location: "body.prompt", message: "required", value: null }],
            }),
            {
              _tag: "AetherApiValidationError",
              status: 422,
              serverMessage: "validation failed",
              errors: [{ location: "body.prompt", message: "required" }],
            },
          ],
          [
            json(500, errorBody("boom")),
            { _tag: "AetherApiServerError", status: 500, serverMessage: "boom" },
          ],
          [json(503, errorBody("maintenance")), { _tag: "AetherApiServerError", status: 503 }],
          [
            json(429, errorBody("slow down")),
            { _tag: "AetherApiUnexpectedStatusError", status: 429 },
          ],
        ];
        for (const [reply, expected] of cases) {
          const { client } = clientWith(reply);
          const error = await fail(operation.call(client));
          expect(error, `${operation.name} → ${JSON.stringify(expected)}`).toMatchObject({
            ...expected,
            endpoint: expect.stringMatching(/^(GET|POST|PUT) \//u),
          });
        }
      });
    });
  }
});

describe("Aether API client: requests", () => {
  it("sends create, respond, update, stop and remove bodies exactly as given", async () => {
    const bodies: unknown[] = [];
    const { client } = clientWith((request) => {
      bodies.push(request.body === undefined ? undefined : JSON.parse(String(request.body)));
      return request.url.toString().endsWith("/tasks")
        ? json(202, { id: TASK_ID, name: "n" })
        : request.url.toString().endsWith("/respond")
          ? json(202, { message_id: "m" })
          : request.method === "PUT"
            ? json(200, taskBody("queued"))
            : json(200, undefined);
    });

    await succeed(client.createTask(createRequest));
    await succeed(client.respondToTask(TASK_ID, respondRequest));
    await succeed(client.updateTask(TASK_ID, updateRequest));
    await succeed(client.stopTask(TASK_ID, { discard_queued_messages: false }));
    await succeed(client.removeFromQueue(TASK_ID, { message_id: "msg_user_03" }));

    expect(bodies).toEqual([
      createRequest,
      respondRequest,
      updateRequest,
      { discard_queued_messages: false },
      { message_id: "msg_user_03" },
    ]);
  });

  it("asks connect to start the workspace only when told to", async () => {
    const { client, requests } = clientWith(json(200, connectConnectingBody()));

    const outcome = await succeed(client.connectWorkspace(WORKSPACE_ID, { start: true }));

    expect(String(requests[0]?.url)).toBe(
      `${API_BASE_URL}/workspaces/${WORKSPACE_ID}/connect?start=true`,
    );
    expect(outcome).toEqual({
      workspace_id: WORKSPACE_ID,
      state: "connecting",
      display_state: "starting",
      retry_after_ms: 1500,
    });
  });

  it("keeps a base path prefix and sends the turns page without a query when none is given", async () => {
    const http = fakeHttp(() => json(200, turnsPageBody()));
    const client = makeAetherApiClient({
      connection: Effect.succeed({
        apiBaseUrl: AetherApiBaseUrl.makeUnsafe("http://localhost:8787/aether/"),
        apiKey: API_KEY,
      }),
      request: http.request,
    });

    await succeed(client.getConversationTurns(TASK_ID));

    expect(String(http.requests[0]?.url)).toBe(
      `http://localhost:8787/aether/tasks/${TASK_ID}/conversation/turns`,
    );
    expect(http.requests[0]?.policy).toMatchObject({
      allowedOrigins: ["http://localhost:8787"],
      allowLoopbackHttp: true,
    });
  });

  it("refuses invalid requests before sending anything, naming the field", async () => {
    const { client, requests } = clientWith(json(200, {}));
    const table: ReadonlyArray<readonly [Effect.Effect<unknown, AetherApiError>, string]> = [
      [client.getTask("../profile"), "taskID: "],
      [client.getTask(TASK_ID.toUpperCase()), "taskID: "],
      [client.getTurnActivity(TASK_ID, "00000000-0000-0000-0000-000000000000"), "turnID: "],
      [client.getProject("not-a-uuid"), "id: "],
      [client.connectWorkspace("x/../../tasks", { start: false }), "workspaceID: "],
      [client.createTask({ ...createRequest, idempotency_key: "retry-1" }), '["idempotency_key"]'],
      [
        client.createTask({ ...createRequest, unknown_field: true } as AetherCreateTaskRequest),
        '["unknown_field"]',
      ],
      [
        client.respondToTask(TASK_ID, {
          message: "answer",
          // A digest without the tool id it fences matches no tool response variant.
          tool_response: {
            tool_name: "propose_plan",
            data: { approved: true },
            expected_input_digest: "b".repeat(64),
          } as unknown as AetherRespondToTaskRequest["tool_response"],
        }),
        '["tool_response"]',
      ],
      [client.respondToTask(TASK_ID, { message: "" }), '["message"]'],
      [
        client.updateTask(TASK_ID, {
          ...updateRequest,
          reasoning_effort: undefined,
        } as unknown as typeof updateRequest),
        '["reasoning_effort"]',
      ],
      [client.getConversationTurns(TASK_ID, { limit: 201 }), '["limit"]'],
      [client.getTurnActivity(TASK_ID, TURN_ID, { before: -1 }), '["before"]'],
      [
        client.getLiveConversation(
          TASK_ID,
          Array.from({ length: 201 }, (_, index) => `m${index}`),
        ),
        "messageIds: ",
      ],
      [client.getLiveConversation(TASK_ID, ["x".repeat(40_000)]), "messageIds: "],
    ];

    for (const [effect, named] of table) {
      const error = await fail(effect);
      expect(error._tag).toBe("AetherApiEncodeError");
      expect(error._tag === "AetherApiEncodeError" && error.detail).toContain(named);
    }
    expect(requests).toEqual([]);
  });
});

describe("Aether API client: decoding", () => {
  it("decodes the task read model without latest_sequence, and ignores it when sent", async () => {
    const { client } = clientWith(json(200, taskBody("processing", { latest_sequence: 99 })));

    const task = await succeed(client.getTask(TASK_ID));

    expect(task).not.toHaveProperty("latest_sequence");
    expect(task).toMatchObject({
      status: "processing",
      run_context: { workspace_id: WORKSPACE_ID },
      latest_message_revision: 17,
      activity_items: [{ type: "workspace_lifecycle" }],
    });
    // Fields Synara does not read are not decoded.
    expect(task).not.toHaveProperty("hardware");
  });

  it("decodes each task status variant", async () => {
    for (const status of ["queued", "processing", "awaiting_input", "errored"] as const) {
      const { client } = clientWith(json(200, taskBody(status)));
      const task = await succeed(client.getTask(TASK_ID));
      expect(task.status).toBe(status);
    }
  });

  it("keeps every key of a pending question's input, for the input digest", async () => {
    const { client } = clientWith(json(200, taskBody("awaiting_input")));

    const task = await succeed(client.getTask(TASK_ID));

    if (task.status !== "awaiting_input" || task.awaiting_input.kind !== "questions") {
      throw new Error("expected pending questions");
    }
    expect(task.awaiting_input.tool_id).toBe("toolu_01");
    expect(task.awaiting_input.input).toEqual({
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
    });
  });

  it("decodes turns with their state and revision, and tool rows with exitCode and durationMs", async () => {
    const turns = await succeed(
      clientWith(json(200, turnsPageBody())).client.getConversationTurns(TASK_ID),
    );
    expect(turns).toMatchObject({
      head_revision: 17,
      turns: [{ id: TURN_ID, state: "settled", revision_sequence: 9 }],
    });

    const activity = await succeed(
      clientWith(json(200, activityPageBody())).client.getTurnActivity(TASK_ID, TURN_ID),
    );
    expect(activity.items.map((item) => ("variant" in item ? item.variant : item.role))).toEqual([
      "text",
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

    const failedTool = await succeed(
      clientWith(
        json(
          200,
          activityPageBody({
            items: [toolMessageBody({ status: "output-error", exitCode: 2, error: "exit 2" })],
          }),
        ),
      ).client.getTurnActivity(TASK_ID, TURN_ID),
    );
    expect(failedTool.items[0]).toMatchObject({ tool: { exitCode: 2, error: "exit 2" } });
  });

  it("decodes the live snapshot's full accumulated content and reset sequence", async () => {
    const live = await succeed(
      clientWith(
        json(
          200,
          liveSnapshotBody({
            resetSequence: 4,
            replacements: [liveMessageBody({ id: "msg_old", variant: "thinking" })],
          }),
        ),
      ).client.getLiveConversation(TASK_ID, ["msg_old"]),
    );

    expect(live).toMatchObject({
      headRevision: 17,
      resetSequence: 4,
      open: [{ variant: "text", content: "Accumulated so far" }],
      replacements: [{ id: "msg_old", variant: "thinking" }],
    });
  });

  it("names the field when a discriminator has a value the client does not know", async () => {
    const cases: ReadonlyArray<
      readonly [
        (client: AetherApiClientShape) => Effect.Effect<unknown, AetherApiError>,
        Json,
        string,
      ]
    > = [
      [
        (client) => client.getTask(TASK_ID),
        taskBody("processing", { status: "archived" }),
        'Expected "queued" | "processing" | "awaiting_input" | "errored", got "archived" at ["status"]',
      ],
      [
        (client) => client.getTask(TASK_ID),
        taskBody("awaiting_input", { awaiting_input: { kind: "form", tool_id: "t" } }),
        'Expected "message" | "questions" | "plan", got "form" at ["awaiting_input"]["kind"]',
      ],
      [
        (client) => client.getConversationTurns(TASK_ID),
        turnsPageBody({ turns: [turnBody({ state: "paused" })] }),
        'Expected "queued" | "running" | "awaiting_input" | "settled" | "errored", got "paused" at ["turns"]["0"]["state"]',
      ],
      [
        (client) => client.getConversationTurns(TASK_ID),
        turnsPageBody({ task: { status: "archived" } }),
        'Expected "queued" | "processing" | "awaiting_input" | "errored", got "archived" at ["task"]["status"]',
      ],
      [
        (client) => client.getTurnActivity(TASK_ID, TURN_ID),
        activityPageBody({ items: [{ ...toolMessageBody(), variant: "image" }] }),
        'Expected "text" | "thinking" | "tool" | "seam", got "image" at ["items"]["0"]["variant"]',
      ],
      [
        (client) => client.getTurnActivity(TASK_ID, TURN_ID),
        activityPageBody({ items: [toolMessageBody({ status: "running" })] }),
        '["items"]["0"]["tool"]["status"]',
      ],
      [
        (client) => client.getLiveConversation(TASK_ID, []),
        liveSnapshotBody({ open: [liveMessageBody({ variant: "image" })] }),
        'Expected "text" | "thinking", got "image" at ["open"]["0"]["variant"]',
      ],
      [
        (client) => client.connectWorkspace(WORKSPACE_ID, { start: false }),
        { ...connectRunningBody(), state: "hibernating" },
        'Expected "running" | "connecting", got "hibernating" at ["state"]',
      ],
    ];
    for (const [call, body, detail] of cases) {
      const error = await fail(call(clientWith(json(200, body)).client));
      expect(error._tag).toBe("AetherApiDecodeError");
      expect(error._tag === "AetherApiDecodeError" && error.detail).toContain(detail);
    }
  });

  it("reports a 2xx body that is not JSON as a decode error", async () => {
    const error = await fail(
      clientWith(raw(200, "<html>ok</html>", "text/html")).client.getProfile(),
    );

    expect(error).toMatchObject({
      _tag: "AetherApiDecodeError",
      status: 200,
      detail: "Response body is not valid JSON.",
    });
  });

  it("keeps a 5xx from a gateway a server error even when its body is not JSON", async () => {
    const error = await fail(
      clientWith(raw(502, "<html>Bad Gateway</html>", "text/html")).client.getTask(TASK_ID),
    );

    expect(error).toEqual(expect.objectContaining({ _tag: "AetherApiServerError", status: 502 }));
    expect(error).not.toHaveProperty("serverMessage");
  });
});

describe("Aether API client: 403 and 402 bodies", () => {
  it("tells runtime admission from organization refusals where both are declared", async () => {
    const admission = await fail(
      clientWith(json(403, admissionBody())).client.createTask(createRequest),
    );
    expect(admission).toMatchObject({
      _tag: "AetherApiForbiddenError",
      reason: { kind: "admission", code: "vm_size_not_allowed", tier: "free", vmSize: "large" },
      serverMessage: "Your plan does not include large workspaces.",
    });

    const org = await fail(
      clientWith(json(403, orgBody())).client.respondToTask(TASK_ID, respondRequest),
    );
    expect(org).toMatchObject({ reason: { kind: "org", code: "org_membership_required" } });

    const other = await fail(
      clientWith(json(403, errorBody("task is archived", "task_archived"))).client.updateTask(
        TASK_ID,
        updateRequest,
      ),
    );
    expect(other).toMatchObject({ reason: { kind: "other", code: "task_archived" } });
  });

  it("rejects a 403 body a read route does not declare, naming the field", async () => {
    const error = await fail(
      clientWith(json(403, errorBody("nope", "something_new"))).client.getTask(TASK_ID),
    );

    expect(error).toMatchObject({ _tag: "AetherApiDecodeError", status: 403 });
    expect(error._tag === "AetherApiDecodeError" && error.detail).toContain('at ["code"]');
  });

  it("rejects a 402 body without its admission fields", async () => {
    const error = await fail(
      clientWith(json(402, withoutKey(admissionBody(), ["vm_size"]))).client.createTask(
        createRequest,
      ),
    );

    expect(error).toMatchObject({
      _tag: "AetherApiDecodeError",
      status: 402,
      detail: 'Missing key at ["vm_size"]',
    });
  });
});

describe("Aether API client: 409 conflicts per endpoint", () => {
  it("create: idempotency_key_reused", async () => {
    const error = await fail(
      clientWith(
        json(409, errorBody("key reused for another request", "idempotency_key_reused")),
      ).client.createTask(createRequest),
    );

    expect(error).toMatchObject({
      _tag: "AetherApiCreateTaskConflictError",
      status: 409,
      conflict: { kind: "idempotency_key_reused" },
      serverMessage: "key reused for another request",
    });
  });

  it("create: any other 409 body is a decode error", async () => {
    const error = await fail(
      clientWith(json(409, errorBody("conflict", "other"))).client.createTask(createRequest),
    );

    expect(error).toMatchObject({ _tag: "AetherApiDecodeError", status: 409 });
    expect(error._tag === "AetherApiDecodeError" && error.detail).toContain('at ["code"]');
  });

  it("respond: stale_pending_input, task_creator_deleted, awaiting_input_kind and the generic body", async () => {
    const cases: ReadonlyArray<readonly [Json, Json]> = [
      [
        { error: "input changed", code: "stale_pending_input", current_tool_id: "toolu_09" },
        { kind: "stale_pending_input", currentToolId: "toolu_09" },
      ],
      [{ error: "input changed", code: "stale_pending_input" }, { kind: "stale_pending_input" }],
      [
        { error: "creator deleted", code: "task_creator_deleted" },
        { kind: "task_creator_deleted" },
      ],
      [
        { error: "task awaits answers", code: "awaiting_input", awaiting_input_kind: "questions" },
        { kind: "awaiting_input_kind", awaitingInputKind: "questions", code: "awaiting_input" },
      ],
      [
        { error: "busy", code: "task_busy" },
        { kind: "other", code: "task_busy" },
      ],
    ];
    for (const [body, conflict] of cases) {
      const error = await fail(
        clientWith(json(409, body)).client.respondToTask(TASK_ID, respondRequest),
      );
      expect(error).toMatchObject({ _tag: "AetherApiRespondConflictError", status: 409, conflict });
      expect(error._tag === "AetherApiRespondConflictError" && error.conflict).toEqual(conflict);
    }
  });

  it("respond: an unknown awaiting_input_kind is a decode error naming it", async () => {
    const error = await fail(
      clientWith(
        json(409, { error: "task awaits input", awaiting_input_kind: "form" }),
      ).client.respondToTask(TASK_ID, respondRequest),
    );

    expect(error).toMatchObject({
      _tag: "AetherApiDecodeError",
      status: 409,
      detail: 'Expected "message" | "questions" | "plan", got "form" at ["awaiting_input_kind"]',
    });
  });

  it("stop: queued_stop_requires_discard and the generic body", async () => {
    const queued = await fail(
      clientWith(
        json(409, errorBody("discard the queue to stop", "queued_stop_requires_discard")),
      ).client.stopTask(TASK_ID, { discard_queued_messages: false }),
    );
    expect(queued).toMatchObject({
      _tag: "AetherApiStopConflictError",
      conflict: { kind: "queued_stop_requires_discard" },
    });

    const other = await fail(
      clientWith(json(409, errorBody("already stopping"))).client.stopTask(TASK_ID, {
        discard_queued_messages: true,
      }),
    );
    expect(other).toMatchObject({
      _tag: "AetherApiStopConflictError",
      conflict: { kind: "other" },
    });
  });

  it("connect: transitional, startable, not_connectable and task_creator_deleted (which has no kind)", async () => {
    const cases: ReadonlyArray<readonly [Json, Json]> = [
      [
        {
          kind: "transitional",
          error: "suspending",
          workspace_id: WORKSPACE_ID,
          retry_after_ms: 2000,
        },
        { kind: "transitional", workspaceId: WORKSPACE_ID, retryAfterMs: 2000 },
      ],
      [
        { kind: "startable", error: "stopped", workspace_id: WORKSPACE_ID },
        { kind: "startable", workspaceId: WORKSPACE_ID },
      ],
      [
        {
          kind: "not_connectable",
          error: "deleted",
          workspace_id: WORKSPACE_ID,
          display_state: "deleted",
        },
        { kind: "not_connectable", workspaceId: WORKSPACE_ID, displayState: "deleted" },
      ],
      [
        { error: "creator deleted", code: "task_creator_deleted" },
        { kind: "task_creator_deleted" },
      ],
    ];
    for (const [body, conflict] of cases) {
      const error = await fail(
        clientWith(json(409, body)).client.connectWorkspace(WORKSPACE_ID, { start: false }),
      );
      expect(error).toMatchObject({ _tag: "AetherApiConnectConflictError", status: 409 });
      expect(error._tag === "AetherApiConnectConflictError" && error.conflict).toEqual(conflict);
    }
  });

  it("connect: an unknown conflict kind, or a body with neither kind nor code, is a decode error", async () => {
    const unknownKind = await fail(
      clientWith(
        json(409, { kind: "migrating", error: "moving", workspace_id: WORKSPACE_ID }),
      ).client.connectWorkspace(WORKSPACE_ID, { start: false }),
    );
    expect(unknownKind).toMatchObject({
      _tag: "AetherApiDecodeError",
      detail:
        'Expected "transitional" | "startable" | "not_connectable", got "migrating" at ["kind"]',
    });

    const neither = await fail(
      clientWith(json(409, errorBody("conflict"))).client.connectWorkspace(WORKSPACE_ID, {
        start: false,
      }),
    );
    expect(neither).toMatchObject({
      _tag: "AetherApiDecodeError",
      detail: 'Missing key at ["kind"]',
    });
  });

  it("a 409 from a route that declares none is an unexpected status", async () => {
    const error = await fail(clientWith(json(409, errorBody("conflict"))).client.getTask(TASK_ID));

    expect(error).toMatchObject({
      _tag: "AetherApiUnexpectedStatusError",
      status: 409,
      serverMessage: "conflict",
    });
  });
});

describe("Aether API client: transport", () => {
  it("maps outbound failures to transport and timeout errors with the cause attached", async () => {
    const timeout = new OutboundHttpError(
      "timeout",
      "Outbound request exceeded its 30000ms deadline.",
    );
    const cases: ReadonlyArray<readonly [Error, Json]> = [
      [timeout, { _tag: "AetherApiTimeoutError", timeoutMs: 30_000, cause: timeout }],
      [
        new OutboundPolicyError("private-address", "private"),
        { _tag: "AetherApiTransportError", reason: "policy" },
      ],
      [
        new OutboundHttpError("invalid-redirect", "redirect limit"),
        {
          _tag: "AetherApiTransportError",
          reason: "invalid-redirect",
          detail: "The Aether API answered with a redirect, which is never followed.",
        },
      ],
      [
        new OutboundHttpError("response-too-large", "too large"),
        { _tag: "AetherApiTransportError", reason: "response-too-large" },
      ],
      [
        new Error("connect ECONNREFUSED 10.0.0.1:443"),
        {
          _tag: "AetherApiTransportError",
          reason: "unknown",
          detail: "The request failed before a response arrived.",
        },
      ],
    ];
    for (const [thrown, expected] of cases) {
      const error = await fail(clientWith(thrown).client.getProfile());
      expect(error).toMatchObject({ endpoint: "GET /profile", ...expected });
    }
  });

  it("times out an exchange that never finishes and aborts the request", async () => {
    let signal: AbortSignal | undefined;
    const client = makeAetherApiClient({
      connection: Effect.succeed({ apiBaseUrl: API_BASE_URL, apiKey: API_KEY }),
      timeoutMs: 50,
      request: (request) => {
        signal = request.signal;
        return new Promise(() => undefined);
      },
    });

    const error = await fail(client.listProjects());

    expect(error).toMatchObject({
      _tag: "AetherApiTimeoutError",
      endpoint: "GET /projects",
      timeoutMs: 50,
    });
    expect(signal?.aborted).toBe(true);
  });

  it("sends nothing when the client is not configured", async () => {
    const http = fakeHttp(() => json(200, profileBody()));
    const client = makeAetherApiClient({
      connection: resolveAetherApiConnection({
        apiBaseUrl: Effect.succeed(API_BASE_URL),
        readApiKey: Effect.succeed(null),
      }),
      request: http.request,
    });

    const error = await fail(client.getProfile());

    expect(error).toBeInstanceOf(AetherApiNotConfiguredError);
    expect(error).toMatchObject({
      detail: "No Aether API key is saved. Add one in Settings → Providers.",
    });
    expect(http.requests).toEqual([]);

    const unreadable = await Effect.runPromiseExit(
      resolveAetherApiConnection({
        apiBaseUrl: Effect.succeed(API_BASE_URL),
        readApiKey: Effect.fail(new Error("EACCES")),
      }),
    );
    expect(Exit.isFailure(unreadable) && Cause.pretty(unreadable.cause)).toContain(
      "Could not read the saved Aether API key",
    );
  });

  it("reads the base URL from settings and the key from the secret store for each request", async () => {
    const http = fakeHttp(() => json(200, profileBody()));
    let secret: string | null = "first-key";
    const credentials = Layer.succeed(ProviderCredentials, {
      getSecret: () => Effect.sync(() => secret),
      replaceSecret: () => Effect.void,
      isSecretConfigured: () => Effect.succeed(true),
    });
    const settings = ServerSettingsService.layerTest({
      providers: {
        aether: { apiBaseUrl: AetherApiBaseUrl.makeUnsafe("https://aether.example.com") },
      },
    });

    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* AetherApiClient;
        yield* client.getProfile();
        secret = "rotated-key";
        yield* client.getProfile();
      }).pipe(
        Effect.provide(
          makeAetherApiClientLive({ request: http.request }).pipe(
            Layer.provide(Layer.merge(credentials, settings)),
          ),
        ),
      ),
    );

    expect(http.requests.map((request) => String(request.url))).toEqual([
      "https://aether.example.com/profile",
      "https://aether.example.com/profile",
    ]);
    expect(
      http.requests.map((request) => new Headers(request.headers).get("authorization")),
    ).toEqual(["Bearer first-key", "Bearer rotated-key"]);
  });
});

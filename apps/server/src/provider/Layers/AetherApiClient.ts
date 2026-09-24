/**
 * AetherApiClientLive - The Aether REST client over `outboundHttp`.
 *
 * Every request is origin-pinned, public-address-only (loopback only for an `http://localhost`
 * base URL), never follows a redirect, and is bounded in size and time. The base URL and API key
 * are read from settings and the secret store for each request. Nothing is retried: `POST /tasks`
 * carries `idempotency_key` and `POST .../respond` carries `client_message_id` so that callers
 * can.
 *
 * @module AetherApiClientLive
 */
import { resolveAetherApiUrl, type AetherApiBaseUrl } from "@synara/contracts";
import {
  decodeOutboundJson,
  OutboundHttpError,
  outboundHttp,
  type OutboundHttpRequest,
  type OutboundHttpResponse,
} from "@synara/shared/outboundHttp";
import { Effect, Layer, Option, Schema } from "effect";

import { ProviderCredentials } from "../../providerCredentials.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import {
  AetherApiConflictError,
  AetherApiDecodeError,
  AetherApiHttpError,
  AetherApiNotConfiguredError,
  AetherApiPaymentRequiredError,
  AetherApiTimeoutError,
  AetherApiTransportError,
  type AetherApiError,
} from "../aether/aetherApiErrors.ts";
import {
  AetherAdmissionErrorBody,
  AetherConflictBodies,
  AetherConversationTurnsPage,
  AetherCreateTaskResponse,
  AetherErrorBody,
  AetherLiveConversationSnapshot,
  AetherProfile,
  AetherProjectList,
  AetherRespondToTaskResponse,
  AetherTurnActivityPage,
  AetherWorkspaceConnect,
  type AetherConflictBody,
} from "../aether/aetherApiSchemas.ts";
import {
  AetherApiClient,
  type AetherApiClientShape,
  type AetherApiConnection,
  type AetherPageQuery,
} from "../Services/AetherApiClient.ts";

/** One deadline per request, enforced by outboundHttp over DNS, connect and the whole body. */
export const AETHER_API_TIMEOUT_MS = 30_000;
const MAX_BODY_BYTES = 32 * 1024 * 1024;
const JSON_LIMITS = { maxDepth: 64, maxNodes: 1_000_000 } as const;

/** The transport: `outboundHttp.request` in production, a fake in tests. */
export type AetherHttpRequest = (request: OutboundHttpRequest) => Promise<OutboundHttpResponse>;

export interface AetherApiClientOptions {
  readonly apiBaseUrl: Effect.Effect<AetherApiBaseUrl, unknown>;
  /** `null` when no key is saved. */
  readonly readApiKey: Effect.Effect<string | null, unknown>;
  readonly request?: AetherHttpRequest;
}

type Decodable = Schema.Top & { readonly DecodingServices: never };
type Method = "GET" | "POST";

interface Exchange {
  readonly query?: Readonly<Record<string, string>>;
  readonly body?: unknown;
  /** The endpoint's 409 bodies; a 409 elsewhere is an `AetherApiHttpError`. */
  readonly conflict?: Decodable & { readonly Type: AetherConflictBody };
}

/** The only constructor. `AetherApiClientLive` calls it with settings and the secret store. */
export function makeAetherApiClient(options: AetherApiClientOptions): AetherApiClientShape {
  const request = options.request ?? ((input) => outboundHttp.request(input));

  const connection: Effect.Effect<AetherApiConnection, AetherApiNotConfiguredError> = Effect.gen(
    function* () {
      const apiBaseUrl = yield* options.apiBaseUrl.pipe(
        Effect.mapError(
          (cause) => new AetherApiNotConfiguredError({ reason: "unreadable-settings", cause }),
        ),
      );
      const apiKey = yield* options.readApiKey.pipe(
        Effect.mapError(
          (cause) => new AetherApiNotConfiguredError({ reason: "unreadable-api-key", cause }),
        ),
      );
      if (apiKey === null) {
        return yield* new AetherApiNotConfiguredError({ reason: "missing-api-key" });
      }
      return { apiBaseUrl, apiKey };
    },
  );

  /** Sends one request; resolves with the 2xx response, fails with every other outcome. */
  const send = (endpoint: string, method: Method, path: string, init: Exchange = {}) =>
    Effect.gen(function* () {
      const { apiBaseUrl, apiKey } = yield* connection;
      const url = resolveAetherApiUrl(apiBaseUrl, path);
      for (const [name, value] of Object.entries(init.query ?? {})) {
        url.searchParams.set(name, value);
      }
      const response = yield* Effect.tryPromise({
        try: (signal) =>
          request({
            policy: {
              service: "aether-api",
              allowedOrigins: [url.origin],
              timeoutMs: AETHER_API_TIMEOUT_MS,
              maxRequestBytes: MAX_BODY_BYTES,
              maxResponseBytes: MAX_BODY_BYTES,
              // The bearer key must never follow a redirect.
              maxRedirects: 0,
              maxConcurrent: 8,
              maxQueued: 64,
              requirePublicAddress: true,
              // The base URL schema admits `http:` only for loopback hosts; outboundHttp then
              // also checks that the host resolves to a loopback address.
              allowLoopbackHttp: url.protocol === "http:",
            },
            url,
            method,
            headers: {
              authorization: `Bearer ${apiKey}`,
              accept: "application/json",
              ...(init.body === undefined ? {} : { "content-type": "application/json" }),
            },
            ...(init.body === undefined ? {} : { body: JSON.stringify(init.body) }),
            signal,
          }),
        catch: (cause) =>
          cause instanceof OutboundHttpError && cause.code === "timeout"
            ? new AetherApiTimeoutError({ endpoint, timeoutMs: AETHER_API_TIMEOUT_MS })
            : new AetherApiTransportError({ endpoint, cause }),
      });
      const { status } = response;
      if (status >= 200 && status <= 299) return response;
      if (status === 402) {
        const body = yield* decodeBody(AetherAdmissionErrorBody, endpoint, response);
        return yield* new AetherApiPaymentRequiredError({
          endpoint,
          code: body.code,
          tier: body.tier,
          vmSize: body.vm_size,
          serverMessage: body.error,
        });
      }
      if (status === 409 && init.conflict !== undefined) {
        const body = yield* decodeBody(init.conflict, endpoint, response);
        return yield* new AetherApiConflictError({ endpoint, body });
      }
      // Absent when the body is not JSON or not an error body.
      const serverMessage = Option.liftThrowable(() =>
        decodeOutboundJson(response, JSON_LIMITS),
      )().pipe(Option.flatMap(Schema.decodeUnknownOption(AetherErrorBody)));
      return yield* new AetherApiHttpError({
        endpoint,
        status,
        ...(Option.isSome(serverMessage) ? { serverMessage: serverMessage.value.error } : {}),
      });
    });

  const call = <S extends Decodable>(
    method: Method,
    endpoint: string,
    path: string,
    schema: S,
    init?: Exchange,
  ): Effect.Effect<S["Type"], AetherApiError> =>
    send(endpoint, method, path, init).pipe(
      Effect.flatMap((response) => decodeBody(schema, endpoint, response)),
    );

  return {
    connection,
    getProfile: () => call("GET", "GET /profile", "/profile", AetherProfile),
    listProjects: () =>
      call("GET", "GET /projects", "/projects", AetherProjectList).pipe(
        Effect.map((list) => list.projects),
      ),
    createTask: (body) =>
      call("POST", "POST /tasks", "/tasks", AetherCreateTaskResponse, {
        body,
        conflict: AetherConflictBodies.createTask,
      }),
    respondToTask: (taskId, body) =>
      call(
        "POST",
        "POST /tasks/{taskID}/respond",
        `/tasks/${taskId}/respond`,
        AetherRespondToTaskResponse,
        {
          body,
          conflict: AetherConflictBodies.respondToTask,
        },
      ),
    // Both answer 200 with an empty body.
    stopTask: (taskId, body) =>
      send("POST /tasks/{taskID}/stop", "POST", `/tasks/${taskId}/stop`, {
        body,
        conflict: AetherConflictBodies.stopTask,
      }).pipe(Effect.asVoid),
    removeFromQueue: (taskId, messageId) =>
      send("POST /tasks/{taskID}/remove-from-queue", "POST", `/tasks/${taskId}/remove-from-queue`, {
        body: { message_id: messageId },
      }).pipe(Effect.asVoid),
    getConversationTurns: (taskId, page) =>
      call(
        "GET",
        "GET /tasks/{taskID}/conversation/turns",
        `/tasks/${taskId}/conversation/turns`,
        AetherConversationTurnsPage,
        { query: pageQuery(page) },
      ),
    getTurnActivity: (taskId, turnId, page) =>
      call(
        "GET",
        "GET /tasks/{taskID}/conversation/turns/{turnID}/activity",
        `/tasks/${taskId}/conversation/turns/${turnId}/activity`,
        AetherTurnActivityPage,
        { query: pageQuery(page) },
      ),
    getLiveConversation: (taskId, messageIds) =>
      call(
        "GET",
        "GET /tasks/{taskID}/conversation/live",
        `/tasks/${taskId}/conversation/live`,
        AetherLiveConversationSnapshot,
        { query: { messageIds: JSON.stringify(messageIds) } },
      ),
    connectWorkspace: (workspaceId, { start }) =>
      call(
        "POST",
        "POST /workspaces/{workspaceID}/connect",
        `/workspaces/${workspaceId}/connect`,
        AetherWorkspaceConnect,
        { query: { start: String(start) }, conflict: AetherConflictBodies.connectWorkspace },
      ),
  };
}

function pageQuery(page: AetherPageQuery | undefined): Record<string, string> {
  return {
    ...(page?.before === undefined ? {} : { before: String(page.before) }),
    ...(page?.limit === undefined ? {} : { limit: String(page.limit) }),
  };
}

/** Parses and decodes a JSON body; a body that is not JSON is a decode error too. */
function decodeBody<S extends Decodable>(
  schema: S,
  endpoint: string,
  response: OutboundHttpResponse,
): Effect.Effect<S["Type"], AetherApiDecodeError> {
  const { status } = response;
  return Effect.try({
    try: () => decodeOutboundJson(response, JSON_LIMITS),
    catch: (cause) =>
      new AetherApiDecodeError({ endpoint, status, detail: "The body is not valid JSON.", cause }),
  }).pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(schema)),
    Effect.mapError((cause) =>
      cause instanceof AetherApiDecodeError
        ? cause
        : new AetherApiDecodeError({ endpoint, status, detail: cause.message, cause }),
    ),
  );
}

export const AetherApiClientLive = Layer.effect(
  AetherApiClient,
  Effect.gen(function* () {
    const serverSettings = yield* ServerSettingsService;
    const credentials = yield* ProviderCredentials;
    return makeAetherApiClient({
      apiBaseUrl: serverSettings.getSettings.pipe(
        Effect.map((settings) => settings.providers.aether.apiBaseUrl),
      ),
      readApiKey: credentials.getSecret("aether"),
    });
  }),
);

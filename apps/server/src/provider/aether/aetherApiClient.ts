// FILE: aetherApiClient.ts
// Purpose: Typed Effect client for the Aether REST API (profile, projects, tasks, conversation
// turns/activity/live, subagents, workspace connect).
// Layer: Server provider boundary (Aether). Every request goes through `outboundHttp`
// (origin-pinned, public-address-only, no redirects, bounded bodies) with the API key from the
// server secret store; every response is decoded at this boundary.
//
// No request is retried here. `POST /tasks` carries an optional `idempotency_key` and
// `POST /tasks/{taskID}/respond` a `client_message_id` precisely so a caller can retry them.

import { resolveAetherApiUrl, type AetherApiBaseUrl } from "@synara/contracts";
import {
  decodeOutboundJson,
  OutboundHttpError,
  outboundHttp,
  type OutboundHttpRequest,
  type OutboundHttpResponse,
} from "@synara/shared/outboundHttp";
import { OutboundPolicyError } from "@synara/shared/outboundHttpPolicy";
import { Effect, Layer, Schema, ServiceMap } from "effect";

import { ProviderCredentials } from "../../providerCredentials.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import {
  AetherApiConnectConflictError,
  AetherApiCreateTaskConflictError,
  AetherApiDecodeError,
  AetherApiEncodeError,
  AetherApiForbiddenError,
  AetherApiNotConfiguredError,
  AetherApiNotFoundError,
  AetherApiPayloadTooLargeError,
  AetherApiPaymentRequiredError,
  AetherApiRequestTimeoutError,
  AetherApiRespondConflictError,
  AetherApiServerError,
  AetherApiStopConflictError,
  AetherApiTimeoutError,
  AetherApiTransportError,
  AetherApiUnauthenticatedError,
  AetherApiUnexpectedStatusError,
  AetherApiValidationError,
  formatAetherSchemaIssue,
  type AetherApiRequestError,
} from "./aetherApiErrors.ts";
import {
  AETHER_LIVE_MESSAGE_IDS_MAX_LENGTH,
  AetherAdmissionErrorBody,
  AetherAwaitingInputConflictBody,
  AetherConnectConflictBody,
  AetherConversationTurnsPage,
  AetherCreateTaskRequest,
  AetherCreateTaskResponse,
  AetherErrorBody,
  AetherIdempotencyKeyReusedBody,
  AetherLiveConversationSnapshot,
  AetherLiveMessageIds,
  AetherOrgAuthorizationErrorBody,
  AetherPageQuery,
  AetherProfile,
  AetherProject,
  AetherProjectIdParam,
  AetherProjectList,
  AetherQueuedStopRequiresDiscardBody,
  AetherRemoveFromQueueRequest,
  AetherRespondToTaskRequest,
  AetherRespondToTaskResponse,
  AetherStalePendingInputBody,
  AetherStopTaskRequest,
  AetherTaskCreatorDeletedBody,
  AetherTaskIdParam,
  AetherTaskSubagents,
  AetherTaskWithActivity,
  AetherTurnActivityPage,
  AetherTurnIdParam,
  AetherUpdateTaskRequest,
  AetherWorkspaceConnect,
  AetherWorkspaceIdParam,
  type AetherConversationTurnsPage as AetherConversationTurnsPageType,
  type AetherCreateTaskRequest as AetherCreateTaskRequestType,
  type AetherCreateTaskResponse as AetherCreateTaskResponseType,
  type AetherLiveConversationSnapshot as AetherLiveConversationSnapshotType,
  type AetherPageQuery as AetherPageQueryType,
  type AetherProfile as AetherProfileType,
  type AetherProject as AetherProjectType,
  type AetherRemoveFromQueueRequest as AetherRemoveFromQueueRequestType,
  type AetherRespondToTaskRequest as AetherRespondToTaskRequestType,
  type AetherRespondToTaskResponse as AetherRespondToTaskResponseType,
  type AetherStopTaskRequest as AetherStopTaskRequestType,
  type AetherTaskSubagents as AetherTaskSubagentsType,
  type AetherTaskWithActivity as AetherTaskWithActivityType,
  type AetherTurnActivityPage as AetherTurnActivityPageType,
  type AetherUpdateTaskRequest as AetherUpdateTaskRequestType,
  type AetherWorkspaceConnect as AetherWorkspaceConnectType,
} from "./aetherApiSchemas.ts";

export const AETHER_API_TIMEOUT_MS = 30_000;
const MAX_REQUEST_BYTES = 32 * 1024 * 1024;
const MAX_RESPONSE_BYTES = 32 * 1024 * 1024;
const JSON_LIMITS = { maxDepth: 64, maxNodes: 1_000_000 } as const;

/** The transport seam: `outboundHttp.request` in production, a fake in tests. */
export type AetherHttpRequest = (request: OutboundHttpRequest) => Promise<OutboundHttpResponse>;

const defaultAetherHttpRequest: AetherHttpRequest = (request) => outboundHttp.request(request);

/** Where to reach Aether and the key to send. Resolved for every request. */
export interface AetherApiConnection {
  readonly apiBaseUrl: AetherApiBaseUrl;
  readonly apiKey: string;
}

export interface AetherApiClientOptions {
  readonly connection: Effect.Effect<AetherApiConnection, AetherApiNotConfiguredError>;
  readonly request?: AetherHttpRequest;
  /** Deadline for one whole exchange: request, response body and decode. */
  readonly timeoutMs?: number;
}

export interface AetherApiClientShape {
  /** The resolved base URL and key, for the workspace socket that shares this client's config. */
  readonly connection: Effect.Effect<AetherApiConnection, AetherApiNotConfiguredError>;
  /** `GET /profile`. */
  readonly getProfile: () => Effect.Effect<AetherProfileType, AetherApiRequestError>;
  /** `GET /projects`: the projects linked to the key's account. */
  readonly listProjects: () => Effect.Effect<
    ReadonlyArray<AetherProjectType>,
    AetherApiRequestError
  >;
  /** `GET /projects/{id}`. */
  readonly getProject: (
    projectId: string,
  ) => Effect.Effect<AetherProjectType, AetherApiRequestError>;
  /** `POST /tasks` (202). */
  readonly createTask: (
    request: AetherCreateTaskRequestType,
  ) => Effect.Effect<
    AetherCreateTaskResponseType,
    AetherApiRequestError | AetherApiCreateTaskConflictError
  >;
  /** `GET /tasks/{taskID}`. */
  readonly getTask: (
    taskId: string,
  ) => Effect.Effect<AetherTaskWithActivityType, AetherApiRequestError>;
  /** `PUT /tasks/{taskID}`: replaces every task setting. */
  readonly updateTask: (
    taskId: string,
    request: AetherUpdateTaskRequestType,
  ) => Effect.Effect<AetherTaskWithActivityType, AetherApiRequestError>;
  /** `POST /tasks/{taskID}/respond` (202). */
  readonly respondToTask: (
    taskId: string,
    request: AetherRespondToTaskRequestType,
  ) => Effect.Effect<
    AetherRespondToTaskResponseType,
    AetherApiRequestError | AetherApiRespondConflictError
  >;
  /** `POST /tasks/{taskID}/stop`. */
  readonly stopTask: (
    taskId: string,
    request: AetherStopTaskRequestType,
  ) => Effect.Effect<void, AetherApiRequestError | AetherApiStopConflictError>;
  /** `POST /tasks/{taskID}/remove-from-queue`. */
  readonly removeFromQueue: (
    taskId: string,
    request: AetherRemoveFromQueueRequestType,
  ) => Effect.Effect<void, AetherApiRequestError>;
  /** `GET /tasks/{taskID}/conversation/turns`: newest page, or the page before `before`. */
  readonly getConversationTurns: (
    taskId: string,
    query?: AetherPageQueryType,
  ) => Effect.Effect<AetherConversationTurnsPageType, AetherApiRequestError>;
  /** `GET /tasks/{taskID}/conversation/turns/{turnID}/activity`. */
  readonly getTurnActivity: (
    taskId: string,
    turnId: string,
    query?: AetherPageQueryType,
  ) => Effect.Effect<AetherTurnActivityPageType, AetherApiRequestError>;
  /**
   * `GET /tasks/{taskID}/conversation/live`: the full accumulated content of the open messages,
   * plus replacements for previously observed ids (at most 200) that have since closed.
   */
  readonly getLiveConversation: (
    taskId: string,
    messageIds: ReadonlyArray<string>,
  ) => Effect.Effect<AetherLiveConversationSnapshotType, AetherApiRequestError>;
  /** `GET /tasks/{taskID}/subagents`: provider-native subagents, not Aether tasks. */
  readonly listSubagents: (
    taskId: string,
  ) => Effect.Effect<AetherTaskSubagentsType, AetherApiRequestError>;
  /**
   * `POST /workspaces/{workspaceID}/connect`. Only `start: true` may boot a stopped workspace;
   * `start: false` attaches to one that is already usable, and answers 409 otherwise.
   */
  readonly connectWorkspace: (
    workspaceId: string,
    options: { readonly start: boolean },
  ) => Effect.Effect<
    AetherWorkspaceConnectType,
    AetherApiRequestError | AetherApiConnectConflictError
  >;
}

export class AetherApiClient extends ServiceMap.Service<AetherApiClient, AetherApiClientShape>()(
  "synara/provider/aether/AetherApiClient",
) {}

/** Reads the configured base URL and the saved API key, failing when either is unavailable. */
export const resolveAetherApiConnection = (input: {
  readonly apiBaseUrl: Effect.Effect<AetherApiBaseUrl, unknown>;
  readonly readApiKey: Effect.Effect<string | null, unknown>;
}): Effect.Effect<AetherApiConnection, AetherApiNotConfiguredError> =>
  Effect.gen(function* () {
    const apiBaseUrl = yield* input.apiBaseUrl.pipe(
      Effect.mapError(
        (cause) =>
          new AetherApiNotConfiguredError({
            detail: "Could not read the Aether API base URL from Synara's settings.",
            cause,
          }),
      ),
    );
    const apiKey = yield* input.readApiKey.pipe(
      Effect.mapError(
        (cause) =>
          new AetherApiNotConfiguredError({
            detail: "Could not read the saved Aether API key from Synara's secret store.",
            cause,
          }),
      ),
    );
    if (apiKey === null) {
      return yield* new AetherApiNotConfiguredError({
        detail: "No Aether API key is saved. Add one in Settings → Providers.",
      });
    }
    return { apiBaseUrl, apiKey };
  });

// ---------------------------------------------------------------------------
// Exchange
// ---------------------------------------------------------------------------

/** Which 403 bodies a route declares. */
type ForbiddenBodies = "org" | "org-or-error" | "admission-org-or-error";

interface ExchangeSpec<A, C = never> {
  /** Method and path template, e.g. `GET /tasks/{taskID}`; the stable name used in errors. */
  readonly endpoint: string;
  readonly method: "GET" | "POST" | "PUT";
  /** Path with already-validated parameters. */
  readonly path: string;
  readonly query?: ReadonlyArray<readonly [string, string]>;
  readonly body?: string;
  /** Decodes a 2xx body; absent when the route answers without one. */
  readonly decode?: DecodeAt<A>;
  readonly forbidden: ForbiddenBodies;
  /** Decodes a 409 body into the route's conflict error; absent when 409 is undeclared. */
  readonly conflict?: (
    body: unknown,
    status: number,
  ) => Effect.Effect<never, C | AetherApiDecodeError>;
}

type DecodeAt<A> = (
  endpoint: string,
  status: number,
) => (input: unknown) => Effect.Effect<A, AetherApiDecodeError>;

const decodeAt =
  <S extends Schema.Top & { readonly DecodingServices: never }>(schema: S): DecodeAt<S["Type"]> =>
  (endpoint, status) =>
  (input) =>
    Schema.decodeUnknownEffect(schema)(input).pipe(
      Effect.mapError(
        (cause) =>
          new AetherApiDecodeError({
            endpoint,
            status,
            detail: formatAetherSchemaIssue(cause.issue),
            cause,
          }),
      ),
    );

const encodeAt =
  <S extends Schema.Top & { readonly EncodingServices: never }>(schema: S, label?: string) =>
  (endpoint: string, value: unknown): Effect.Effect<S["Encoded"], AetherApiEncodeError> =>
    Schema.encodeUnknownEffect(schema)(value, { onExcessProperty: "error" }).pipe(
      Effect.mapError(
        (cause) =>
          new AetherApiEncodeError({
            endpoint,
            detail: `${label === undefined ? "" : `${label}: `}${formatAetherSchemaIssue(cause.issue)}`,
            cause,
          }),
      ),
    );

function transportError(endpoint: string, cause: unknown, timeoutMs: number) {
  if (cause instanceof OutboundHttpError && cause.code === "timeout") {
    return new AetherApiTimeoutError({ endpoint, timeoutMs, cause });
  }
  if (cause instanceof OutboundPolicyError) {
    return new AetherApiTransportError({
      endpoint,
      reason: "policy",
      detail: `The outbound policy refused the Aether API address (${cause.code}).`,
      cause,
    });
  }
  const reason = cause instanceof OutboundHttpError ? cause.code : "unknown";
  return new AetherApiTransportError({
    endpoint,
    reason,
    detail:
      reason === "invalid-redirect"
        ? "The Aether API answered with a redirect, which is never followed."
        : reason === "response-too-large"
          ? "The Aether API response exceeded the size limit."
          : "The request failed before a response arrived.",
    cause,
  });
}

type ParsedBody =
  | { readonly _tag: "Json"; readonly value: unknown }
  | { readonly _tag: "Invalid"; readonly error: AetherApiDecodeError };

/** Parses a JSON body; a body that is not JSON (or exceeds the limits) is a decode error. */
function parseBody(endpoint: string, response: OutboundHttpResponse): ParsedBody {
  try {
    return { _tag: "Json", value: decodeOutboundJson(response, JSON_LIMITS) };
  } catch (cause) {
    return {
      _tag: "Invalid",
      error: new AetherApiDecodeError({
        endpoint,
        status: response.status,
        detail:
          cause instanceof OutboundPolicyError
            ? `Response body exceeded the JSON limits (${cause.code}).`
            : "Response body is not valid JSON.",
        cause,
      }),
    };
  }
}

function readJson(
  endpoint: string,
  response: OutboundHttpResponse,
): Effect.Effect<unknown, AetherApiDecodeError> {
  const parsed = parseBody(endpoint, response);
  return parsed._tag === "Json" ? Effect.succeed(parsed.value) : Effect.fail(parsed.error);
}

/** The generic error body's fields, when an error response carries one. */
function readErrorBody(body: unknown): typeof AetherErrorBody.Type | undefined {
  const result = Schema.decodeUnknownExit(AetherErrorBody)(body);
  return result._tag === "Success" ? result.value : undefined;
}

function optionalMessage(body: unknown): { readonly serverMessage?: string } {
  const error = readErrorBody(body)?.error;
  return error === undefined ? {} : { serverMessage: error };
}

function forbiddenError(
  endpoint: string,
  policy: ForbiddenBodies,
  body: unknown,
): Effect.Effect<never, AetherApiForbiddenError | AetherApiDecodeError> {
  return Effect.gen(function* () {
    if (policy === "admission-org-or-error") {
      const admission = Schema.decodeUnknownExit(AetherAdmissionErrorBody)(body);
      if (admission._tag === "Success") {
        const { code, tier, vm_size, error } = admission.value;
        return yield* new AetherApiForbiddenError({
          endpoint,
          status: 403,
          reason: { kind: "admission", code, tier, vmSize: vm_size },
          serverMessage: error,
        });
      }
    }
    if (policy !== "org") {
      const org = Schema.decodeUnknownExit(AetherOrgAuthorizationErrorBody)(body);
      if (org._tag === "Success") {
        return yield* new AetherApiForbiddenError({
          endpoint,
          status: 403,
          reason: { kind: "org", code: org.value.code },
          serverMessage: org.value.error,
        });
      }
      const generic = readErrorBody(body);
      if (generic !== undefined) {
        return yield* new AetherApiForbiddenError({
          endpoint,
          status: 403,
          reason: {
            kind: "other",
            ...(generic.code === undefined ? {} : { code: generic.code }),
          },
          serverMessage: generic.error,
        });
      }
    }
    // Only the organization body is declared (or nothing else matched): decode it strictly so
    // the failure names the field.
    const org = yield* decodeAt(AetherOrgAuthorizationErrorBody)(endpoint, 403)(body);
    return yield* new AetherApiForbiddenError({
      endpoint,
      status: 403,
      reason: { kind: "org", code: org.code },
      serverMessage: org.error,
    });
  });
}

function statusError<C>(
  spec: ExchangeSpec<unknown, C>,
  response: OutboundHttpResponse,
): Effect.Effect<never, AetherApiRequestError | C> {
  const { endpoint } = spec;
  const status = response.status;
  // Parsed once. Statuses decided by their code alone use it only for `serverMessage`; the
  // bodies the client branches on (402, 403, 409) must parse, or the error is a decode error.
  const parsed = parseBody(endpoint, response);
  const json: Effect.Effect<unknown, AetherApiDecodeError> =
    parsed._tag === "Json" ? Effect.succeed(parsed.value) : Effect.fail(parsed.error);
  const body = parsed._tag === "Json" ? parsed.value : undefined;
  switch (status) {
    case 401:
      return Effect.fail(
        new AetherApiUnauthenticatedError({ endpoint, status, ...optionalMessage(body) }),
      );
    case 402:
      return json.pipe(
        Effect.flatMap(decodeAt(AetherAdmissionErrorBody)(endpoint, status)),
        Effect.flatMap((admission) =>
          Effect.fail(
            new AetherApiPaymentRequiredError({
              endpoint,
              status,
              code: admission.code,
              tier: admission.tier,
              vmSize: admission.vm_size,
              serverMessage: admission.error,
            }),
          ),
        ),
      );
    case 403:
      return json.pipe(
        Effect.flatMap((decoded) => forbiddenError(endpoint, spec.forbidden, decoded)),
      );
    case 404:
      return Effect.fail(
        new AetherApiNotFoundError({ endpoint, status, ...optionalMessage(body) }),
      );
    case 408:
      return Effect.fail(
        new AetherApiRequestTimeoutError({ endpoint, status, ...optionalMessage(body) }),
      );
    case 409:
      if (spec.conflict !== undefined) {
        const decodeConflict = spec.conflict;
        return json.pipe(Effect.flatMap((decoded) => decodeConflict(decoded, status)));
      }
      break;
    case 413:
      return Effect.fail(
        new AetherApiPayloadTooLargeError({ endpoint, status, ...optionalMessage(body) }),
      );
    case 400:
    case 422: {
      const generic = readErrorBody(body);
      return Effect.fail(
        new AetherApiValidationError({
          endpoint,
          status,
          ...(generic === undefined ? {} : { serverMessage: generic.error }),
          errors: generic?.errors ?? [],
        }),
      );
    }
    default:
      if (status >= 500 && status <= 599) {
        return Effect.fail(
          new AetherApiServerError({ endpoint, status, ...optionalMessage(body) }),
        );
      }
  }
  return Effect.fail(
    new AetherApiUnexpectedStatusError({ endpoint, status, ...optionalMessage(body) }),
  );
}

// ---------------------------------------------------------------------------
// 409 bodies
// ---------------------------------------------------------------------------

const codeOf = (body: unknown): unknown =>
  typeof body === "object" && body !== null && !Array.isArray(body)
    ? (body as Record<string, unknown>).code
    : undefined;
const hasKey = (body: unknown, key: string): boolean =>
  typeof body === "object" && body !== null && !Array.isArray(body) && Object.hasOwn(body, key);

const createTaskConflict = (endpoint: string) => (body: unknown, status: number) =>
  decodeAt(AetherIdempotencyKeyReusedBody)(
    endpoint,
    status,
  )(body).pipe(
    Effect.flatMap((reused) =>
      Effect.fail(
        new AetherApiCreateTaskConflictError({
          endpoint,
          status: 409,
          conflict: { kind: reused.code },
          serverMessage: reused.error,
        }),
      ),
    ),
  );

const respondConflict =
  (endpoint: string) =>
  (
    body: unknown,
    status: number,
  ): Effect.Effect<never, AetherApiRespondConflictError | AetherApiDecodeError> => {
    const fail = (conflict: AetherApiRespondConflictError["conflict"], serverMessage: string) =>
      Effect.fail(
        new AetherApiRespondConflictError({ endpoint, status: 409, conflict, serverMessage }),
      );
    if (hasKey(body, "awaiting_input_kind")) {
      return decodeAt(AetherAwaitingInputConflictBody)(
        endpoint,
        status,
      )(body).pipe(
        Effect.flatMap((decoded) =>
          fail(
            {
              kind: "awaiting_input_kind",
              awaitingInputKind: decoded.awaiting_input_kind,
              ...(decoded.code === undefined ? {} : { code: decoded.code }),
            },
            decoded.error,
          ),
        ),
      );
    }
    switch (codeOf(body)) {
      case "stale_pending_input":
        return decodeAt(AetherStalePendingInputBody)(
          endpoint,
          status,
        )(body).pipe(
          Effect.flatMap((decoded) =>
            fail(
              {
                kind: "stale_pending_input",
                ...(decoded.current_tool_id === undefined
                  ? {}
                  : { currentToolId: decoded.current_tool_id }),
              },
              decoded.error,
            ),
          ),
        );
      case "task_creator_deleted":
        return decodeAt(AetherTaskCreatorDeletedBody)(
          endpoint,
          status,
        )(body).pipe(Effect.flatMap((decoded) => fail({ kind: decoded.code }, decoded.error)));
      default:
        return decodeAt(AetherErrorBody)(
          endpoint,
          status,
        )(body).pipe(
          Effect.flatMap((decoded) =>
            fail(
              { kind: "other", ...(decoded.code === undefined ? {} : { code: decoded.code }) },
              decoded.error,
            ),
          ),
        );
    }
  };

const stopConflict =
  (endpoint: string) =>
  (
    body: unknown,
    status: number,
  ): Effect.Effect<never, AetherApiStopConflictError | AetherApiDecodeError> => {
    const fail = (conflict: AetherApiStopConflictError["conflict"], serverMessage: string) =>
      Effect.fail(
        new AetherApiStopConflictError({ endpoint, status: 409, conflict, serverMessage }),
      );
    return codeOf(body) === "queued_stop_requires_discard"
      ? decodeAt(AetherQueuedStopRequiresDiscardBody)(
          endpoint,
          status,
        )(body).pipe(Effect.flatMap((decoded) => fail({ kind: decoded.code }, decoded.error)))
      : decodeAt(AetherErrorBody)(
          endpoint,
          status,
        )(body).pipe(
          Effect.flatMap((decoded) =>
            fail(
              { kind: "other", ...(decoded.code === undefined ? {} : { code: decoded.code }) },
              decoded.error,
            ),
          ),
        );
  };

const connectConflict =
  (endpoint: string) =>
  (
    body: unknown,
    status: number,
  ): Effect.Effect<never, AetherApiConnectConflictError | AetherApiDecodeError> => {
    const fail = (conflict: AetherApiConnectConflictError["conflict"], serverMessage: string) =>
      Effect.fail(
        new AetherApiConnectConflictError({ endpoint, status: 409, conflict, serverMessage }),
      );
    // `task_creator_deleted` is the one connect conflict with a `code` and no `kind`.
    if (!hasKey(body, "kind") && codeOf(body) === "task_creator_deleted") {
      return decodeAt(AetherTaskCreatorDeletedBody)(
        endpoint,
        status,
      )(body).pipe(Effect.flatMap((decoded) => fail({ kind: decoded.code }, decoded.error)));
    }
    return decodeAt(AetherConnectConflictBody)(
      endpoint,
      status,
    )(body).pipe(
      Effect.flatMap((decoded) => {
        switch (decoded.kind) {
          case "transitional":
            return fail(
              {
                kind: decoded.kind,
                workspaceId: decoded.workspace_id,
                retryAfterMs: decoded.retry_after_ms,
              },
              decoded.error,
            );
          case "startable":
            return fail({ kind: decoded.kind, workspaceId: decoded.workspace_id }, decoded.error);
          case "not_connectable":
            return fail(
              {
                kind: decoded.kind,
                workspaceId: decoded.workspace_id,
                displayState: decoded.display_state,
              },
              decoded.error,
            );
        }
      }),
    );
  };

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

export function makeAetherApiClient(options: AetherApiClientOptions): AetherApiClientShape {
  const request = options.request ?? defaultAetherHttpRequest;
  const timeoutMs = options.timeoutMs ?? AETHER_API_TIMEOUT_MS;

  const exchange = <A, C = never>(
    spec: ExchangeSpec<A, C>,
  ): Effect.Effect<A, AetherApiRequestError | C> =>
    Effect.gen(function* () {
      const { apiBaseUrl, apiKey } = yield* options.connection;
      const url = resolveAetherApiUrl(apiBaseUrl, spec.path);
      for (const [key, value] of spec.query ?? []) url.searchParams.set(key, value);
      const headers: Record<string, string> = {
        authorization: `Bearer ${apiKey}`,
        accept: "application/json",
        ...(spec.body === undefined ? {} : { "content-type": "application/json" }),
      };
      const response = yield* Effect.tryPromise({
        try: (signal) =>
          request({
            policy: {
              service: "aether-api",
              allowedOrigins: [url.origin],
              timeoutMs,
              maxRequestBytes: MAX_REQUEST_BYTES,
              maxResponseBytes: MAX_RESPONSE_BYTES,
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
            method: spec.method,
            headers,
            ...(spec.body === undefined ? {} : { body: spec.body }),
            signal,
          }),
        catch: (cause) => transportError(spec.endpoint, cause, timeoutMs),
      });
      if (response.status < 200 || response.status > 299) {
        return yield* statusError(spec, response);
      }
      if (spec.decode === undefined) {
        return undefined as A;
      }
      const json = yield* readJson(spec.endpoint, response);
      return yield* spec.decode(spec.endpoint, response.status)(json);
    }).pipe(
      Effect.timeoutOrElse({
        duration: timeoutMs,
        onTimeout: () =>
          Effect.fail(new AetherApiTimeoutError({ endpoint: spec.endpoint, timeoutMs })),
      }),
    );

  const taskPath = (endpoint: string, taskId: string) =>
    encodeAt(AetherTaskIdParam, "taskID")(endpoint, taskId).pipe(
      Effect.map((id) => `/tasks/${id}`),
    );

  const pageQuery = (endpoint: string, query: AetherPageQueryType | undefined) =>
    encodeAt(AetherPageQuery)(endpoint, query ?? {}).pipe(
      Effect.map(
        (encoded): ReadonlyArray<readonly [string, string]> => [
          ...(encoded.before === undefined ? [] : [["before", String(encoded.before)] as const]),
          ...(encoded.limit === undefined ? [] : [["limit", String(encoded.limit)] as const]),
        ],
      ),
    );

  const jsonBody = <S extends Schema.Top & { readonly EncodingServices: never }>(
    schema: S,
    endpoint: string,
    value: unknown,
  ) => encodeAt(schema)(endpoint, value).pipe(Effect.map((encoded) => JSON.stringify(encoded)));

  return {
    connection: options.connection,

    getProfile: () => {
      const endpoint = "GET /profile";
      return exchange({
        endpoint,
        method: "GET",
        path: "/profile",
        decode: decodeAt(AetherProfile),
        forbidden: "org",
      });
    },

    listProjects: () => {
      const endpoint = "GET /projects";
      return exchange({
        endpoint,
        method: "GET",
        path: "/projects",
        decode: decodeAt(AetherProjectList),
        forbidden: "org",
      }).pipe(Effect.map((list) => list.projects));
    },

    getProject: (projectId) => {
      const endpoint = "GET /projects/{id}";
      return encodeAt(AetherProjectIdParam, "id")(endpoint, projectId).pipe(
        Effect.flatMap((id) =>
          exchange({
            endpoint,
            method: "GET",
            path: `/projects/${id}`,
            decode: decodeAt(AetherProject),
            forbidden: "org",
          }),
        ),
      );
    },

    createTask: (input) => {
      const endpoint = "POST /tasks";
      return jsonBody(AetherCreateTaskRequest, endpoint, input).pipe(
        Effect.flatMap((body) =>
          exchange({
            endpoint,
            method: "POST",
            path: "/tasks",
            body,
            decode: decodeAt(AetherCreateTaskResponse),
            forbidden: "admission-org-or-error",
            conflict: createTaskConflict(endpoint),
          }),
        ),
      );
    },

    getTask: (taskId) => {
      const endpoint = "GET /tasks/{taskID}";
      return taskPath(endpoint, taskId).pipe(
        Effect.flatMap((path) =>
          exchange({
            endpoint,
            method: "GET",
            path,
            decode: decodeAt(AetherTaskWithActivity),
            forbidden: "org",
          }),
        ),
      );
    },

    updateTask: (taskId, input) => {
      const endpoint = "PUT /tasks/{taskID}";
      return Effect.all([
        taskPath(endpoint, taskId),
        jsonBody(AetherUpdateTaskRequest, endpoint, input),
      ]).pipe(
        Effect.flatMap(([path, body]) =>
          exchange({
            endpoint,
            method: "PUT",
            path,
            body,
            decode: decodeAt(AetherTaskWithActivity),
            forbidden: "org-or-error",
          }),
        ),
      );
    },

    respondToTask: (taskId, input) => {
      const endpoint = "POST /tasks/{taskID}/respond";
      return Effect.all([
        taskPath(endpoint, taskId),
        jsonBody(AetherRespondToTaskRequest, endpoint, input),
      ]).pipe(
        Effect.flatMap(([path, body]) =>
          exchange({
            endpoint,
            method: "POST",
            path: `${path}/respond`,
            body,
            decode: decodeAt(AetherRespondToTaskResponse),
            forbidden: "admission-org-or-error",
            conflict: respondConflict(endpoint),
          }),
        ),
      );
    },

    stopTask: (taskId, input) => {
      const endpoint = "POST /tasks/{taskID}/stop";
      return Effect.all([
        taskPath(endpoint, taskId),
        jsonBody(AetherStopTaskRequest, endpoint, input),
      ]).pipe(
        Effect.flatMap(([path, body]) =>
          exchange<void, AetherApiStopConflictError>({
            endpoint,
            method: "POST",
            path: `${path}/stop`,
            body,
            forbidden: "org-or-error",
            conflict: stopConflict(endpoint),
          }),
        ),
      );
    },

    removeFromQueue: (taskId, input) => {
      const endpoint = "POST /tasks/{taskID}/remove-from-queue";
      return Effect.all([
        taskPath(endpoint, taskId),
        jsonBody(AetherRemoveFromQueueRequest, endpoint, input),
      ]).pipe(
        Effect.flatMap(([path, body]) =>
          exchange<void, never>({
            endpoint,
            method: "POST",
            path: `${path}/remove-from-queue`,
            body,
            forbidden: "org-or-error",
          }),
        ),
      );
    },

    getConversationTurns: (taskId, query) => {
      const endpoint = "GET /tasks/{taskID}/conversation/turns";
      return Effect.all([taskPath(endpoint, taskId), pageQuery(endpoint, query)]).pipe(
        Effect.flatMap(([path, params]) =>
          exchange({
            endpoint,
            method: "GET",
            path: `${path}/conversation/turns`,
            query: params,
            decode: decodeAt(AetherConversationTurnsPage),
            forbidden: "org",
          }),
        ),
      );
    },

    getTurnActivity: (taskId, turnId, query) => {
      const endpoint = "GET /tasks/{taskID}/conversation/turns/{turnID}/activity";
      return Effect.all([
        taskPath(endpoint, taskId),
        encodeAt(AetherTurnIdParam, "turnID")(endpoint, turnId),
        pageQuery(endpoint, query),
      ]).pipe(
        Effect.flatMap(([path, turn, params]) =>
          exchange({
            endpoint,
            method: "GET",
            path: `${path}/conversation/turns/${turn}/activity`,
            query: params,
            decode: decodeAt(AetherTurnActivityPage),
            forbidden: "org",
          }),
        ),
      );
    },

    getLiveConversation: (taskId, messageIds) => {
      const endpoint = "GET /tasks/{taskID}/conversation/live";
      const encodedIds = encodeAt(AetherLiveMessageIds, "messageIds")(endpoint, messageIds).pipe(
        Effect.map((ids) => JSON.stringify(ids)),
        Effect.filterOrFail(
          (json) => json.length <= AETHER_LIVE_MESSAGE_IDS_MAX_LENGTH,
          (json) =>
            new AetherApiEncodeError({
              endpoint,
              detail: `messageIds: the JSON array is ${json.length} characters; Aether accepts at most ${AETHER_LIVE_MESSAGE_IDS_MAX_LENGTH}.`,
            }),
        ),
      );
      return Effect.all([taskPath(endpoint, taskId), encodedIds]).pipe(
        Effect.flatMap(([path, ids]) =>
          exchange({
            endpoint,
            method: "GET",
            path: `${path}/conversation/live`,
            query: [["messageIds", ids]],
            decode: decodeAt(AetherLiveConversationSnapshot),
            forbidden: "org",
          }),
        ),
      );
    },

    listSubagents: (taskId) => {
      const endpoint = "GET /tasks/{taskID}/subagents";
      return taskPath(endpoint, taskId).pipe(
        Effect.flatMap((path) =>
          exchange({
            endpoint,
            method: "GET",
            path: `${path}/subagents`,
            decode: decodeAt(AetherTaskSubagents),
            forbidden: "org",
          }),
        ),
      );
    },

    connectWorkspace: (workspaceId, { start }) => {
      const endpoint = "POST /workspaces/{workspaceID}/connect";
      return encodeAt(AetherWorkspaceIdParam, "workspaceID")(endpoint, workspaceId).pipe(
        Effect.flatMap((id) =>
          exchange({
            endpoint,
            method: "POST",
            path: `/workspaces/${id}/connect`,
            query: [["start", start ? "true" : "false"]],
            decode: decodeAt(AetherWorkspaceConnect),
            forbidden: "org",
            conflict: connectConflict(endpoint),
          }),
        ),
      );
    },
  };
}

/** The client over Synara's settings and secret store. `request` and `timeoutMs` are for tests. */
export const makeAetherApiClientLive = (options: Omit<AetherApiClientOptions, "connection"> = {}) =>
  Layer.effect(
    AetherApiClient,
    Effect.gen(function* () {
      const serverSettings = yield* ServerSettingsService;
      const credentials = yield* ProviderCredentials;
      return makeAetherApiClient({
        ...options,
        connection: resolveAetherApiConnection({
          apiBaseUrl: serverSettings.getSettings.pipe(
            Effect.map((settings) => settings.providers.aether.apiBaseUrl),
          ),
          readApiKey: credentials.getSecret("aether"),
        }),
      });
    }),
  );

export const AetherApiClientLive = makeAetherApiClientLive();

// FILE: aetherWorkspaceSocket.ts
// Purpose: Connects to an Aether workspace (REST connect, then its WebSocket) and offers
// correlated git diff and files read requests, the user-activity keepalive, and a clean close.
// Layer: Server provider boundary (Aether).
//
// The socket authenticates with the API key as `?token=` on the connect transport's
// `websocket_path` (the form Aether's own clients use), under the configured API base URL. The
// host is resolved once with outboundHttp's address rules and the connection pinned to that
// address; redirects are refused. The URL carrying the key never appears in errors or logs.
//
// Out of scope here: the terminal, agent and ports channels, and reconnecting. A dropped socket
// fails its in-flight requests and resolves `closed`; the owner decides whether to connect again.

import { randomUUID } from "node:crypto";
import type { LookupFunction } from "node:net";

import { resolveAetherApiUrl, type AetherApiBaseUrl } from "@synara/contracts";
import { invokePinnedDnsLookup, resolveOutboundPinnedAddress } from "@synara/shared/outboundHttp";
import { Cause, Deferred, Effect, Option, Queue, Schema, Scope } from "effect";
import WebSocket from "ws";

import type { AetherApiClientShape, AetherApiConnection } from "./aetherApiClient.ts";
import {
  formatAetherSchemaIssue,
  type AetherApiConnectConflictError,
  type AetherApiRequestError,
} from "./aetherApiErrors.ts";
import type { AetherWorkspaceTransport } from "./aetherApiSchemas.ts";
import {
  AETHER_ACTIVITY_PING_INTERVAL_MS,
  AetherFileReadRequest,
  AetherFileReadResponse,
  AetherGitDiffRequest,
  AetherGitDiffResponse,
  AetherUserActivityMessage,
  AetherWorkspaceErrorMessage,
  type AetherFileRead,
  type AetherFileErrorCode,
  type AetherGitDiffMode,
  type AetherGitDiffResult,
} from "./aetherWorkspaceProtocol.ts";

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/** The socket did not open: bad transport path, address policy, upgrade refused, or timeout. */
export class AetherWorkspaceSocketOpenError extends Schema.TaggedErrorClass<AetherWorkspaceSocketOpenError>()(
  "AetherWorkspaceSocketOpenError",
  {
    workspaceId: Schema.String,
    detail: Schema.String,
    /** The HTTP status when the server refused the upgrade. */
    status: Schema.optional(Schema.Number),
    cause: Schema.optional(Schema.Defect),
  },
) {
  override get message(): string {
    return `Aether workspace socket failed to open (${this.workspaceId}): ${this.detail}`;
  }
}

/** The socket is closed, or closed while a request was in flight. */
export class AetherWorkspaceSocketClosedError extends Schema.TaggedErrorClass<AetherWorkspaceSocketClosedError>()(
  "AetherWorkspaceSocketClosedError",
  {
    workspaceId: Schema.String,
    detail: Schema.String,
    code: Schema.optional(Schema.Number),
    reason: Schema.optional(Schema.String),
  },
) {
  override get message(): string {
    return `Aether workspace socket is closed (${this.workspaceId}): ${this.detail}`;
  }
}

/** The workspace did not answer a correlated request in time. */
export class AetherWorkspaceRequestTimeoutError extends Schema.TaggedErrorClass<AetherWorkspaceRequestTimeoutError>()(
  "AetherWorkspaceRequestTimeoutError",
  {
    workspaceId: Schema.String,
    channel: Schema.String,
    requestType: Schema.String,
    requestId: Schema.String,
    timeoutMs: Schema.Number,
    /** The last error frame the workspace sent, which often explains the silence. */
    lastServerError: Schema.optional(Schema.String),
  },
) {
  override get message(): string {
    return `Aether workspace did not answer the ${this.channel} '${this.requestType}' request within ${this.timeoutMs}ms${
      this.lastServerError === undefined ? "." : ` (workspace error: ${this.lastServerError}).`
    }`;
  }
}

/** A frame did not match the protocol: an invalid request, or a response that did not decode. */
export class AetherWorkspaceProtocolError extends Schema.TaggedErrorClass<AetherWorkspaceProtocolError>()(
  "AetherWorkspaceProtocolError",
  {
    workspaceId: Schema.String,
    channel: Schema.String,
    requestType: Schema.String,
    direction: Schema.Literals(["request", "response"]),
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect),
  },
) {
  override get message(): string {
    return `Aether workspace ${this.channel} '${this.requestType}' ${this.direction} does not match the protocol: ${this.detail}`;
  }
}

/** The workspace answered a request with `success: false`. */
export class AetherWorkspaceRequestFailedError extends Schema.TaggedErrorClass<AetherWorkspaceRequestFailedError>()(
  "AetherWorkspaceRequestFailedError",
  {
    workspaceId: Schema.String,
    channel: Schema.String,
    requestType: Schema.String,
    detail: Schema.String,
    /** The files channel's error code; the git channel has none. */
    code: Schema.optional(Schema.String),
  },
) {
  override get message(): string {
    return `Aether workspace ${this.channel} '${this.requestType}' request failed${
      this.code === undefined ? "" : ` (${this.code})`
    }: ${this.detail}`;
  }
}

export type AetherWorkspaceRequestError =
  | AetherWorkspaceSocketClosedError
  | AetherWorkspaceRequestTimeoutError
  | AetherWorkspaceProtocolError
  | AetherWorkspaceRequestFailedError;

export type AetherWorkspaceSocketError =
  | AetherWorkspaceSocketOpenError
  | AetherWorkspaceRequestError;

// ---------------------------------------------------------------------------
// Socket handle
// ---------------------------------------------------------------------------

export interface AetherWorkspaceSocketOptions {
  /** Budget for DNS resolution plus the upgrade handshake. Default 15s. */
  readonly openTimeoutMs?: number;
  /** Budget for one correlated request. Default 30s. */
  readonly requestTimeoutMs?: number;
  /** How long a clean close waits for the server's close frame before terminating. Default 5s. */
  readonly closeTimeoutMs?: number;
  /** Largest frame accepted (after decompression). Default 64 MiB. */
  readonly maxPayloadBytes?: number;
}

const DEFAULT_OPEN_TIMEOUT_MS = 15_000;
const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
const DEFAULT_CLOSE_TIMEOUT_MS = 5_000;
const DEFAULT_MAX_PAYLOAD_BYTES = 64 * 1024 * 1024;
const NORMAL_CLOSURE = 1000;

export interface AetherWorkspaceSocketCloseInfo {
  readonly code: number;
  readonly reason: string;
}

export interface AetherWorkspaceSocket {
  readonly workspaceId: string;
  /** Authorizes port previews for this workspace (not used by this transport). */
  readonly previewToken: string;
  /** The cumulative diff (`main`: merge-base to the working tree; `lastCommit`: the last commit). */
  readonly requestGitDiff: (input: {
    readonly mode: AetherGitDiffMode;
  }) => Effect.Effect<AetherGitDiffResult, AetherWorkspaceRequestError>;
  /** Reads one file; binary content arrives base64-encoded. */
  readonly readFile: (path: string) => Effect.Effect<AetherFileRead, AetherWorkspaceRequestError>;
  /** One `user_activity` ping: renews the workspace's interactive idle lease. */
  readonly sendUserActivity: () => Effect.Effect<void, AetherWorkspaceSocketClosedError>;
  /**
   * Pings now and then every `intervalMs` (at most the protocol's 30s) until interrupted or the
   * socket closes, which fails it. Fork it for as long as the workspace must stay awake.
   */
  readonly keepAlive: (options?: {
    readonly intervalMs?: number;
  }) => Effect.Effect<never, AetherWorkspaceSocketClosedError>;
  /** Closes with a normal closure and waits for it; idempotent. Scope close does the same. */
  readonly close: () => Effect.Effect<void>;
  /** Completes when the socket has closed, from either side. */
  readonly closed: Effect.Effect<AetherWorkspaceSocketCloseInfo>;
}

/**
 * The socket URL for a connect transport, without the key. `websocket_path` must be an absolute
 * path on the API origin; it is resolved under the base URL like REST paths.
 */
export function aetherWorkspaceSocketUrl(
  apiBaseUrl: AetherApiBaseUrl,
  websocketPath: string,
): { readonly httpUrl: URL; readonly socketUrl: URL } | { readonly issue: string } {
  if (!websocketPath.startsWith("/") || websocketPath.startsWith("//")) {
    return { issue: "The workspace websocket_path is not an absolute path." };
  }
  if (/[?#\\]/u.test(websocketPath)) {
    return { issue: "The workspace websocket_path must not carry a query, fragment or backslash." };
  }
  const httpUrl = resolveAetherApiUrl(apiBaseUrl, websocketPath);
  if (httpUrl.origin !== new URL(apiBaseUrl).origin) {
    return { issue: "The workspace websocket_path leaves the Aether API origin." };
  }
  const socketUrl = new URL(httpUrl);
  socketUrl.protocol = httpUrl.protocol === "https:" ? "wss:" : "ws:";
  return { httpUrl, socketUrl };
}

type PendingRequest = Deferred.Deferred<unknown, AetherWorkspaceSocketClosedError>;

type Notice =
  | { readonly _tag: "ServerError"; readonly error: string }
  | { readonly _tag: "Malformed"; readonly detail: string };

const UPGRADE_STATUS_PATTERN = /^Unexpected server response: (\d{3})$/u;

/**
 * Opens the workspace socket for a `running` connect transport. The socket closes when the
 * scope closes; requests still in flight then fail with `AetherWorkspaceSocketClosedError`.
 */
export const openAetherWorkspaceSocket = (input: {
  readonly connection: AetherApiConnection;
  readonly workspaceId: string;
  readonly transport: AetherWorkspaceTransport;
  readonly options?: AetherWorkspaceSocketOptions;
}): Effect.Effect<AetherWorkspaceSocket, AetherWorkspaceSocketOpenError, Scope.Scope> =>
  Effect.gen(function* () {
    const { workspaceId } = input;
    const openTimeoutMs = input.options?.openTimeoutMs ?? DEFAULT_OPEN_TIMEOUT_MS;
    const requestTimeoutMs = input.options?.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
    const closeTimeoutMs = input.options?.closeTimeoutMs ?? DEFAULT_CLOSE_TIMEOUT_MS;
    const maxPayload = input.options?.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES;

    const urls = aetherWorkspaceSocketUrl(
      input.connection.apiBaseUrl,
      input.transport.websocket_path,
    );
    if ("issue" in urls) {
      return yield* new AetherWorkspaceSocketOpenError({ workspaceId, detail: urls.issue });
    }

    const notices = yield* Queue.unbounded<Notice>();
    const pending = new Map<string, PendingRequest>();
    const closedSignal = Deferred.makeUnsafe<AetherWorkspaceSocketCloseInfo>();
    let closeInfo: AetherWorkspaceSocketCloseInfo | undefined;
    let lastServerError: string | undefined;

    const closedError = (detail: string) =>
      new AetherWorkspaceSocketClosedError({
        workspaceId,
        detail,
        ...(closeInfo === undefined ? {} : { code: closeInfo.code, reason: closeInfo.reason }),
      });

    const failPending = (detail: string) => {
      const error = closedError(detail);
      for (const deferred of pending.values()) {
        Deferred.doneUnsafe(deferred, Effect.fail(error));
      }
      pending.clear();
    };

    // Runs in the socket's event callbacks: route responses to their waiters synchronously,
    // queue everything worth a log line for the scoped logger below.
    const onFrame = (data: WebSocket.RawData, isBinary: boolean) => {
      if (isBinary) {
        Queue.offerUnsafe(notices, { _tag: "Malformed", detail: "binary frame" });
        return;
      }
      const text = (
        Array.isArray(data) ? Buffer.concat(data) : Buffer.isBuffer(data) ? data : Buffer.from(data)
      ).toString("utf8");
      let frame: unknown;
      try {
        frame = JSON.parse(text);
      } catch {
        Queue.offerUnsafe(notices, { _tag: "Malformed", detail: "frame is not JSON" });
        return;
      }
      if (typeof frame !== "object" || frame === null || Array.isArray(frame)) {
        Queue.offerUnsafe(notices, { _tag: "Malformed", detail: "frame is not an object" });
        return;
      }
      const record = frame as Record<string, unknown>;
      if (typeof record.requestId === "string") {
        const waiter = pending.get(record.requestId);
        if (waiter !== undefined) {
          pending.delete(record.requestId);
          Deferred.doneUnsafe(waiter, Effect.succeed(frame));
        }
        // No waiter: the request already timed out, or the frame answers another client.
        return;
      }
      if (record.channel === "error") {
        const decoded = Schema.decodeUnknownExit(AetherWorkspaceErrorMessage)(frame);
        if (decoded._tag === "Success") {
          lastServerError = decoded.value.error;
          Queue.offerUnsafe(notices, { _tag: "ServerError", error: decoded.value.error });
        } else {
          const error = Cause.findErrorOption(decoded.cause);
          Queue.offerUnsafe(notices, {
            _tag: "Malformed",
            detail: `error frame: ${
              Option.isSome(error) ? formatAetherSchemaIssue(error.value.issue) : "did not decode"
            }`,
          });
        }
      }
      // Other channels (terminal, agent, ports, file changes, git checkpoints) share the socket
      // but are not this transport's concern.
    };

    const pinnedAddress = yield* Effect.tryPromise({
      try: (signal) =>
        resolveOutboundPinnedAddress(urls.httpUrl, {
          requirePublicAddress: true,
          allowLoopbackHttp: urls.httpUrl.protocol === "http:",
          signal,
        }),
      catch: (cause) =>
        new AetherWorkspaceSocketOpenError({
          workspaceId,
          detail: "The workspace host did not resolve to an allowed address.",
          cause,
        }),
    });
    const lookup: LookupFunction = (_hostname, options, callback) =>
      invokePinnedDnsLookup(pinnedAddress, options, callback);

    const tokenUrl = new URL(urls.socketUrl);
    tokenUrl.searchParams.set("token", input.connection.apiKey);
    const socketOptions: WebSocket.ClientOptions & { readonly lookup: LookupFunction } = {
      handshakeTimeout: openTimeoutMs,
      maxPayload,
      followRedirects: false,
      lookup,
    };

    const socket = yield* Effect.acquireRelease(
      Effect.callback<WebSocket, AetherWorkspaceSocketOpenError>((resume) => {
        const ws = new WebSocket(tokenUrl, socketOptions);
        let opened = false;
        const failOpen = (detail: string, extra: { status?: number; cause?: unknown } = {}) => {
          if (opened) return;
          opened = true;
          ws.terminate();
          resume(
            Effect.fail(new AetherWorkspaceSocketOpenError({ workspaceId, detail, ...extra })),
          );
        };
        ws.on("open", () => {
          if (opened) return;
          opened = true;
          resume(Effect.succeed(ws));
        });
        ws.on("error", (cause) => {
          const status = UPGRADE_STATUS_PATTERN.exec(cause.message)?.[1];
          if (status !== undefined) {
            failOpen(`The workspace refused the socket upgrade (HTTP ${status}).`, {
              status: Number(status),
            });
          } else {
            failOpen("The socket failed before it opened.", { cause });
          }
        });
        ws.on("close", (code, reason) => {
          failOpen(`The socket closed before it opened (code ${code}).`);
          closeInfo = { code, reason: reason.toString("utf8") };
          failPending("The workspace socket closed with the request in flight.");
          Deferred.doneUnsafe(closedSignal, Effect.succeed(closeInfo));
        });
        ws.on("message", onFrame);
        return Effect.sync(() => {
          // Interrupted while opening.
          opened = true;
          ws.terminate();
        });
      }).pipe(
        Effect.timeoutOrElse({
          duration: openTimeoutMs,
          onTimeout: () =>
            Effect.fail(
              new AetherWorkspaceSocketOpenError({
                workspaceId,
                detail: `The socket did not open within ${openTimeoutMs}ms.`,
              }),
            ),
        }),
      ),
      (ws) => closeSocket(ws),
    );

    function closeSocket(ws: WebSocket): Effect.Effect<void> {
      return Effect.gen(function* () {
        if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) {
          ws.close(NORMAL_CLOSURE, "Synara closed the workspace connection");
        }
        yield* Deferred.await(closedSignal).pipe(
          Effect.timeoutOrElse({
            duration: closeTimeoutMs,
            onTimeout: () =>
              Effect.sync(() => {
                ws.terminate();
              }),
          }),
          Effect.asVoid,
        );
        failPending("The workspace socket was closed.");
      });
    }

    // Workspace error frames and malformed frames cannot be tied to a request; log them.
    yield* Effect.forkScoped(
      Effect.forever(
        Queue.take(notices).pipe(
          Effect.flatMap((notice) =>
            notice._tag === "ServerError"
              ? Effect.logWarning("aether.workspace.server-error", {
                  workspaceId,
                  error: notice.error,
                })
              : Effect.logWarning("aether.workspace.malformed-frame", {
                  workspaceId,
                  detail: notice.detail,
                }),
          ),
        ),
      ),
    );

    const send = (text: string): Effect.Effect<void, AetherWorkspaceSocketClosedError> =>
      Effect.callback<void, AetherWorkspaceSocketClosedError>((resume) => {
        if (socket.readyState !== WebSocket.OPEN) {
          resume(Effect.fail(closedError("The workspace socket is not open.")));
          return;
        }
        socket.send(text, (cause) => {
          resume(
            cause === undefined || cause === null
              ? Effect.void
              : Effect.fail(closedError("The workspace socket failed to send.")),
          );
        });
      });

    const encodeFrame = <S extends Schema.Top & { readonly EncodingServices: never }>(
      schema: S,
      value: S["Type"],
      channel: string,
      requestType: string,
    ) =>
      Schema.encodeUnknownEffect(Schema.fromJsonString(schema))(value, {
        onExcessProperty: "error",
      }).pipe(
        Effect.mapError(
          (cause) =>
            new AetherWorkspaceProtocolError({
              workspaceId,
              channel,
              requestType,
              direction: "request",
              detail: formatAetherSchemaIssue(cause.issue),
              cause,
            }),
        ),
      );

    const request = <S extends Schema.Top & { readonly DecodingServices: never }>(options: {
      readonly channel: "git" | "files";
      readonly requestType: string;
      readonly frame: (requestId: string) => Effect.Effect<string, AetherWorkspaceProtocolError>;
      readonly response: S;
    }): Effect.Effect<S["Type"], AetherWorkspaceRequestError> =>
      Effect.gen(function* () {
        const { channel, requestType } = options;
        const requestId = `synara-${channel}-${randomUUID()}`;
        const text = yield* options.frame(requestId);
        if (closeInfo !== undefined) {
          return yield* closedError("The workspace socket is closed.");
        }
        const deferred = Deferred.makeUnsafe<unknown, AetherWorkspaceSocketClosedError>();
        pending.set(requestId, deferred);
        const frame = yield* send(text).pipe(
          Effect.andThen(Deferred.await(deferred)),
          Effect.timeoutOrElse({
            duration: requestTimeoutMs,
            onTimeout: () =>
              Effect.fail(
                new AetherWorkspaceRequestTimeoutError({
                  workspaceId,
                  channel,
                  requestType,
                  requestId,
                  timeoutMs: requestTimeoutMs,
                  ...(lastServerError === undefined ? {} : { lastServerError }),
                }),
              ),
          }),
          Effect.ensuring(Effect.sync(() => pending.delete(requestId))),
        );
        return yield* Schema.decodeUnknownEffect(options.response)(frame).pipe(
          Effect.mapError(
            (cause) =>
              new AetherWorkspaceProtocolError({
                workspaceId,
                channel,
                requestType,
                direction: "response",
                detail: formatAetherSchemaIssue(cause.issue),
                cause,
              }),
          ),
        );
      });

    const sendUserActivity = () =>
      encodeFrame(
        AetherUserActivityMessage,
        { channel: "activity", type: "user_activity" },
        "activity",
        "user_activity",
      ).pipe(
        // The frame is a constant; only the socket can fail here.
        Effect.orDie,
        Effect.flatMap(send),
      );

    return {
      workspaceId,
      previewToken: input.transport.preview_token,

      requestGitDiff: ({ mode }) =>
        request({
          channel: "git",
          requestType: "diff",
          frame: (requestId) =>
            encodeFrame(
              AetherGitDiffRequest,
              { channel: "git", type: "diff", requestId, mode },
              "git",
              "diff",
            ),
          response: AetherGitDiffResponse,
        }).pipe(
          Effect.flatMap((response) =>
            response.success
              ? Effect.succeed(response.diff)
              : Effect.fail(
                  new AetherWorkspaceRequestFailedError({
                    workspaceId,
                    channel: "git",
                    requestType: "diff",
                    detail: response.error,
                  }),
                ),
          ),
        ),

      readFile: (path) =>
        request({
          channel: "files",
          requestType: "read",
          frame: (requestId) =>
            encodeFrame(
              AetherFileReadRequest,
              { channel: "files", type: "read", requestId, path },
              "files",
              "read",
            ),
          response: AetherFileReadResponse,
        }).pipe(
          Effect.flatMap((response) =>
            response.success
              ? Effect.succeed(response)
              : Effect.fail(
                  new AetherWorkspaceRequestFailedError({
                    workspaceId,
                    channel: "files",
                    requestType: "read",
                    detail: response.error,
                    code: response.code satisfies AetherFileErrorCode,
                  }),
                ),
          ),
        ),

      sendUserActivity,

      keepAlive: (options) => {
        const intervalMs = Math.min(
          options?.intervalMs ?? AETHER_ACTIVITY_PING_INTERVAL_MS,
          AETHER_ACTIVITY_PING_INTERVAL_MS,
        );
        return Effect.forever(sendUserActivity().pipe(Effect.andThen(Effect.sleep(intervalMs))));
      },

      close: () => closeSocket(socket),

      closed: Deferred.await(closedSignal),
    } satisfies AetherWorkspaceSocket;
  });

export type AetherWorkspaceAttach =
  | { readonly _tag: "Connected"; readonly socket: AetherWorkspaceSocket }
  /** The workspace is getting ready; connect again after `retryAfterMs`. */
  | {
      readonly _tag: "Connecting";
      readonly workspaceId: string;
      readonly displayState: string;
      readonly retryAfterMs: number;
    };

/**
 * `POST /workspaces/{workspaceID}/connect`, then the socket when the workspace is running.
 * `start: true` may boot a stopped workspace (a user-initiated turn); `start: false` attaches
 * only, and a workspace that is not usable fails with `AetherApiConnectConflictError`.
 */
export const connectAetherWorkspace = (input: {
  readonly client: Pick<AetherApiClientShape, "connection" | "connectWorkspace">;
  readonly workspaceId: string;
  readonly start: boolean;
  readonly options?: AetherWorkspaceSocketOptions;
}): Effect.Effect<
  AetherWorkspaceAttach,
  AetherApiRequestError | AetherApiConnectConflictError | AetherWorkspaceSocketOpenError,
  Scope.Scope
> =>
  Effect.gen(function* () {
    const outcome = yield* input.client.connectWorkspace(input.workspaceId, {
      start: input.start,
    });
    if (outcome.state === "connecting") {
      return {
        _tag: "Connecting",
        workspaceId: outcome.workspace_id,
        displayState: outcome.display_state,
        retryAfterMs: outcome.retry_after_ms,
      } as const;
    }
    const connection = yield* input.client.connection;
    const socket = yield* openAetherWorkspaceSocket({
      connection,
      workspaceId: outcome.workspace_id,
      transport: outcome.transport,
      ...(input.options === undefined ? {} : { options: input.options }),
    });
    return { _tag: "Connected", socket } as const;
  });

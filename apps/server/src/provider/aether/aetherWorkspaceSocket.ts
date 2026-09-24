// FILE: aetherWorkspaceSocket.ts
// Purpose: Connects to an Aether workspace (REST connect, then its WebSocket) and offers
// correlated git diff and files read requests, the user-activity keepalive, and a clean close.
// Layer: Server provider boundary (Aether).
//
// The socket authenticates with the API key as `?token=` on the connect transport's
// `websocket_path` (the form Aether's own clients use), under the configured API base URL. The
// host is resolved once with outboundHttp's address rules and the connection pinned to that
// address; redirects are refused. The URL carrying the key never appears in an error.
//
// Out of scope: the terminal, agent and ports channels, and reconnecting. A dropped socket fails
// its in-flight requests and completes `closed`; the owner decides whether to connect again.

import { randomUUID } from "node:crypto";
import type { LookupFunction } from "node:net";

import { resolveAetherApiUrl } from "@synara/contracts";
import { invokePinnedDnsLookup, resolvePinnedAddress } from "@synara/shared/outboundHttp";
import { Deferred, Effect, Option, Schema, type Scope } from "effect";
import WebSocket from "ws";

import type { AetherApiClientShape, AetherApiConnection } from "../Services/AetherApiClient.ts";
import type { AetherApiError } from "./aetherApiErrors.ts";
import type { AetherWorkspaceId } from "./aetherApiSchemas.ts";
import {
  AetherFileReadResponse,
  AetherGitDiffResponse,
  AetherWorkspaceAnswer,
  AetherWorkspaceErrorFrame,
  type AetherFileRead,
  type AetherGitDiffResult,
} from "./aetherWorkspaceProtocol.ts";

/** The socket did not open, or it closed (before or during a request). Connect again later. */
export class AetherWorkspaceSocketError extends Schema.TaggedErrorClass<AetherWorkspaceSocketError>()(
  "AetherWorkspaceSocketError",
  {
    workspaceId: Schema.String,
    reason: Schema.Literals(["open-failed", "closed"]),
    detail: Schema.String,
    /** The HTTP status when the workspace refused the upgrade. */
    status: Schema.optional(Schema.Number),
    cause: Schema.optional(Schema.Defect),
  },
) {
  override get message(): string {
    return `Aether workspace socket ${this.reason} (${this.workspaceId}): ${this.detail}`;
  }
}

/** A request got no answer in time, a `success: false` answer, or an answer that did not decode. */
export class AetherWorkspaceRequestError extends Schema.TaggedErrorClass<AetherWorkspaceRequestError>()(
  "AetherWorkspaceRequestError",
  {
    workspaceId: Schema.String,
    request: Schema.Literals(["git.diff", "files.read"]),
    reason: Schema.Literals(["timeout", "failed", "malformed"]),
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect),
  },
) {
  override get message(): string {
    return `Aether workspace ${this.request} request ${this.reason} (${this.workspaceId}): ${this.detail}`;
  }
}

export interface AetherWorkspaceSocketOptions {
  /** Budget for address resolution plus the upgrade. Default 15s. */
  readonly openTimeoutMs?: number;
  /** Budget for one correlated request. Default 30s. */
  readonly requestTimeoutMs?: number;
}

export interface AetherWorkspaceSocketCloseInfo {
  readonly code: number;
  readonly reason: string;
  /** The socket error that closed it, if one did. */
  readonly error?: unknown;
}

type RequestFailure = AetherWorkspaceSocketError | AetherWorkspaceRequestError;

export interface AetherWorkspaceSocket {
  /** The cumulative diff: `main` from the merge-base, `lastCommit` for the last commit. */
  readonly requestGitDiff: (
    mode: "main" | "lastCommit",
  ) => Effect.Effect<AetherGitDiffResult, RequestFailure>;
  /** Reads one file; binary content arrives base64-encoded. */
  readonly readFile: (path: string) => Effect.Effect<AetherFileRead, RequestFailure>;
  /**
   * Sends `user_activity` now and every 30s (the protocol's ping interval) until interrupted or
   * the socket closes. It is the only thing that renews the workspace's interactive idle lease.
   */
  readonly keepAlive: Effect.Effect<never, AetherWorkspaceSocketError>;
  /** Completes when the socket has closed, from either side. */
  readonly closed: Effect.Effect<AetherWorkspaceSocketCloseInfo>;
}

export type AetherWorkspaceAttach =
  | { readonly _tag: "Connected"; readonly socket: AetherWorkspaceSocket }
  /** The workspace is getting ready; connect again after `retryAfterMs`. */
  | { readonly _tag: "Connecting"; readonly retryAfterMs: number };

const ACTIVITY_PING_INTERVAL_MS = 30_000;
const CLOSE_TIMEOUT_MS = 5_000;
const MAX_FRAME_BYTES = 64 * 1024 * 1024;
const USER_ACTIVITY_FRAME = JSON.stringify({ channel: "activity", type: "user_activity" });

/**
 * `POST /workspaces/{workspaceID}/connect`, then the socket when the workspace is running. Only
 * `start: true` may boot a stopped workspace; with `start: false`, a workspace that is not usable
 * fails with `AetherApiConflictError`. The socket closes with the scope.
 */
export const connectAetherWorkspace = (input: {
  readonly client: Pick<AetherApiClientShape, "connection" | "connectWorkspace">;
  readonly workspaceId: AetherWorkspaceId;
  readonly start: boolean;
  readonly options?: AetherWorkspaceSocketOptions;
}): Effect.Effect<
  AetherWorkspaceAttach,
  AetherApiError | AetherWorkspaceSocketError,
  Scope.Scope
> =>
  Effect.gen(function* () {
    const outcome = yield* input.client.connectWorkspace(input.workspaceId, {
      start: input.start,
    });
    if (outcome.state === "connecting") {
      return { _tag: "Connecting", retryAfterMs: outcome.retry_after_ms } as const;
    }
    const socket = yield* openSocket(
      yield* input.client.connection,
      input.workspaceId,
      outcome.transport.websocket_path,
      input.options ?? {},
    );
    return { _tag: "Connected", socket } as const;
  });

/** The socket URL (without the key), or why the path is refused. */
function socketUrl(connection: AetherApiConnection, websocketPath: string): URL | string {
  if (!websocketPath.startsWith("/") || websocketPath.startsWith("//")) {
    return "The workspace websocket_path is not an absolute path.";
  }
  if (/[?#\\]/u.test(websocketPath)) {
    return "The workspace websocket_path must not carry a query, fragment or backslash.";
  }
  const url = resolveAetherApiUrl(connection.apiBaseUrl, websocketPath);
  if (url.origin !== new URL(connection.apiBaseUrl).origin) {
    return "The workspace websocket_path leaves the Aether API origin.";
  }
  return url;
}

const openSocket = (
  connection: AetherApiConnection,
  workspaceId: AetherWorkspaceId,
  websocketPath: string,
  options: AetherWorkspaceSocketOptions,
): Effect.Effect<AetherWorkspaceSocket, AetherWorkspaceSocketError, Scope.Scope> =>
  Effect.gen(function* () {
    const openTimeoutMs = options.openTimeoutMs ?? 15_000;
    const requestTimeoutMs = options.requestTimeoutMs ?? 30_000;
    const socketError = (
      reason: "open-failed" | "closed",
      detail: string,
      extra: { readonly status?: number; readonly cause?: unknown } = {},
    ) => new AetherWorkspaceSocketError({ workspaceId, reason, detail, ...extra });

    const pending = new Map<string, Deferred.Deferred<unknown, AetherWorkspaceSocketError>>();
    const closedSignal = Deferred.makeUnsafe<AetherWorkspaceSocketCloseInfo>();
    let failure: unknown;
    // The last error frame (or unreadable frame): no request id ties it to a request, so it is
    // reported with the next request that times out.
    let lastProblem: string | undefined;

    const failPending = (error: AetherWorkspaceSocketError) => {
      for (const deferred of pending.values()) Deferred.doneUnsafe(deferred, Effect.fail(error));
      pending.clear();
    };

    const onFrame = (data: WebSocket.RawData) => {
      const text = (
        Array.isArray(data) ? Buffer.concat(data) : Buffer.isBuffer(data) ? data : Buffer.from(data)
      ).toString("utf8");
      let frame: unknown;
      try {
        frame = JSON.parse(text);
      } catch {
        lastProblem = "the workspace sent a frame that is not JSON";
        return;
      }
      const answer = Schema.decodeUnknownOption(AetherWorkspaceAnswer)(frame);
      const waiter = Option.isSome(answer) ? pending.get(answer.value.requestId) : undefined;
      if (Option.isSome(answer) && waiter !== undefined) {
        pending.delete(answer.value.requestId);
        Deferred.doneUnsafe(waiter, Effect.succeed(frame));
        return;
      }
      const errorFrame = Schema.decodeUnknownOption(AetherWorkspaceErrorFrame)(frame);
      if (Option.isSome(errorFrame))
        lastProblem = `the workspace reported: ${errorFrame.value.error}`;
      // Anything else is a late answer to a timed-out request, or another channel's frame.
    };

    const url = socketUrl(connection, websocketPath);
    if (typeof url === "string") return yield* socketError("open-failed", url);
    const tokenUrl = new URL(url);
    tokenUrl.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    tokenUrl.searchParams.set("token", connection.apiKey);

    // One deadline over address resolution and the upgrade.
    const open = Effect.gen(function* () {
      const pinned = yield* Effect.tryPromise({
        try: (signal) => resolvePinnedAddress(url, true, url.protocol === "http:", signal),
        catch: (cause) =>
          socketError("open-failed", "The workspace host did not resolve to an allowed address.", {
            cause,
          }),
      });
      // `ws` hands `lookup` to `net`/`tls.connect`; its typings omit it.
      const wsOptions: WebSocket.ClientOptions & { readonly lookup: LookupFunction } = {
        maxPayload: MAX_FRAME_BYTES,
        followRedirects: false,
        lookup: (_hostname, lookupOptions, callback) =>
          invokePinnedDnsLookup(pinned, lookupOptions, callback),
      };
      return yield* Effect.callback<WebSocket, AetherWorkspaceSocketError>((resume) => {
        const ws = new WebSocket(tokenUrl, wsOptions);
        let settled = false;
        const failOpen = (detail: string, extra?: { status?: number; cause?: unknown }) => {
          if (settled) return;
          settled = true;
          ws.terminate();
          resume(Effect.fail(socketError("open-failed", detail, extra)));
        };
        ws.once("open", () => {
          settled = true;
          resume(Effect.succeed(ws));
        });
        ws.on("unexpected-response", (_request, response) =>
          failOpen(`The workspace refused the socket upgrade (HTTP ${response.statusCode}).`, {
            ...(response.statusCode === undefined ? {} : { status: response.statusCode }),
          }),
        );
        ws.on("error", (cause) => {
          if (!settled) return failOpen("The socket failed before it opened.", { cause });
          // After open, `ws` follows an error with `close`: fail what is in flight with the
          // cause now, and report it again through `closed`.
          failure = cause;
          failPending(socketError("closed", "The workspace socket failed.", { cause }));
        });
        ws.on("close", (code, reason) => {
          failOpen(`The socket closed before it opened (code ${code}).`);
          failPending(
            socketError("closed", `The workspace socket closed (code ${code}).`, {
              ...(failure === undefined ? {} : { cause: failure }),
            }),
          );
          Deferred.doneUnsafe(
            closedSignal,
            Effect.succeed({
              code,
              reason: reason.toString("utf8"),
              ...(failure === undefined ? {} : { error: failure }),
            }),
          );
        });
        ws.on("message", onFrame);
        // Interrupted while opening, including by the deadline below.
        return Effect.sync(() => ws.terminate());
      });
    }).pipe(
      Effect.timeoutOrElse({
        duration: openTimeoutMs,
        onTimeout: () =>
          Effect.fail(
            socketError("open-failed", `The socket did not open within ${openTimeoutMs}ms.`),
          ),
      }),
    );

    const ws = yield* Effect.acquireRelease(open, (socket) =>
      Effect.gen(function* () {
        if (socket.readyState === WebSocket.OPEN) {
          socket.close(1000, "Synara closed the workspace connection");
        }
        yield* Deferred.await(closedSignal).pipe(
          Effect.timeoutOrElse({
            duration: CLOSE_TIMEOUT_MS,
            onTimeout: () => Effect.sync(() => socket.terminate()),
          }),
        );
      }),
    );

    const send = (text: string) =>
      Effect.callback<void, AetherWorkspaceSocketError>((resume) => {
        if (ws.readyState !== WebSocket.OPEN) {
          return resume(Effect.fail(socketError("closed", "The workspace socket is not open.")));
        }
        ws.send(text, (cause) =>
          resume(
            cause == null
              ? Effect.void
              : Effect.fail(
                  socketError("closed", "The workspace socket failed to send.", { cause }),
                ),
          ),
        );
      });

    const request = <S extends Schema.Top & { readonly DecodingServices: never }>(
      name: "git.diff" | "files.read",
      frame: Record<string, unknown>,
      response: S,
    ): Effect.Effect<S["Type"], RequestFailure> =>
      Effect.gen(function* () {
        const requestId = `synara-${randomUUID()}`;
        const deferred = Deferred.makeUnsafe<unknown, AetherWorkspaceSocketError>();
        pending.set(requestId, deferred);
        const answer = yield* send(JSON.stringify({ ...frame, requestId })).pipe(
          Effect.andThen(Deferred.await(deferred)),
          Effect.timeoutOrElse({
            duration: requestTimeoutMs,
            onTimeout: () =>
              Effect.fail(
                new AetherWorkspaceRequestError({
                  workspaceId,
                  request: name,
                  reason: "timeout",
                  detail: `No answer within ${requestTimeoutMs}ms${
                    lastProblem === undefined ? "" : `; ${lastProblem}`
                  }.`,
                }),
              ),
          }),
          Effect.ensuring(Effect.sync(() => pending.delete(requestId))),
        );
        return yield* Schema.decodeUnknownEffect(response)(answer).pipe(
          Effect.mapError(
            (cause) =>
              new AetherWorkspaceRequestError({
                workspaceId,
                request: name,
                reason: "malformed",
                detail: cause.message,
                cause,
              }),
          ),
        );
      });

    const failed = (name: "git.diff" | "files.read", detail: string) =>
      new AetherWorkspaceRequestError({ workspaceId, request: name, reason: "failed", detail });

    return {
      requestGitDiff: (mode) =>
        request("git.diff", { channel: "git", type: "diff", mode }, AetherGitDiffResponse).pipe(
          Effect.flatMap((answer) =>
            answer.success
              ? Effect.succeed(answer.diff)
              : Effect.fail(failed("git.diff", answer.error)),
          ),
        ),
      readFile: (path) =>
        request(
          "files.read",
          { channel: "files", type: "read", path },
          AetherFileReadResponse,
        ).pipe(
          Effect.flatMap((answer) =>
            answer.success
              ? Effect.succeed(answer)
              : Effect.fail(failed("files.read", answer.error)),
          ),
        ),
      keepAlive: Effect.forever(
        send(USER_ACTIVITY_FRAME).pipe(Effect.andThen(Effect.sleep(ACTIVITY_PING_INTERVAL_MS))),
      ),
      closed: Deferred.await(closedSignal),
    };
  });

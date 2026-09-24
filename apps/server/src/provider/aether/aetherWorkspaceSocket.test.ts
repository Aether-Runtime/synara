import type { AddressInfo } from "node:net";

import { AetherApiBaseUrl } from "@synara/contracts";
import type { OutboundHttpRequest, OutboundHttpResponse } from "@synara/shared/outboundHttp";
import { Cause, Effect, Exit, Fiber, Option } from "effect";
import { afterEach, describe, expect, it } from "vitest";
import WebSocket, { WebSocketServer } from "ws";

import { makeAetherApiClient } from "./aetherApiClient.ts";
import {
  aetherWorkspaceSocketUrl,
  connectAetherWorkspace,
  type AetherWorkspaceSocket,
  type AetherWorkspaceSocketOptions,
} from "./aetherWorkspaceSocket.ts";

const API_KEY = "aether-secret-key";
const WORKSPACE_ID = "3f0c9a52-6d1e-4c2b-9a7e-0b1d2c3e4f50";
const PREVIEW_TOKEN = "abcdefghijklmnopqrstuvwxyz012345";
const WEBSOCKET_PATH = `/workspaces/${WORKSPACE_ID}/ws`;

type Frame = Record<string, unknown>;

interface FakeWorkspace {
  readonly apiBaseUrl: AetherApiBaseUrl;
  readonly upgrades: Array<{ readonly url: string }>;
  readonly received: Frame[];
  readonly sockets: WebSocket[];
  readonly closes: Array<{ readonly code: number; readonly reason: string }>;
  /** Answers each client frame; return nothing to stay silent. */
  respond: (frame: Frame, socket: WebSocket) => void;
  readonly close: () => Promise<void>;
}

const servers: FakeWorkspace[] = [];

afterEach(async () => {
  await Promise.all(servers.splice(0).map((server) => server.close()));
});

async function startFakeWorkspace(
  options: { readonly refuseUpgradeWith?: number } = {},
): Promise<FakeWorkspace> {
  const server = new WebSocketServer({
    host: "127.0.0.1",
    port: 0,
    ...(options.refuseUpgradeWith === undefined
      ? {}
      : {
          verifyClient: (
            _info: unknown,
            callback: (result: boolean, code?: number, message?: string) => void,
          ) => callback(false, options.refuseUpgradeWith, "Refused"),
        }),
  });
  await new Promise<void>((resolve) => server.once("listening", () => resolve()));
  const { port } = server.address() as AddressInfo;
  const fake: FakeWorkspace = {
    apiBaseUrl: AetherApiBaseUrl.makeUnsafe(`http://127.0.0.1:${port}`),
    upgrades: [],
    received: [],
    sockets: [],
    closes: [],
    respond: () => undefined,
    close: () =>
      new Promise<void>((resolve) => {
        for (const client of server.clients) client.terminate();
        server.close(() => resolve());
      }),
  };
  server.on("connection", (socket, request) => {
    fake.upgrades.push({ url: request.url ?? "" });
    fake.sockets.push(socket);
    socket.on("message", (data) => {
      const frame = JSON.parse(String(data)) as Frame;
      fake.received.push(frame);
      fake.respond(frame, socket);
    });
    socket.on("close", (code, reason) => fake.closes.push({ code, reason: String(reason) }));
  });
  servers.push(fake);
  return fake;
}

function connectResponse(body: unknown, status = 200): OutboundHttpResponse {
  return {
    status,
    headers: new Headers({ "content-type": "application/json" }),
    body: new TextEncoder().encode(JSON.stringify(body)),
    url: "",
  };
}

const running = {
  workspace_id: WORKSPACE_ID,
  state: "running",
  display_state: "ready",
  transport: { websocket_path: WEBSOCKET_PATH, preview_token: PREVIEW_TOKEN },
};

function clientFor(fake: FakeWorkspace, connectBody: unknown = running) {
  const requests: OutboundHttpRequest[] = [];
  const client = makeAetherApiClient({
    connection: Effect.succeed({ apiBaseUrl: fake.apiBaseUrl, apiKey: API_KEY }),
    request: async (request) => {
      requests.push(request);
      return connectResponse(connectBody);
    },
  });
  return { client, requests };
}

/** Connects, runs `use` with the socket, then closes the scope. */
function withSocket<A, E>(
  fake: FakeWorkspace,
  use: (socket: AetherWorkspaceSocket) => Effect.Effect<A, E>,
  options: AetherWorkspaceSocketOptions = {},
) {
  const { client } = clientFor(fake);
  return Effect.runPromiseExit(
    Effect.scoped(
      Effect.gen(function* () {
        const attach = yield* connectAetherWorkspace({
          client,
          workspaceId: WORKSPACE_ID,
          start: false,
          options: { requestTimeoutMs: 2_000, ...options },
        });
        if (attach._tag !== "Connected") throw new Error(`unexpected ${attach._tag}`);
        return yield* use(attach.socket);
      }),
    ),
  );
}

function failureOf<A, E>(exit: Exit.Exit<A, E>): E {
  if (Exit.isSuccess(exit)) throw new Error("expected a failure");
  const error = Cause.findErrorOption(exit.cause);
  if (Option.isNone(error))
    throw new Error(`expected a typed failure: ${Cause.pretty(exit.cause)}`);
  return error.value;
}

function valueOf<A, E>(exit: Exit.Exit<A, E>): A {
  if (Exit.isFailure(exit)) throw new Error(Cause.pretty(exit.cause));
  return exit.value;
}

const diffAnswer = (requestId: unknown, files: unknown[]) => ({
  channel: "git",
  type: "diff",
  requestId,
  success: true,
  diff: { baseRef: "origin/main", files },
});

const textFile = {
  oldPath: "src/a.ts",
  newPath: "src/a.ts",
  displayPath: "src/a.ts",
  status: "modified",
  kind: "text",
  additions: 1,
  deletions: 1,
  hunks: [
    {
      header: "@@ -1,2 +1,2 @@",
      oldStart: 1,
      oldCount: 2,
      newStart: 1,
      newCount: 2,
      lines: [
        { kind: "context", oldLine: 1, newLine: 1, text: "keep" },
        { kind: "del", oldLine: 2, newLine: null, text: "old" },
        { kind: "add", oldLine: null, newLine: 2, text: "new", noTrailingNewline: true },
      ],
    },
  ],
};
const binaryFile = {
  oldPath: "",
  newPath: "assets/logo.png",
  displayPath: "assets/logo.png",
  status: "added",
  kind: "binary",
  additions: 0,
  deletions: 0,
  hunks: [],
};
const tooLargeFile = {
  oldPath: "data/dump.sql",
  newPath: "data/dump.sql",
  displayPath: "data/dump.sql",
  status: "modified",
  kind: "too_large",
  maxBytes: 1_048_576,
};

describe("Aether workspace socket", () => {
  it("builds the socket URL from the base URL, refusing paths that leave the API origin", () => {
    const base = AetherApiBaseUrl.makeUnsafe("https://proxy.example.com/aether/");
    const resolved = aetherWorkspaceSocketUrl(base, WEBSOCKET_PATH);
    expect("socketUrl" in resolved && resolved.socketUrl.href).toBe(
      `wss://proxy.example.com/aether${WEBSOCKET_PATH}`,
    );
    for (const path of ["workspaces/x", "//evil.example.com/ws", "/ws?token=x", "/ws#x", "/\\ws"]) {
      expect(aetherWorkspaceSocketUrl(base, path)).toHaveProperty("issue");
    }
  });

  it("connects without starting, authenticates with ?token= and correlates concurrent requests", async () => {
    const fake = await startFakeWorkspace();
    const { client, requests } = clientFor(fake);
    // Answer the file read before the diff, so correlation (not order) must pair them.
    const held: Frame[] = [];
    fake.respond = (frame, socket) => {
      held.push(frame);
      if (held.length < 2) return;
      for (const pending of held.toReversed()) {
        socket.send(
          JSON.stringify(
            pending.channel === "git"
              ? diffAnswer(pending.requestId, [textFile])
              : {
                  channel: "files",
                  type: "read",
                  requestId: pending.requestId,
                  success: true,
                  path: pending.path,
                  content: "aGVsbG8=",
                  encoding: "base64",
                  size: 5,
                  modified: "2026-09-24T00:00:00Z",
                  isBinary: true,
                },
          ),
        );
      }
    };

    const exit = await Effect.runPromiseExit(
      Effect.scoped(
        Effect.gen(function* () {
          const attach = yield* connectAetherWorkspace({
            client,
            workspaceId: WORKSPACE_ID,
            start: false,
          });
          if (attach._tag !== "Connected") throw new Error("not connected");
          expect(attach.socket.previewToken).toBe(PREVIEW_TOKEN);
          return yield* Effect.all(
            [
              attach.socket.requestGitDiff({ mode: "main" }),
              attach.socket.readFile("assets/logo.png"),
            ],
            { concurrency: "unbounded" },
          );
        }),
      ),
    );

    const [diff, file] = valueOf(exit);
    expect(diff.files.map((entry) => entry.kind)).toEqual(["text"]);
    expect(file).toMatchObject({
      path: "assets/logo.png",
      encoding: "base64",
      content: "aGVsbG8=",
    });
    expect(String(requests[0]?.url)).toBe(
      `${fake.apiBaseUrl}/workspaces/${WORKSPACE_ID}/connect?start=false`,
    );
    const upgrade = new URL(fake.upgrades[0]?.url ?? "", "http://placeholder");
    expect(upgrade.pathname).toBe(WEBSOCKET_PATH);
    expect(upgrade.searchParams.get("token")).toBe(API_KEY);
    // Each request carries its own id, and the two ids differ.
    const ids = fake.received.map((frame) => frame.requestId);
    expect(new Set(ids).size).toBe(2);
    expect(fake.received).toContainEqual({
      channel: "git",
      type: "diff",
      requestId: ids.find((id) => String(id).includes("git")),
      mode: "main",
    });
  });

  it("decodes text, binary and too_large diff files, ignoring keys it does not know", async () => {
    const fake = await startFakeWorkspace();
    fake.respond = (frame, socket) =>
      socket.send(
        JSON.stringify({
          ...diffAnswer(frame.requestId, [
            { ...textFile, addedLater: 1 },
            binaryFile,
            tooLargeFile,
          ]),
          servedBy: "workspace-service",
        }),
      );

    const diff = valueOf(
      await withSocket(fake, (socket) => socket.requestGitDiff({ mode: "main" })),
    );

    expect(diff.baseRef).toBe("origin/main");
    const [text, binary, tooLarge] = diff.files;
    expect(text).toEqual(textFile);
    expect(binary).toEqual(binaryFile);
    expect(tooLarge).toEqual(tooLargeFile);
    expect(tooLarge && "hunks" in tooLarge).toBe(false);
  });

  it("rejects the retired isBinary diff shape, naming the missing kind", async () => {
    const fake = await startFakeWorkspace();
    const { kind: _kind, ...withoutKind } = binaryFile;
    fake.respond = (frame, socket) =>
      socket.send(
        JSON.stringify(diffAnswer(frame.requestId, [{ ...withoutKind, isBinary: true }])),
      );

    const error = failureOf(
      await withSocket(fake, (socket) => socket.requestGitDiff({ mode: "main" })),
    );

    expect(error).toMatchObject({
      _tag: "AetherWorkspaceProtocolError",
      direction: "response",
      channel: "git",
    });
    expect(error._tag === "AetherWorkspaceProtocolError" && error.detail).toContain(
      'Missing key at ["diff"]["files"]["0"]["kind"]',
    );
  });

  it("rejects an unknown diff file kind, naming the field", async () => {
    const fake = await startFakeWorkspace();
    fake.respond = (frame, socket) =>
      socket.send(JSON.stringify(diffAnswer(frame.requestId, [{ ...binaryFile, kind: "lfs" }])));

    const error = failureOf(
      await withSocket(fake, (socket) => socket.requestGitDiff({ mode: "main" })),
    );

    expect(error._tag === "AetherWorkspaceProtocolError" && error.detail).toBe(
      'Expected "text" | "binary" | "too_large", got "lfs" at ["diff"]["files"]["0"]["kind"]',
    );
  });

  it("reports success: false answers as failed requests", async () => {
    const fake = await startFakeWorkspace();
    fake.respond = (frame, socket) =>
      socket.send(
        JSON.stringify(
          frame.channel === "git"
            ? {
                channel: "git",
                type: "diff",
                requestId: frame.requestId,
                success: false,
                error: "not a git repository",
              }
            : {
                channel: "files",
                type: "error",
                requestId: frame.requestId,
                success: false,
                error: "no such file",
                code: "NOT_FOUND",
                path: frame.path,
              },
        ),
      );

    const exit = await withSocket(fake, (socket) =>
      Effect.all([
        socket.requestGitDiff({ mode: "lastCommit" }).pipe(Effect.flip),
        socket.readFile("missing.txt").pipe(Effect.flip),
      ]),
    );

    const [diffError, readError] = valueOf(exit);
    expect(diffError).toMatchObject({
      _tag: "AetherWorkspaceRequestFailedError",
      channel: "git",
      detail: "not a git repository",
    });
    expect(readError).toMatchObject({
      _tag: "AetherWorkspaceRequestFailedError",
      channel: "files",
      code: "NOT_FOUND",
      detail: "no such file",
    });
  });

  it("times out a request the workspace never answers, citing its last error frame", async () => {
    const fake = await startFakeWorkspace();
    fake.respond = (_frame, socket) =>
      socket.send(JSON.stringify({ channel: "error", type: "error", error: "Invalid message" }));

    const error = failureOf(
      await withSocket(fake, (socket) => socket.requestGitDiff({ mode: "main" }), {
        requestTimeoutMs: 100,
      }),
    );

    expect(error).toMatchObject({
      _tag: "AetherWorkspaceRequestTimeoutError",
      channel: "git",
      requestType: "diff",
      timeoutMs: 100,
      lastServerError: "Invalid message",
    });
  });

  it("drops a late answer to a timed-out request without disturbing the next one", async () => {
    const fake = await startFakeWorkspace();
    let calls = 0;
    fake.respond = (frame, socket) => {
      calls += 1;
      // The first answer arrives after its request timed out, while the second is pending.
      const [files, delayMs] = calls === 1 ? [[binaryFile], 250] : [[textFile], 100];
      setTimeout(() => socket.send(JSON.stringify(diffAnswer(frame.requestId, files))), delayMs);
    };

    const exit = await withSocket(
      fake,
      (socket) =>
        Effect.gen(function* () {
          const first = yield* socket.requestGitDiff({ mode: "main" }).pipe(Effect.flip);
          const second = yield* socket.requestGitDiff({ mode: "main" });
          return { first, second };
        }),
      { requestTimeoutMs: 200 },
    );

    const { first, second } = valueOf(exit);
    expect(first._tag).toBe("AetherWorkspaceRequestTimeoutError");
    expect(second.files).toEqual([textFile]);
  });

  it("sends user_activity pings, once or on an interval", async () => {
    const fake = await startFakeWorkspace();

    const exit = await withSocket(fake, (socket) =>
      Effect.gen(function* () {
        yield* socket.sendUserActivity();
        const keepAlive = yield* Effect.forkChild(socket.keepAlive({ intervalMs: 20 }));
        yield* Effect.sleep(90);
        yield* Fiber.interrupt(keepAlive);
      }),
    );

    valueOf(exit);
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(fake.received.length).toBeGreaterThanOrEqual(4);
    for (const frame of fake.received) {
      expect(frame).toEqual({ channel: "activity", type: "user_activity" });
    }
  });

  it("closes with a normal closure when the scope closes, and refuses to send afterwards", async () => {
    const fake = await startFakeWorkspace();
    let captured: AetherWorkspaceSocket | undefined;

    valueOf(
      await withSocket(fake, (socket) =>
        Effect.sync(() => {
          captured = socket;
        }),
      ),
    );
    await new Promise((resolve) => setTimeout(resolve, 20));

    expect(fake.closes).toEqual([{ code: 1000, reason: "Synara closed the workspace connection" }]);
    const socket = captured;
    if (socket === undefined) throw new Error("no socket");
    expect(await Effect.runPromise(socket.closed)).toMatchObject({ code: 1000 });
    const afterClose = failureOf(await Effect.runPromiseExit(socket.sendUserActivity()));
    expect(afterClose._tag).toBe("AetherWorkspaceSocketClosedError");
    const requestAfterClose = failureOf(
      await Effect.runPromiseExit(socket.requestGitDiff({ mode: "main" })),
    );
    expect(requestAfterClose._tag).toBe("AetherWorkspaceSocketClosedError");
  });

  it("fails an in-flight request when the workspace closes the socket", async () => {
    const fake = await startFakeWorkspace();
    fake.respond = (_frame, socket) => socket.close(4001, "workspace suspending");

    const exit = await withSocket(fake, (socket) =>
      Effect.gen(function* () {
        const error = yield* socket.requestGitDiff({ mode: "main" }).pipe(Effect.flip);
        const closed = yield* socket.closed;
        return { error, closed };
      }),
    );

    const { error, closed } = valueOf(exit);
    expect(error).toMatchObject({
      _tag: "AetherWorkspaceSocketClosedError",
      code: 4001,
      reason: "workspace suspending",
    });
    expect(closed).toEqual({ code: 4001, reason: "workspace suspending" });
  });

  it("fails to open when the workspace refuses the upgrade, without leaking the key", async () => {
    const fake = await startFakeWorkspace({ refuseUpgradeWith: 401 });

    const error = failureOf(await withSocket(fake, () => Effect.void));

    expect(error).toMatchObject({ _tag: "AetherWorkspaceSocketOpenError", status: 401 });
    expect(error.message).not.toContain(API_KEY);
    expect(JSON.stringify(error)).not.toContain(API_KEY);
  });

  it("returns Connecting without opening a socket while the workspace starts", async () => {
    const fake = await startFakeWorkspace();
    const { client, requests } = clientFor(fake, {
      workspace_id: WORKSPACE_ID,
      state: "connecting",
      display_state: "starting",
      retry_after_ms: 1500,
    });

    const exit = await Effect.runPromiseExit(
      Effect.scoped(connectAetherWorkspace({ client, workspaceId: WORKSPACE_ID, start: true })),
    );

    expect(valueOf(exit)).toEqual({
      _tag: "Connecting",
      workspaceId: WORKSPACE_ID,
      displayState: "starting",
      retryAfterMs: 1500,
    });
    expect(String(requests[0]?.url)).toContain("start=true");
    expect(fake.upgrades).toEqual([]);
  });

  it("fails to open a transport path that leaves the API origin", async () => {
    const fake = await startFakeWorkspace();
    const { client } = clientFor(fake, {
      ...running,
      transport: { websocket_path: "//evil.example.com/ws", preview_token: PREVIEW_TOKEN },
    });

    const exit = await Effect.runPromiseExit(
      Effect.scoped(connectAetherWorkspace({ client, workspaceId: WORKSPACE_ID, start: false })),
    );

    expect(failureOf(exit)).toMatchObject({ _tag: "AetherWorkspaceSocketOpenError" });
    expect(fake.upgrades).toEqual([]);
  });
});

import * as Net from "node:net";
import type { AddressInfo } from "node:net";

import { AetherApiBaseUrl } from "@synara/contracts";
import { Cause, Effect, Exit, Fiber, Option } from "effect";
import { afterEach, describe, expect, it } from "vitest";
import WebSocket, { WebSocketServer } from "ws";

import { makeAetherApiClient } from "../Layers/AetherApiClient.ts";
import { WORKSPACE_ID } from "./testing/aetherApiFixtures.ts";
import {
  connectAetherWorkspace,
  type AetherWorkspaceSocket,
  type AetherWorkspaceSocketOptions,
} from "./aetherWorkspaceSocket.ts";

const API_KEY = "aether-secret-key";
const WEBSOCKET_PATH = `/workspaces/${WORKSPACE_ID}/ws`;

type Frame = Record<string, unknown>;

interface FakeWorkspace {
  readonly apiBaseUrl: AetherApiBaseUrl;
  readonly upgrades: string[];
  readonly received: Frame[];
  readonly closes: Array<{ readonly code: number; readonly reason: string }>;
  /** Answers each client frame; return nothing to stay silent. */
  respond: (frame: Frame, socket: WebSocket) => void;
}

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  await Promise.all(cleanups.splice(0).map((cleanup) => cleanup()));
});

async function startFakeWorkspace(refuseUpgradeWith?: number): Promise<FakeWorkspace> {
  const server = new WebSocketServer({
    host: "127.0.0.1",
    port: 0,
    ...(refuseUpgradeWith === undefined
      ? {}
      : { verifyClient: (_info, done) => done(false, refuseUpgradeWith, "Refused") }),
  });
  await new Promise<void>((resolve) => server.once("listening", () => resolve()));
  const fake: FakeWorkspace = {
    apiBaseUrl: AetherApiBaseUrl.makeUnsafe(
      `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
    ),
    upgrades: [],
    received: [],
    closes: [],
    respond: () => undefined,
  };
  server.on("connection", (socket, request) => {
    fake.upgrades.push(request.url ?? "");
    socket.on("message", (data) => {
      const frame = JSON.parse(String(data)) as Frame;
      fake.received.push(frame);
      fake.respond(frame, socket);
    });
    socket.on("close", (code, reason) => fake.closes.push({ code, reason: String(reason) }));
  });
  cleanups.push(
    () =>
      new Promise((resolve) => {
        for (const client of server.clients) client.terminate();
        server.close(() => resolve());
      }),
  );
  return fake;
}

function connect(
  apiBaseUrl: AetherApiBaseUrl,
  connectBody: unknown = { state: "running", transport: { websocket_path: WEBSOCKET_PATH } },
  options: AetherWorkspaceSocketOptions = {},
) {
  const client = makeAetherApiClient({
    apiBaseUrl: Effect.succeed(apiBaseUrl),
    readApiKey: Effect.succeed(API_KEY),
    request: async () => ({
      status: 200,
      headers: new Headers(),
      body: new TextEncoder().encode(JSON.stringify(connectBody)),
      url: "",
    }),
  });
  return connectAetherWorkspace({ client, workspaceId: WORKSPACE_ID, start: false, options });
}

/** Connects, runs `use` with the socket, then closes the scope. */
function withSocket<A, E>(
  fake: FakeWorkspace,
  use: (socket: AetherWorkspaceSocket) => Effect.Effect<A, E>,
  options: AetherWorkspaceSocketOptions = {},
) {
  return Effect.runPromiseExit(
    Effect.scoped(
      Effect.gen(function* () {
        const attach = yield* connect(fake.apiBaseUrl, undefined, options);
        if (attach._tag !== "Connected") throw new Error("not connected");
        return yield* use(attach.socket);
      }),
    ),
  );
}

function failureOf<A, E>(exit: Exit.Exit<A, E>): E {
  if (Exit.isSuccess(exit)) throw new Error("expected a failure");
  const error = Cause.findErrorOption(exit.cause);
  if (Option.isNone(error)) throw new Error(Cause.pretty(exit.cause));
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
  diff: { baseRef: "3f2c9d8e7b6a5f4e3d2c1b0a9f8e7d6c5b4a3f2e", files },
});
const paths = { oldPath: "src/a.ts", newPath: "src/a.ts", displayPath: "src/a.ts" };
const textFile = {
  ...paths,
  status: "modified",
  kind: "text",
  hunks: [
    {
      header: "@@ -1,2 +1,2 @@",
      lines: [
        { kind: "context", oldLine: 1, newLine: 1, text: "keep" },
        { kind: "del", oldLine: 2, newLine: null, text: "old" },
        { kind: "add", oldLine: null, newLine: 2, text: "new", noTrailingNewline: true },
      ],
    },
  ],
};
const binaryFile = { ...paths, status: "added", kind: "binary", hunks: [] };
const tooLargeFile = { ...paths, status: "modified", kind: "too_large", maxBytes: 1_048_576 };

describe("Aether workspace socket", () => {
  it("authenticates with ?token= on the transport path and correlates concurrent requests", async () => {
    const fake = await startFakeWorkspace();
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
                  requestId: pending.requestId,
                  success: true,
                  content: "aGk=",
                  encoding: "base64",
                },
          ),
        );
      }
    };

    const [diff, file] = valueOf(
      await withSocket(fake, (socket) =>
        Effect.all([socket.requestGitDiff("main"), socket.readFile("logo.png")], {
          concurrency: "unbounded",
        }),
      ),
    );

    expect(diff.files.map((entry) => entry.kind)).toEqual(["text"]);
    expect(file).toEqual({ success: true, content: "aGk=", encoding: "base64" });
    const upgrade = new URL(fake.upgrades[0] ?? "", "http://placeholder");
    expect(upgrade.pathname).toBe(WEBSOCKET_PATH);
    expect(upgrade.searchParams.get("token")).toBe(API_KEY);
    expect(fake.received).toContainEqual({
      channel: "git",
      type: "diff",
      mode: "main",
      requestId: expect.stringMatching(/^synara-/u),
    });
    expect(new Set(fake.received.map((frame) => frame.requestId)).size).toBe(2);
  });

  it("decodes text, binary and too_large diff files", async () => {
    const fake = await startFakeWorkspace();
    fake.respond = (frame, socket) =>
      socket.send(
        JSON.stringify(diffAnswer(frame.requestId, [textFile, binaryFile, tooLargeFile])),
      );

    const diff = valueOf(await withSocket(fake, (socket) => socket.requestGitDiff("main")));

    expect(diff.files).toEqual([
      {
        oldPath: "src/a.ts",
        newPath: "src/a.ts",
        status: "modified",
        kind: "text",
        hunks: [
          {
            header: "@@ -1,2 +1,2 @@",
            lines: [
              { kind: "context", text: "keep" },
              { kind: "del", text: "old" },
              { kind: "add", text: "new", noTrailingNewline: true },
            ],
          },
        ],
      },
      { oldPath: "src/a.ts", newPath: "src/a.ts", status: "added", kind: "binary", hunks: [] },
      {
        oldPath: "src/a.ts",
        newPath: "src/a.ts",
        status: "modified",
        kind: "too_large",
        maxBytes: 1_048_576,
      },
    ]);
  });

  it("rejects a diff file without a known kind, like the retired isBinary shape", async () => {
    for (const file of [
      { ...binaryFile, kind: undefined, isBinary: true },
      { ...binaryFile, kind: "lfs" },
    ]) {
      const fake = await startFakeWorkspace();
      fake.respond = (frame, socket) =>
        socket.send(JSON.stringify(diffAnswer(frame.requestId, [file])));

      const error = failureOf(await withSocket(fake, (socket) => socket.requestGitDiff("main")));

      expect(error).toMatchObject({ _tag: "AetherWorkspaceRequestError", reason: "malformed" });
      expect(error._tag === "AetherWorkspaceRequestError" && error.detail).toContain(
        '["diff"]["files"][0]',
      );
    }
  });

  it("reports success: false answers as failed requests", async () => {
    const fake = await startFakeWorkspace();
    fake.respond = (frame, socket) =>
      socket.send(
        JSON.stringify({ requestId: frame.requestId, success: false, error: "no such file" }),
      );

    const [diffError, readError] = valueOf(
      await withSocket(fake, (socket) =>
        Effect.all([
          socket.requestGitDiff("lastCommit").pipe(Effect.flip),
          socket.readFile("x").pipe(Effect.flip),
        ]),
      ),
    );

    expect(diffError).toMatchObject({
      request: "git.diff",
      reason: "failed",
      detail: "no such file",
    });
    expect(readError).toMatchObject({
      request: "files.read",
      reason: "failed",
      detail: "no such file",
    });
  });

  it("times out a request, citing the workspace's last error frame, and drops the late answer", async () => {
    const fake = await startFakeWorkspace();
    let calls = 0;
    fake.respond = (frame, socket) => {
      calls += 1;
      if (calls === 1) {
        socket.send(JSON.stringify({ channel: "error", type: "error", error: "Invalid message" }));
        // Arrives after the first request timed out, while the second is pending.
        setTimeout(
          () => socket.send(JSON.stringify(diffAnswer(frame.requestId, [binaryFile]))),
          250,
        );
        return;
      }
      setTimeout(() => socket.send(JSON.stringify(diffAnswer(frame.requestId, [textFile]))), 100);
    };

    const { first, second } = valueOf(
      await withSocket(
        fake,
        (socket) =>
          Effect.gen(function* () {
            const first = yield* socket.requestGitDiff("main").pipe(Effect.flip);
            const second = yield* socket.requestGitDiff("main");
            return { first, second };
          }),
        { requestTimeoutMs: 200 },
      ),
    );

    expect(first).toMatchObject({
      _tag: "AetherWorkspaceRequestError",
      reason: "timeout",
      detail: "No answer within 200ms; the workspace reported: Invalid message.",
    });
    expect(second.files.map((file) => file.kind)).toEqual(["text"]);
  });

  it("sends user_activity as soon as the keepalive starts", async () => {
    const fake = await startFakeWorkspace();

    valueOf(
      await withSocket(fake, (socket) =>
        Effect.gen(function* () {
          const keepAlive = yield* Effect.forkChild(socket.keepAlive);
          yield* Effect.sleep(100);
          yield* Fiber.interrupt(keepAlive);
        }),
      ),
    );

    expect(fake.received).toEqual([{ channel: "activity", type: "user_activity" }]);
  });

  it("closes with a normal closure with the scope, and refuses requests afterwards", async () => {
    const fake = await startFakeWorkspace();
    let captured: AetherWorkspaceSocket | undefined;

    valueOf(await withSocket(fake, (socket) => Effect.sync(() => (captured = socket))));
    await new Promise((resolve) => setTimeout(resolve, 20));

    expect(fake.closes).toEqual([{ code: 1000, reason: "Synara closed the workspace connection" }]);
    if (captured === undefined) throw new Error("no socket");
    expect(await Effect.runPromise(captured.closed)).toMatchObject({ code: 1000 });
    expect(failureOf(await Effect.runPromiseExit(captured.requestGitDiff("main")))).toMatchObject({
      _tag: "AetherWorkspaceSocketError",
      reason: "closed",
    });
  });

  it("fails an in-flight request when the workspace closes the socket", async () => {
    const fake = await startFakeWorkspace();
    fake.respond = (_frame, socket) => socket.close(4001, "workspace suspending");

    const { error, closed } = valueOf(
      await withSocket(fake, (socket) =>
        Effect.gen(function* () {
          const error = yield* socket.requestGitDiff("main").pipe(Effect.flip);
          return { error, closed: yield* socket.closed };
        }),
      ),
    );

    expect(error).toMatchObject({ _tag: "AetherWorkspaceSocketError", reason: "closed" });
    expect(closed).toEqual({ code: 4001, reason: "workspace suspending" });
  });

  it("surfaces a socket error after open on the in-flight request and through closed", async () => {
    const fake = await startFakeWorkspace();
    // A text frame whose payload is not UTF-8: the client's `ws` rejects it with an error.
    fake.respond = (_frame, socket) =>
      (socket as unknown as { _socket: Net.Socket })._socket.write(Buffer.from([0x81, 0x01, 0xff]));

    const { error, closed } = valueOf(
      await withSocket(fake, (socket) =>
        Effect.gen(function* () {
          const error = yield* socket.requestGitDiff("main").pipe(Effect.flip);
          return { error, closed: yield* socket.closed };
        }),
      ),
    );

    expect(error).toMatchObject({ _tag: "AetherWorkspaceSocketError", reason: "closed" });
    expect(error._tag === "AetherWorkspaceSocketError" && error.cause).toBeInstanceOf(Error);
    expect(closed.error).toBeInstanceOf(Error);
  });

  it("fails to open when the upgrade is refused, with the status and without the key", async () => {
    const fake = await startFakeWorkspace(401);

    const error = failureOf(await withSocket(fake, () => Effect.void));

    expect(error).toMatchObject({
      _tag: "AetherWorkspaceSocketError",
      reason: "open-failed",
      status: 401,
    });
    expect(JSON.stringify(error) + error.message).not.toContain(API_KEY);
  });

  it("returns Connecting without opening a socket, and refuses a path off the API origin", async () => {
    const fake = await startFakeWorkspace();

    const connecting = await Effect.runPromiseExit(
      Effect.scoped(connect(fake.apiBaseUrl, { state: "connecting", retry_after_ms: 1500 })),
    );
    const offOrigin = await Effect.runPromiseExit(
      Effect.scoped(
        connect(fake.apiBaseUrl, {
          state: "running",
          transport: { websocket_path: "//evil.example.com/ws" },
        }),
      ),
    );

    expect(valueOf(connecting)).toEqual({ _tag: "Connecting", retryAfterMs: 1500 });
    expect(failureOf(offOrigin)).toMatchObject({ reason: "open-failed" });
    expect(fake.upgrades).toEqual([]);
  });

  it("gives up opening when the server accepts the connection but never upgrades it", async () => {
    const held: Net.Socket[] = [];
    const tcp = Net.createServer((socket) => held.push(socket));
    await new Promise<void>((resolve) => tcp.listen(0, "127.0.0.1", resolve));
    cleanups.push(async () => {
      for (const socket of held) socket.destroy();
      await new Promise((resolve) => tcp.close(resolve));
    });
    const port = (tcp.address() as AddressInfo).port;

    const exit = await Effect.runPromiseExit(
      Effect.scoped(
        connect(AetherApiBaseUrl.makeUnsafe(`http://127.0.0.1:${port}`), undefined, {
          openTimeoutMs: 150,
        }),
      ),
    );

    expect(failureOf(exit)).toMatchObject({
      reason: "open-failed",
      detail: "The socket did not open within 150ms.",
    });
    expect(held).toHaveLength(1);
  });
});

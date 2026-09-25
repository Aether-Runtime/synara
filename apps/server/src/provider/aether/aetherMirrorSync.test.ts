// FILE: aetherMirrorSync.test.ts
// Purpose: The Aether mirror sync against real Git: a repository with a linked worktree the
// provider claims, and a fake workspace socket serving cumulative diffs in their wire shape.
// Layer: Server provider tests (Aether).
import * as fs from "node:fs/promises";
import path from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import { ThreadId } from "@synara/contracts";
import { Deferred, Effect, Fiber, FileSystem, Layer } from "effect";
import { describe, expect, it as vitestIt } from "vitest";

import { ServerConfig } from "../../config.ts";
import { GitCoreLive } from "../../git/Layers/GitCore.ts";
import { GitCore, type GitCoreShape, type WorktreeOwner } from "../../git/Services/GitCore.ts";
import { AetherTaskId } from "./aetherApiSchemas.ts";
import { commandLineBatches, syncAetherMirror } from "./aetherMirrorSync.ts";
import type { AetherGitDiffFile } from "./aetherWorkspaceProtocol.ts";
import {
  added,
  binary,
  deleted,
  diffOf,
  hunk,
  makeFakeWorkspace,
  modeOnly,
  modified,
  NO_NEWLINE,
  renamed,
  tooLarge,
} from "./testing/aetherMirrorFixtures.ts";

const TestLayer = Layer.mergeAll(
  NodeServices.layer,
  GitCoreLive.pipe(
    Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "synara-aether-mirror-test-" })),
    Layer.provide(NodeServices.layer),
  ),
);

const isWindows = process.platform === "win32";
const aetherOwner: WorktreeOwner = {
  threadId: ThreadId.makeUnsafe("thread-aether-mirror"),
  provider: "aether",
};
const TASK = AetherTaskId.makeUnsafe("6a3e1f0c-2b4d-4e8f-9a1b-3c5d7e9f1a2b");
const OTHER_TASK = AetherTaskId.makeUnsafe("1f2e3d4c-5b6a-4978-8a9b-0c1d2e3f4a5b");

const LOGO = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x00, 0xff, 0x10]);
const OLD_BIN = Buffer.from([0x00, 0x01, 0x02]);
const ICON = Buffer.from([0xfe, 0xed, 0x00]);

const BASE: Readonly<Record<string, string | Buffer>> = {
  ".gitignore": "build/\n",
  "app.txt": "one\ntwo\nthree\n",
  "lib.txt": "lib\n",
  "docs/guide.md": "# Guide\n\nRead me.\n",
  "tools/run.sh": "#!/bin/sh\necho run\n",
  "crlf.txt": "one\r\ntwo\r\n",
  "eof.txt": "last",
  "assets/logo.bin": LOGO,
  "assets/old.bin": OLD_BIN,
  "assets/icon.bin": ICON,
};
const EXECUTABLES = ["tools/run.sh", "assets/logo.bin"];

const hex = (bytes: Uint8Array) => `hex:${Buffer.from(bytes).toString("hex")}`;
const BASE_TREE: Readonly<Record<string, string>> = Object.fromEntries(
  Object.entries(BASE).map(([file, content]) => [
    file,
    typeof content === "string" ? content : hex(content),
  ]),
);
const without = (tree: Readonly<Record<string, string>>, ...files: ReadonlyArray<string>) =>
  Object.fromEntries(Object.entries(tree).filter(([file]) => !files.includes(file)));

const git = (cwd: string, args: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const gitCore = yield* GitCore;
    const result = yield* gitCore.execute({
      operation: "aetherMirrorSync.test",
      cwd,
      args,
      timeoutMs: 15_000,
    });
    return result.stdout.trim();
  });

/** Wait in real time (the test clock does not move on its own). */
const settle = (ms: number) => Effect.promise(() => new Promise((done) => setTimeout(done, ms)));

/** GitCore whose worktree move (`read-tree -m -u`) waits until `release` completes. */
const pauseBeforeMove = Effect.gen(function* () {
  const gitCore = yield* GitCore;
  const reached = yield* Deferred.make<void>();
  const release = yield* Deferred.make<void>();
  const paused: GitCoreShape = {
    ...gitCore,
    execute: (input) =>
      input.args[0] === "read-tree" && input.args.includes("-u")
        ? Deferred.succeed(reached, undefined).pipe(
            Effect.andThen(Deferred.await(release)),
            Effect.andThen(gitCore.execute(input)),
          )
        : gitCore.execute(input),
  };
  return { gitCore, paused, reached, release };
});

/**
 * A repository on `main` with a linked worktree on its own branch, claimed by the Aether
 * thread for the test's scope, and a fake workspace to serve diffs from.
 */
const makeMirror = (options: { readonly symlinks?: Readonly<Record<string, string>> } = {}) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const gitCore = yield* GitCore;
    const root = yield* fileSystem
      .makeTempDirectoryScoped({ prefix: "aether-mirror-" })
      .pipe(Effect.flatMap((dir) => Effect.promise(() => fs.realpath(dir))));
    const repo = path.join(root, "repo");
    yield* Effect.promise(async () => {
      for (const [file, content] of Object.entries(BASE)) {
        await fs.mkdir(path.dirname(path.join(repo, file)), { recursive: true });
        await fs.writeFile(path.join(repo, file), content);
      }
      if (!isWindows) {
        for (const file of EXECUTABLES) await fs.chmod(path.join(repo, file), 0o755);
      }
      for (const [link, target] of Object.entries(options.symlinks ?? {})) {
        await fs.symlink(target, path.join(repo, link));
      }
    });
    yield* git(repo, ["init", "--initial-branch=main"]);
    yield* git(repo, ["config", "user.email", "test@test.com"]);
    yield* git(repo, ["config", "user.name", "Test"]);
    yield* git(repo, ["config", "core.autocrlf", "false"]);
    yield* git(repo, ["add", "-A"]);
    yield* git(repo, ["commit", "-m", "base"]);
    const worktree = path.join(root, "owned");
    yield* git(repo, ["worktree", "add", "-b", "feature/owned", worktree]);
    const baseCommit = yield* git(worktree, ["rev-parse", "HEAD"]);
    const claim = yield* gitCore.claimWorktree(worktree, aetherOwner);
    const fake = makeFakeWorkspace();

    const inWorktree = (file: string) => path.join(worktree, file);
    return {
      root,
      repo,
      worktree,
      baseCommit,
      fake,
      serve: (
        files: ReadonlyArray<AetherGitDiffFile>,
        bytes: Record<string, Uint8Array | string> = {},
      ) => fake.serve(diffOf(baseCommit, ...files), bytes),
      sync: (taskId: AetherTaskId = TASK) =>
        syncAetherMirror({ workspace: fake.workspace, claim, taskId, baseCommit }),
      /** The sync's `AetherMirrorError`; anything else fails the test. */
      failure: (taskId: AetherTaskId = TASK) =>
        syncAetherMirror({ workspace: fake.workspace, claim, taskId, baseCommit }).pipe(
          Effect.flip,
          Effect.flatMap((error) =>
            error._tag === "AetherMirrorError" ? Effect.succeed(error) : Effect.die(error),
          ),
        ),
      /** Tracked and untracked files (ignored ones excluded); `assets/` as hex, links as targets. */
      tree: Effect.gen(function* () {
        const listed = yield* git(worktree, ["ls-files", "-co", "--exclude-standard", "-z"]);
        const files = [...new Set(listed.split("\0").filter((file) => file.length > 0))].toSorted();
        return yield* Effect.promise(async () => {
          const entries: Record<string, string> = {};
          for (const file of files) {
            const stat = await fs.lstat(inWorktree(file)).catch(() => undefined);
            if (stat === undefined || stat.isDirectory()) continue;
            entries[file] = stat.isSymbolicLink()
              ? `link:${await fs.readlink(inWorktree(file))}`
              : file.startsWith("assets/")
                ? hex(await fs.readFile(inWorktree(file)))
                : await fs.readFile(inWorktree(file), "utf8");
          }
          return entries;
        });
      }),
      read: (file: string) => Effect.promise(() => fs.readFile(inWorktree(file), "utf8")),
      write: (file: string, content: string) =>
        Effect.promise(async () => {
          await fs.mkdir(path.dirname(inWorktree(file)), { recursive: true });
          await fs.writeFile(inWorktree(file), content);
        }),
      exists: (file: string) =>
        Effect.promise(() =>
          fs.lstat(file).then(
            () => true,
            () => false,
          ),
        ),
      stat: (file: string) => Effect.promise(() => fs.lstat(inWorktree(file))),
      recordFile: git(worktree, ["rev-parse", "--absolute-git-dir"]).pipe(
        Effect.map((gitDir) => path.join(gitDir, "synara-aether-mirror.json")),
      ),
    };
  });

describe("commandLineBatches", () => {
  vitestIt("keeps groups whole and every batch within the budget", () => {
    const groups = Array.from({ length: 50 }, (_, index) => ["--cacheinfo", `100644,oid,${index}`]);
    const batches = commandLineBatches(groups, 100);
    expect(batches.length).toBeGreaterThan(1);
    expect(batches.flat()).toEqual(groups.flat());
    for (const batch of batches) expect(batch.join(" ").length).toBeLessThanOrEqual(100);
    expect(batches.every((batch) => batch.length % 2 === 0)).toBe(true);
  });

  vitestIt("still sends a single group longer than the budget", () => {
    expect(commandLineBatches([["x".repeat(20)], ["y"]], 10)).toEqual([["x".repeat(20)], ["y"]]);
  });
});

it.layer(TestLayer)("syncAetherMirror", (it) => {
  describe("text files", () => {
    it.effect("mirrors modified, added, deleted and renamed files onto the task base", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        yield* mirror.write("build/cache.txt", "ignored artifact\n");
        mirror.serve([
          modified("app.txt", hunk(1, 1, " one", "-two", "+TWO", " three")),
          added("notes/new.md", "hello\n"),
          deleted("lib.txt", "lib\n"),
          renamed(
            "docs/guide.md",
            "docs/manual.md",
            hunk(1, 1, " # Guide", " ", "-Read me.", "+Read the manual."),
          ),
        ]);

        const result = yield* mirror.sync();

        expect(result).toMatchObject({ _tag: "synced", filesChanged: 4, modeOnlyPaths: [] });
        expect(yield* mirror.tree).toEqual({
          ...without(BASE_TREE, "lib.txt", "docs/guide.md"),
          "app.txt": "one\nTWO\nthree\n",
          "notes/new.md": "hello\n",
          "docs/manual.md": "# Guide\n\nRead the manual.\n",
        });
        // The index holds the target; HEAD stays on the base; ignored files are untouched.
        expect(yield* git(mirror.worktree, ["write-tree"])).toBe(result.tree);
        expect(yield* git(mirror.worktree, ["rev-parse", "HEAD"])).toBe(mirror.baseCommit);
        expect(yield* mirror.read("build/cache.txt")).toBe("ignored artifact\n");
      }),
    );

    it.effect.skipIf(isWindows)("applies C-quoted and unicode paths", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        const names = ['we"ird\\name.txt', "tab\there.txt", "with space.txt", "ünïcödé/日本語.txt"];
        mirror.serve([
          ...names.map((name) => added(name, `${name}\n`)),
          renamed("lib.txt", 'lib "quoted".txt', hunk(1, 1, " lib", "+more")),
        ]);

        expect((yield* mirror.sync())._tag).toBe("synced");
        for (const name of names) expect(yield* mirror.read(name)).toBe(`${name}\n`);
        expect(yield* mirror.read('lib "quoted".txt')).toBe("lib\nmore\n");

        mirror.serve([
          added('we"ird\\name.txt', 'we"ird\\name.txt\n'),
          added("ünïcödé/日本語.txt", "こんにちは\n"),
        ]);
        expect((yield* mirror.sync())._tag).toBe("synced");
        expect(yield* mirror.tree).toEqual({
          ...BASE_TREE,
          'we"ird\\name.txt': 'we"ird\\name.txt\n',
          "ünïcödé/日本語.txt": "こんにちは\n",
        });
      }),
    );

    it.effect("keeps CRLF line endings and a missing final newline", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.serve([
          modified("crlf.txt", hunk(1, 1, " one\r", "-two\r", "+TWO\r")),
          modified("eof.txt", hunk(1, 1, "-last", NO_NEWLINE, "+last line", NO_NEWLINE)),
          added("new.txt", "no newline"),
        ]);

        expect((yield* mirror.sync())._tag).toBe("synced");
        expect(yield* mirror.read("crlf.txt")).toBe("one\r\nTWO\r\n");
        expect(yield* mirror.read("eof.txt")).toBe("last line");
        expect(yield* mirror.read("new.txt")).toBe("no newline");
      }),
    );

    it.effect("reports a mode-only change without applying it", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.serve([modeOnly("app.txt"), added("notes.md", "hi\n")]);

        expect(yield* mirror.sync()).toMatchObject({
          _tag: "synced",
          filesChanged: 1,
          modeOnlyPaths: ["app.txt"],
        });
        expect(yield* mirror.tree).toEqual({ ...BASE_TREE, "notes.md": "hi\n" });
        expect((yield* mirror.sync())._tag).toBe("unchanged");
      }),
    );

    it.effect.skipIf(isWindows)("keeps an executable's mode when it is modified", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.serve([
          modified("tools/run.sh", hunk(1, 1, " #!/bin/sh", "-echo run", "+echo ran")),
        ]);

        expect((yield* mirror.sync())._tag).toBe("synced");
        expect(yield* mirror.read("tools/run.sh")).toBe("#!/bin/sh\necho ran\n");
        expect((yield* mirror.stat("tools/run.sh")).mode & 0o111).not.toBe(0);
      }),
    );

    it.effect.skipIf(isWindows)("deletes a symlink, and restores it when the diff drops that", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror({ symlinks: { link: "app.txt" } });
        mirror.serve([deleted("link", "app.txt")]);
        expect((yield* mirror.sync())._tag).toBe("synced");
        expect(yield* mirror.exists(path.join(mirror.worktree, "link"))).toBe(false);

        mirror.serve([]);
        expect((yield* mirror.sync())._tag).toBe("synced");
        expect((yield* mirror.tree)["link"]).toBe("link:app.txt");
      }),
    );

    it.effect("turns a file into a directory and back", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.serve([deleted("lib.txt", "lib\n"), added("lib.txt/inner.txt", "x\n")]);
        expect((yield* mirror.sync())._tag).toBe("synced");
        expect(yield* mirror.tree).toEqual({
          ...without(BASE_TREE, "lib.txt"),
          "lib.txt/inner.txt": "x\n",
        });

        mirror.serve([added("notes.md", "turn two\n")]);
        expect((yield* mirror.sync())._tag).toBe("synced");
        expect(yield* mirror.tree).toEqual({ ...BASE_TREE, "notes.md": "turn two\n" });
      }),
    );
  });

  describe("binary files", () => {
    it.effect("writes added, modified, renamed and deleted binaries from the files channel", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        const newLogo = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x00, 0xff]);
        const fresh = Buffer.from([0x00, 0x01, 0x02, 0x03, 0xff, 0xfe]);
        const icon = Buffer.from([0xca, 0xfe]);
        mirror.serve(
          [
            binary("modified", "assets/logo.bin", "assets/logo.bin"),
            binary("added", "/dev/null", "assets/new/fresh.bin"),
            binary("deleted", "assets/old.bin", "/dev/null"),
            binary("renamed", "assets/icon.bin", "assets/icons/icon.bin"),
            added("notes.md", "next to binaries\n"),
          ],
          {
            "assets/logo.bin": newLogo,
            "assets/new/fresh.bin": fresh,
            "assets/icons/icon.bin": icon,
          },
        );

        expect(yield* mirror.sync()).toMatchObject({ _tag: "synced", filesChanged: 5 });
        expect(yield* mirror.tree).toEqual({
          ...without(BASE_TREE, "assets/old.bin", "assets/icon.bin"),
          "assets/logo.bin": hex(newLogo),
          "assets/new/fresh.bin": hex(fresh),
          "assets/icons/icon.bin": hex(icon),
          "notes.md": "next to binaries\n",
        });
        expect(mirror.fake.reads).toEqual([
          "assets/logo.bin",
          "assets/new/fresh.bin",
          "assets/icons/icon.bin",
        ]);
        if (!isWindows) {
          expect((yield* mirror.stat("assets/logo.bin")).mode & 0o111).not.toBe(0);
        }

        mirror.serve([added("notes.md", "text only\n")]);
        expect((yield* mirror.sync())._tag).toBe("synced");
        expect(yield* mirror.tree).toEqual({ ...BASE_TREE, "notes.md": "text only\n" });
      }),
    );

    it.effect("keeps every Git command line short when a diff adds hundreds of binaries", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        const gitCore = yield* GitCore;
        const commandLines: Array<{
          readonly args: ReadonlyArray<string>;
          readonly length: number;
        }> = [];
        const measured: GitCoreShape = {
          ...gitCore,
          execute: (input) => {
            commandLines.push({ args: input.args, length: input.args.join(" ").length });
            return gitCore.execute(input);
          },
        };
        const files = Array.from(
          { length: 300 },
          (_, index) =>
            `assets/icon-set/icon-${String(index).padStart(3, "0")}-with-a-longer-name.bin`,
        );
        mirror.serve(
          files.map((file) => binary("added", "/dev/null", file)),
          Object.fromEntries(files.map((file, index) => [file, Buffer.from([index % 256, 0xff])])),
        );

        const result = yield* mirror.sync().pipe(Effect.provideService(GitCore, measured));
        expect(result).toMatchObject({ _tag: "synced", filesChanged: 300 });
        expect((yield* mirror.tree)[files[299] ?? ""]).toBe(hex(Buffer.from([299 % 256, 0xff])));
        expect(Math.max(...commandLines.map((line) => line.length))).toBeLessThan(10_000);
        const adds = commandLines.filter(
          (line) => line.args.includes("update-index") && line.args.includes("--add"),
        );
        expect(adds.length).toBeGreaterThan(1);
      }),
    );

    it.effect("applies a deletion and a rename onto the same path", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.serve(
          [
            binary("renamed", "assets/icon.bin", "assets/logo.bin"),
            binary("deleted", "assets/logo.bin", "/dev/null"),
          ],
          { "assets/logo.bin": ICON },
        );

        expect((yield* mirror.sync())._tag).toBe("synced");
        expect(yield* mirror.tree).toEqual({
          ...without(BASE_TREE, "assets/icon.bin"),
          "assets/logo.bin": hex(ICON),
        });
      }),
    );
  });

  describe("refusals", () => {
    it.effect("refuses a too-large file and changes nothing", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.serve([added("notes.md", "hi\n"), tooLarge("data/big.json", 1_048_576)]);

        const error = yield* mirror.failure();
        expect(error).toMatchObject({ reason: "too-large", paths: ["data/big.json"] });
        expect(error.detail).toContain("1048576");
        expect(yield* mirror.tree).toEqual(BASE_TREE);
      }),
    );

    it.effect("refuses diff paths that leave the worktree or name .git, changing nothing", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        const sentinel = path.join(mirror.root, "outside-sentinel.txt");
        yield* Effect.promise(() => fs.writeFile(sentinel, "sentinel\n"));
        const escapes = [
          binary("added", "/dev/null", sentinel),
          added("../escape.txt", "x\n"),
          binary("added", "/dev/null", ".git/hooks/post-checkout"),
          binary("deleted", "../outside-sentinel.txt", "/dev/null"),
          renamed("lib.txt", "../lib.txt"),
          // Names Windows or macOS resolve to `.git`: Git refuses them on every platform.
          binary("added", "/dev/null", ".git./config"),
          binary("added", "/dev/null", ".git /config"),
          binary("added", "/dev/null", "GIT~1/hooks/post-checkout"),
          binary("added", "/dev/null", "a/.git::$INDEX_ALLOCATION/x"),
          binary("added", "/dev/null", ".g‌it/config"),
          added(".git./hooks/pre-commit", "x\n"),
        ];

        for (const escape of escapes) {
          mirror.serve([added("notes.md", "x\n"), escape], { [escape.newPath]: "payload" });
          const error = yield* mirror.failure();
          expect(error.reason, escape.newPath).toBe("path-escape");
          expect(yield* mirror.tree).toEqual(BASE_TREE);
        }
        expect(yield* Effect.promise(() => fs.readFile(sentinel, "utf8"))).toBe("sentinel\n");
        expect(yield* mirror.exists(path.join(mirror.root, "escape.txt"))).toBe(false);
      }),
    );

    it.effect.skipIf(isWindows)("refuses a path through a symlinked directory", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        const outside = path.join(mirror.root, "outside-dir");
        yield* Effect.promise(async () => {
          await fs.mkdir(outside);
          // Under the ignored `build/`, so the link is not local work the check would see.
          await fs.mkdir(path.join(mirror.worktree, "build"));
          await fs.symlink(outside, path.join(mirror.worktree, "build", "escape-link"));
        });
        mirror.serve([binary("added", "/dev/null", "build/escape-link/payload.bin")], {
          "build/escape-link/payload.bin": "payload",
        });

        expect(yield* mirror.failure()).toMatchObject({
          reason: "path-escape",
          paths: ["build/escape-link/payload.bin"],
        });
        expect(yield* Effect.promise(() => fs.readdir(outside))).toEqual([]);
        expect((yield* mirror.stat("build/escape-link")).isSymbolicLink()).toBe(true);
      }),
    );

    it.effect("refuses to overwrite an ignored local file where the diff adds one", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        yield* mirror.write("build/keep.bin", "mine\n");
        mirror.serve([binary("added", "/dev/null", "build/keep.bin")], { "build/keep.bin": LOGO });

        expect(yield* mirror.failure()).toMatchObject({
          reason: "local-edits",
          paths: ["build/keep.bin"],
        });
        expect(yield* mirror.read("build/keep.bin")).toBe("mine\n");

        mirror.serve([added("notes.md", "fine\n")]);
        expect((yield* mirror.sync())._tag).toBe("synced");
        expect(yield* mirror.read("build/keep.bin")).toBe("mine\n");
      }),
    );

    it.effect("refuses an ignored local file where the diff needs a directory", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        yield* mirror.write("build/out", "mine\n");
        mirror.serve([added("build/out/x.txt", "theirs\n")]);

        expect(yield* mirror.failure()).toMatchObject({
          reason: "local-edits",
          paths: ["build/out"],
        });
        expect(yield* mirror.read("build/out")).toBe("mine\n");
      }),
    );

    it.effect("refuses a diff from another merge-base", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.fake.serve(diffOf("f".repeat(40), added("notes.md", "hi\n")));

        expect(yield* mirror.failure()).toMatchObject({ reason: "base-mismatch" });
        expect(yield* mirror.tree).toEqual(BASE_TREE);
      }),
    );

    it.effect("keeps the last mirror when a diff does not apply to the base", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.serve([added("notes.md", "turn one\n")]);
        expect((yield* mirror.sync())._tag).toBe("synced");

        mirror.serve([modified("app.txt", hunk(1, 1, " not", "-what", "+is there"))]);
        expect(yield* mirror.failure()).toMatchObject({ reason: "apply-failed" });
        expect(yield* mirror.tree).toEqual({ ...BASE_TREE, "notes.md": "turn one\n" });

        mirror.serve([added("notes.md", "turn three\n")]);
        expect((yield* mirror.sync())._tag).toBe("synced");
      }),
    );

    it.effect("fails with a transport error and changes nothing when the socket fails", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.fake.failDiff("The workspace socket closed (code 1006).");
        expect(yield* mirror.failure()).toMatchObject({ reason: "transport" });

        mirror.serve([added("notes.md", "x\n"), binary("added", "/dev/null", "assets/new.bin")], {
          "assets/new.bin": LOGO,
        });
        mirror.fake.failRead("No answer within 30000ms.");
        expect(yield* mirror.failure()).toMatchObject({
          reason: "transport",
          paths: ["assets/new.bin"],
        });
        expect(yield* mirror.tree).toEqual(BASE_TREE);
      }),
    );
  });

  describe("turns", () => {
    it.effect("applies each turn's larger cumulative diff onto the base", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.serve([
          modified("app.txt", hunk(1, 1, " one", "-two", "+TWO", " three")),
          added("notes.md", "hello\n"),
        ]);
        expect((yield* mirror.sync())._tag).toBe("synced");

        mirror.serve([
          modified("app.txt", hunk(1, 1, " one", "-two", "-three", "+TWO", "+THREE", "+four")),
          added("notes.md", "hello\nworld\n"),
          added("src/gen/fresh.txt", "new in turn two\n"),
          deleted("lib.txt", "lib\n"),
        ]);
        expect(yield* mirror.sync()).toMatchObject({ _tag: "synced", filesChanged: 4 });
        expect(yield* mirror.tree).toEqual({
          ...without(BASE_TREE, "lib.txt"),
          "app.txt": "one\nTWO\nTHREE\nfour\n",
          "notes.md": "hello\nworld\n",
          "src/gen/fresh.txt": "new in turn two\n",
        });

        // Turn 3: the agent reverted its notes and restored lib.txt.
        mirror.serve([modified("app.txt", hunk(1, 1, " one", "-two", "+TWO", " three"))]);
        expect((yield* mirror.sync())._tag).toBe("synced");
        expect(yield* mirror.tree).toEqual({ ...BASE_TREE, "app.txt": "one\nTWO\nthree\n" });
        expect(yield* mirror.exists(path.join(mirror.worktree, "src"))).toBe(false);

        // Turn 4 changed nothing: the worktree is not rewritten.
        const before = yield* mirror.stat("app.txt");
        yield* settle(20);
        expect((yield* mirror.sync())._tag).toBe("unchanged");
        const after = yield* mirror.stat("app.txt");
        expect([after.ino, after.mtimeMs]).toEqual([before.ino, before.mtimeMs]);
      }),
    );

    it.effect("reports an empty first turn as unchanged", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.serve([]);
        const result = yield* mirror.sync();
        expect(result).toMatchObject({
          _tag: "unchanged",
          tree: yield* git(mirror.worktree, ["rev-parse", "HEAD^{tree}"]),
        });
      }),
    );

    it.effect("keeps a local file that only the mirrored .gitignore ignores", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        const ignoreEnv = modified(".gitignore", hunk(1, 1, " build/", "+.env.local"));
        mirror.serve([ignoreEnv]);
        expect((yield* mirror.sync())._tag).toBe("synced");

        yield* mirror.write(".env.local", "SECRET=1\n");
        mirror.serve([ignoreEnv, added("notes.md", "turn two\n")]);
        expect((yield* mirror.sync())._tag).toBe("synced");
        expect(yield* mirror.read(".env.local")).toBe("SECRET=1\n");
      }),
    );

    it.effect("reports local files the mirror stopped ignoring, and keeps them", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        yield* mirror.write("build/cache.txt", "artifact\n");
        const unignoreBuild = modified(".gitignore", hunk(1, 0, "-build/"));
        mirror.serve([unignoreBuild]);
        expect((yield* mirror.sync())._tag).toBe("synced");

        mirror.serve([unignoreBuild, added("notes.md", "turn two\n")]);
        expect(yield* mirror.failure()).toMatchObject({
          reason: "local-edits",
          paths: ["build/cache.txt"],
        });
        expect(yield* mirror.read("build/cache.txt")).toBe("artifact\n");
      }),
    );
  });

  describe("local edits", () => {
    it.effect("refuses to overwrite local edits, naming them, until they are moved away", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.serve([modified("app.txt", hunk(1, 1, " one", "-two", "+TWO", " three"))]);
        expect((yield* mirror.sync())._tag).toBe("synced");

        yield* mirror.write("app.txt", "my local edit\n");
        yield* mirror.write("scratch.txt", "mine\n");
        mirror.serve([added("notes.md", "turn two\n")]);
        expect(yield* mirror.failure()).toMatchObject({
          reason: "local-edits",
          paths: ["app.txt", "scratch.txt"],
        });
        expect(yield* mirror.read("app.txt")).toBe("my local edit\n");
        expect(yield* mirror.read("scratch.txt")).toBe("mine\n");

        // Stashing everything and resetting to the base loses nothing, and the mirror resumes.
        yield* git(mirror.worktree, ["stash", "push", "--include-untracked"]);
        yield* git(mirror.worktree, ["reset", "--hard", mirror.baseCommit]);
        expect((yield* mirror.sync())._tag).toBe("synced");
        expect(yield* mirror.tree).toEqual({ ...BASE_TREE, "notes.md": "turn two\n" });
      }),
    );

    it.effect("refuses a change that is only staged", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.serve([added("notes.md", "hello\n")]);
        expect((yield* mirror.sync())._tag).toBe("synced");

        yield* mirror.write("app.txt", "staged\n");
        yield* git(mirror.worktree, ["add", "app.txt"]);
        yield* mirror.write("app.txt", BASE["app.txt"] as string);

        expect(yield* mirror.failure()).toMatchObject({
          reason: "local-edits",
          paths: ["app.txt"],
        });
      }),
    );

    it.effect("refuses a local commit", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.serve([added("notes.md", "hello\n")]);
        expect((yield* mirror.sync())._tag).toBe("synced");
        yield* git(mirror.worktree, ["commit", "-m", "local commit"]);

        const error = yield* mirror.failure();
        expect(error).toMatchObject({ reason: "local-edits", paths: [] });
        expect(error.detail).toContain("HEAD moved");
      }),
    );

    it.effect("refuses edits made before the first sync", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        yield* mirror.write("lib.txt", "edited before any turn\n");
        mirror.serve([added("notes.md", "hello\n")]);

        expect(yield* mirror.failure()).toMatchObject({
          reason: "local-edits",
          paths: ["lib.txt"],
        });
        expect(yield* mirror.read("lib.txt")).toBe("edited before any turn\n");
      }),
    );

    it.effect("trusts its own record across calls, but never another task's", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.serve([added("notes.md", "hello\n")]);
        expect((yield* mirror.sync(TASK))._tag).toBe("synced");

        expect(yield* mirror.failure(OTHER_TASK)).toMatchObject({
          reason: "local-edits",
          paths: ["notes.md"],
        });
        expect((yield* mirror.sync(TASK))._tag).toBe("unchanged");
      }),
    );

    it.effect("fails loudly on a corrupt record", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.serve([added("notes.md", "hello\n")]);
        expect((yield* mirror.sync())._tag).toBe("synced");
        const recordFile = yield* mirror.recordFile;
        yield* Effect.promise(() => fs.writeFile(recordFile, "{not json"));

        expect(yield* mirror.failure()).toMatchObject({ reason: "record" });
        expect(yield* mirror.read("notes.md")).toBe("hello\n");
      }),
    );
  });

  describe("crashes and concurrent edits", () => {
    it.effect("accepts the tree a crashed sync wrote before it could record it", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.serve([added("notes.md", "turn one\n")]);
        const first = yield* mirror.sync();
        const turnTwo = [added("notes.md", "turn two\n"), deleted("lib.txt", "lib\n")];
        mirror.serve(turnTwo);
        const second = yield* mirror.sync();
        // The record as it stood between `read-tree` and the final write.
        const recordFile = yield* mirror.recordFile;
        yield* Effect.promise(() =>
          fs.writeFile(
            recordFile,
            JSON.stringify({ taskId: TASK, target: first.tree, pending: second.tree }),
          ),
        );

        expect(yield* mirror.sync()).toEqual({ _tag: "unchanged", tree: second.tree });
        mirror.serve([...turnTwo, added("fresh.txt", "turn three\n")]);
        expect((yield* mirror.sync())._tag).toBe("synced");
        expect(yield* mirror.tree).toEqual({
          ...without(BASE_TREE, "lib.txt"),
          "notes.md": "turn two\n",
          "fresh.txt": "turn three\n",
        });
      }),
    );

    it.effect("never reverts an edit made while a sync runs", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        const { paused, reached, release } = yield* pauseBeforeMove;
        mirror.serve([added("notes.md", "hello\n")]);
        const syncing = yield* mirror
          .sync()
          .pipe(Effect.provideService(GitCore, paused), Effect.forkChild);
        yield* Deferred.await(reached);
        yield* mirror.write("lib.txt", "edited during the sync\n");
        yield* Deferred.succeed(release, undefined);
        expect((yield* Fiber.join(syncing))._tag).toBe("synced");
        expect(yield* mirror.read("lib.txt")).toBe("edited during the sync\n");

        expect(yield* mirror.failure()).toMatchObject({
          reason: "local-edits",
          paths: ["lib.txt"],
        });
        expect(yield* mirror.read("lib.txt")).toBe("edited during the sync\n");
      }),
    );

    it.effect("stops when a file it is writing was edited during the sync", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        const { paused, reached, release } = yield* pauseBeforeMove;
        mirror.serve([modified("app.txt", hunk(1, 1, " one", "-two", "+TWO", " three"))]);
        const syncing = yield* mirror
          .sync()
          .pipe(Effect.provideService(GitCore, paused), Effect.forkChild);
        yield* Deferred.await(reached);
        yield* mirror.write("app.txt", "edited during the sync\n");
        yield* Deferred.succeed(release, undefined);

        expect(yield* Effect.flip(Fiber.join(syncing))).toMatchObject({ reason: "apply-failed" });
        expect(yield* mirror.read("app.txt")).toBe("edited during the sync\n");
        expect(yield* mirror.failure()).toMatchObject({
          reason: "local-edits",
          paths: ["app.txt"],
        });
      }),
    );
  });

  describe("ownership", () => {
    it.effect(
      "holds the repository lock as the owner: a non-owner mutation waits, then is refused",
      () =>
        Effect.gen(function* () {
          const mirror = yield* makeMirror();
          const { gitCore, paused, reached, release } = yield* pauseBeforeMove;
          mirror.serve([modified("app.txt", hunk(1, 1, " one", "-two", "+TWO", " three"))]);
          const syncing = yield* mirror
            .sync()
            .pipe(Effect.provideService(GitCore, paused), Effect.forkChild);
          yield* Deferred.await(reached);

          const ran: Array<string> = [];
          const intruder = yield* gitCore
            .withMutation(
              mirror.worktree,
              Effect.promise(() => fs.writeFile(path.join(mirror.worktree, "intruder.txt"), "x")),
            )
            .pipe(Effect.flip, Effect.forkChild);
          const neighbour = yield* gitCore
            .withMutation(
              mirror.repo,
              Effect.sync(() => ran.push("main checkout")),
            )
            .pipe(Effect.forkChild);
          yield* settle(200);
          // Both wait for the sync's lock on the shared repository.
          expect(ran).toEqual([]);

          yield* Deferred.succeed(release, undefined);
          expect((yield* Fiber.join(syncing))._tag).toBe("synced");
          yield* Fiber.join(neighbour);
          expect(ran).toEqual(["main checkout"]);
          expect(yield* Fiber.join(intruder)).toMatchObject({
            _tag: "WorktreeOwnedError",
            ownerThreadId: aetherOwner.threadId,
          });
          expect(yield* mirror.exists(path.join(mirror.worktree, "intruder.txt"))).toBe(false);
          expect(yield* mirror.read("app.txt")).toBe("one\nTWO\nthree\n");
        }),
    );
  });
});

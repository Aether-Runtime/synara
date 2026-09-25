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
import { describe, expect } from "vitest";

import { ServerConfig } from "../../config.ts";
import { GitCoreLive } from "../../git/Layers/GitCore.ts";
import { GitCore, type GitCoreShape, type WorktreeOwner } from "../../git/Services/GitCore.ts";
import { AetherTaskId } from "./aetherApiSchemas.ts";
import { syncAetherMirror } from "./aetherMirrorSync.ts";
import {
  added,
  binary,
  deleted,
  diffOf,
  hunk,
  makeFakeWorkspace,
  modeOnly,
  modified,
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
  "assets/logo.bin": LOGO,
  "assets/old.bin": OLD_BIN,
  "assets/icon.bin": ICON,
};

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
      if (!isWindows) await fs.chmod(path.join(repo, "assets/logo.bin"), 0o755);
      for (const [link, target] of Object.entries(options.symlinks ?? {})) {
        await fs.symlink(target, path.join(repo, link));
      }
    });
    yield* git(repo, ["init", "--initial-branch=main"]);
    yield* git(repo, ["config", "user.email", "test@test.com"]);
    yield* git(repo, ["config", "user.name", "Test"]);
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
      claim,
      fake,
      sync: (taskId: AetherTaskId = TASK) =>
        syncAetherMirror({ workspace: fake.workspace, claim, taskId, baseCommit }),
      /** Tracked and untracked files (ignored ones excluded); `assets/` as hex, links as targets. */
      tree: Effect.gen(function* () {
        const listed = yield* git(worktree, ["ls-files", "-co", "--exclude-standard", "-z"]);
        const files = [...new Set(listed.split("\0").filter((file) => file.length > 0))].toSorted();
        return yield* Effect.promise(async () => {
          const entries: Record<string, string> = {};
          for (const file of files) {
            const stat = await fs.lstat(inWorktree(file)).catch(() => undefined);
            if (stat === undefined) continue;
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
      stat: (file: string) => Effect.promise(() => fs.stat(inWorktree(file))),
    };
  });

it.layer(TestLayer)("syncAetherMirror", (it) => {
  describe("text files", () => {
    it.effect("mirrors modified, added, deleted and renamed files onto the task base", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        yield* mirror.write("build/cache.txt", "ignored artifact\n");
        mirror.fake.serve(
          diffOf(
            modified("app.txt", hunk(1, 1, " one", "-two", "+TWO", " three")),
            added("notes/new.md", "hello\n"),
            deleted("lib.txt", "lib\n"),
            renamed(
              "docs/guide.md",
              "docs/manual.md",
              hunk(1, 1, " # Guide", " ", "-Read me.", "+Read the manual."),
            ),
          ),
        );

        const result = yield* mirror.sync();

        expect(result).toMatchObject({ _tag: "synced", filesChanged: 4, modeOnlyPaths: [] });
        expect(yield* mirror.tree).toEqual({
          ...without(BASE_TREE, "lib.txt", "docs/guide.md"),
          "app.txt": "one\nTWO\nthree\n",
          "notes/new.md": "hello\n",
          "docs/manual.md": "# Guide\n\nRead the manual.\n",
        });
        // HEAD stays on the base; ignored files survive the reset and clean.
        expect(yield* git(mirror.worktree, ["rev-parse", "HEAD"])).toBe(mirror.baseCommit);
        expect(yield* mirror.read("build/cache.txt")).toBe("ignored artifact\n");
        if (result._tag === "synced") {
          expect(result.fingerprint.head).toBe(mirror.baseCommit);
        }
      }),
    );

    it.effect.skipIf(isWindows)("applies C-quoted and unicode paths", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        const names = ['we"ird\\name.txt', "tab\there.txt", "with space.txt", "ünïcödé/日本語.txt"];
        mirror.fake.serve(
          diffOf(
            ...names.map((name) => added(name, `${name}\n`)),
            renamed("lib.txt", 'lib "quoted".txt', hunk(1, 1, " lib", "+more")),
          ),
        );

        expect((yield* mirror.sync())._tag).toBe("synced");
        for (const name of names) expect(yield* mirror.read(name)).toBe(`${name}\n`);
        expect(yield* mirror.read('lib "quoted".txt')).toBe("lib\nmore\n");
        expect(yield* mirror.exists(path.join(mirror.worktree, "lib.txt"))).toBe(false);

        // Turn 2 drops one quoted file and edits the unicode one.
        mirror.fake.serve(
          diffOf(
            added('we"ird\\name.txt', 'we"ird\\name.txt\n'),
            added("ünïcödé/日本語.txt", "こんにちは\n"),
          ),
        );
        expect((yield* mirror.sync())._tag).toBe("synced");
        expect(yield* mirror.tree).toEqual({
          ...BASE_TREE,
          'we"ird\\name.txt': 'we"ird\\name.txt\n',
          "ünïcödé/日本語.txt": "こんにちは\n",
        });
      }),
    );

    it.effect("reports a mode-only change without applying it", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.fake.serve(diffOf(modeOnly("tools/run.sh"), added("notes.md", "hi\n")));

        expect(yield* mirror.sync()).toMatchObject({
          _tag: "synced",
          filesChanged: 1,
          modeOnlyPaths: ["tools/run.sh"],
        });
        expect(yield* mirror.tree).toEqual({ ...BASE_TREE, "notes.md": "hi\n" });
        // The tree it left verifies clean on the next sync.
        expect((yield* mirror.sync())._tag).toBe("unchanged");
      }),
    );
  });

  describe("binary files", () => {
    it.effect("writes added, modified and renamed binaries from the files channel", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        const newLogo = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x00, 0xff]);
        const fresh = Buffer.from([0x00, 0x01, 0x02, 0x03, 0xff, 0xfe]);
        const icon = Buffer.from([0xca, 0xfe]);
        mirror.fake.serve(
          diffOf(
            binary("modified", "assets/logo.bin", "assets/logo.bin"),
            binary("added", "/dev/null", "assets/new/fresh.bin"),
            binary("deleted", "assets/old.bin", "/dev/null"),
            binary("renamed", "assets/icon.bin", "assets/icons/icon.bin"),
            added("notes.md", "next to binaries\n"),
          ),
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
          // A modified binary is rewritten in place and keeps its mode.
          expect((yield* mirror.stat("assets/logo.bin")).mode & 0o111).not.toBe(0);
        }

        // Turn 2 no longer touches the binaries: they are back to the base.
        mirror.fake.serve(diffOf(added("notes.md", "text only\n")));
        expect((yield* mirror.sync())._tag).toBe("synced");
        expect(yield* mirror.tree).toEqual({ ...BASE_TREE, "notes.md": "text only\n" });
      }),
    );

    it.effect("removes every old binary path before writing a new one", () =>
      Effect.gen(function* () {
        // `assets/logo.bin` is both a deletion and a rename's target.
        const mirror = yield* makeMirror();
        mirror.fake.serve(
          diffOf(
            binary("renamed", "assets/icon.bin", "assets/logo.bin"),
            binary("deleted", "assets/logo.bin", "/dev/null"),
          ),
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
    it.effect("fails loudly on a too-large file and changes nothing", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.fake.serve(
          diffOf(added("notes.md", "hi\n"), tooLarge("modified", "data/big.json", 1_048_576)),
        );

        const error = yield* Effect.flip(mirror.sync());
        expect(error).toMatchObject({
          _tag: "MirrorTooLargeError",
          files: [{ path: "data/big.json", maxBytes: 1_048_576 }],
        });
        expect(error.message).toContain("data/big.json");
        expect(yield* mirror.tree).toEqual(BASE_TREE);

        // Deleting a too-large file needs no content, so it is mirrored.
        mirror.fake.serve(diffOf(tooLarge("deleted", "lib.txt", 1_048_576)));
        expect((yield* mirror.sync())._tag).toBe("synced");
        expect(yield* mirror.tree).toEqual(without(BASE_TREE, "lib.txt"));
      }),
    );

    it.effect("refuses diff paths that leave the worktree, before touching anything", () =>
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
        ];

        for (const escape of escapes) {
          mirror.fake.serve(diffOf(added("notes.md", "x\n"), escape), {
            [escape.newPath]: "payload",
          });
          const error = yield* Effect.flip(mirror.sync());
          expect(error._tag, escape.newPath).toBe("MirrorPathEscapeError");
          expect(yield* mirror.tree).toEqual(BASE_TREE);
        }
        expect(yield* Effect.promise(() => fs.readFile(sentinel, "utf8"))).toBe("sentinel\n");
        expect(yield* mirror.exists(path.join(mirror.root, "escape.txt"))).toBe(false);
        expect(mirror.fake.reads).toEqual([]);
      }),
    );

    it.effect.skipIf(isWindows)("refuses a binary path through a symlinked directory", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        const outside = path.join(mirror.root, "outside-dir");
        yield* Effect.promise(async () => {
          await fs.mkdir(outside);
          // Under the ignored `build/`, so it survives the reset and clean.
          await fs.mkdir(path.join(mirror.worktree, "build"));
          await fs.symlink(outside, path.join(mirror.worktree, "build", "escape-link"));
        });
        mirror.fake.serve(diffOf(binary("added", "/dev/null", "build/escape-link/payload.bin")), {
          "build/escape-link/payload.bin": "payload",
        });

        const error = yield* Effect.flip(mirror.sync());
        expect(error).toMatchObject({
          _tag: "MirrorPathEscapeError",
          path: "build/escape-link/payload.bin",
        });
        expect(error.message).toContain("symlink");
        expect(yield* Effect.promise(() => fs.readdir(outside))).toEqual([]);

        // The refusal came after the reset; the next sync is not blocked by it.
        mirror.fake.serve(diffOf(added("notes.md", "after\n")));
        expect((yield* mirror.sync())._tag).toBe("synced");
      }),
    );

    it.effect.skipIf(isWindows)("replaces a symlink leaf instead of writing through it", () =>
      Effect.gen(function* () {
        // The base tracks a link out of the worktree; the VM replaced it with a regular file.
        const mirror = yield* makeMirror({
          symlinks: { "assets/link.bin": "../../outside-sentinel.txt" },
        });
        const sentinel = path.join(mirror.root, "outside-sentinel.txt");
        yield* Effect.promise(() => fs.writeFile(sentinel, "sentinel\n"));
        mirror.fake.serve(diffOf(binary("modified", "assets/link.bin", "assets/link.bin")), {
          "assets/link.bin": LOGO,
        });

        expect((yield* mirror.sync())._tag).toBe("synced");
        expect((yield* mirror.tree)["assets/link.bin"]).toBe(hex(LOGO));
        expect(yield* Effect.promise(() => fs.readFile(sentinel, "utf8"))).toBe("sentinel\n");
      }),
    );

    it.effect("fails with the transport error and changes nothing when the socket fails", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.fake.failDiff("The workspace socket closed (code 1006).");
        expect(yield* Effect.flip(mirror.sync())).toMatchObject({
          _tag: "MirrorTransportError",
          request: "git.diff",
        });

        mirror.fake.serve(
          diffOf(added("notes.md", "x\n"), binary("added", "/dev/null", "assets/new.bin")),
          { "assets/new.bin": LOGO },
        );
        mirror.fake.failRead("No answer within 30000ms.");
        expect(yield* Effect.flip(mirror.sync())).toMatchObject({
          _tag: "MirrorTransportError",
          request: "files.read",
          path: "assets/new.bin",
        });
        expect(yield* mirror.tree).toEqual(BASE_TREE);
      }),
    );
  });

  describe("turns", () => {
    it.effect("applies each turn's larger cumulative diff onto the base", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.fake.serve(
          diffOf(
            modified("app.txt", hunk(1, 1, " one", "-two", "+TWO", " three")),
            added("notes.md", "hello\n"),
          ),
        );
        expect((yield* mirror.sync())._tag).toBe("synced");

        // Turn 2 re-touches both files: without the reset, neither would apply again.
        mirror.fake.serve(
          diffOf(
            modified("app.txt", hunk(1, 1, " one", "-two", "-three", "+TWO", "+THREE", "+four")),
            added("notes.md", "hello\nworld\n"),
            added("fresh.txt", "new in turn two\n"),
            deleted("lib.txt", "lib\n"),
          ),
        );
        expect(yield* mirror.sync()).toMatchObject({ _tag: "synced", filesChanged: 4 });
        expect(yield* mirror.tree).toEqual({
          ...without(BASE_TREE, "lib.txt"),
          "app.txt": "one\nTWO\nTHREE\nfour\n",
          "notes.md": "hello\nworld\n",
          "fresh.txt": "new in turn two\n",
        });

        // Turn 3: the agent reverted its notes and restored lib.txt; they leave the diff.
        const turn3 = diffOf(modified("app.txt", hunk(1, 1, " one", "-two", "+TWO", " three")));
        mirror.fake.serve(turn3);
        expect((yield* mirror.sync())._tag).toBe("synced");
        expect(yield* mirror.tree).toEqual({ ...BASE_TREE, "app.txt": "one\nTWO\nthree\n" });

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
        expect(yield* mirror.sync()).toMatchObject({
          _tag: "unchanged",
          fingerprint: { head: mirror.baseCommit },
        });
      }),
    );

    it.effect("keeps the last mirror when a diff does not apply to the base", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.fake.serve(diffOf(added("notes.md", "turn one\n")));
        expect((yield* mirror.sync())._tag).toBe("synced");

        mirror.fake.serve(diffOf(modified("app.txt", hunk(1, 1, " not", "-what", "+is there"))));
        expect(yield* Effect.flip(mirror.sync())).toMatchObject({
          _tag: "MirrorApplyFailedError",
          step: "check",
        });
        expect(yield* mirror.tree).toEqual({ ...BASE_TREE, "notes.md": "turn one\n" });

        mirror.fake.serve(diffOf(added("notes.md", "turn three\n")));
        expect((yield* mirror.sync())._tag).toBe("synced");
      }),
    );

    it.effect("records a partly synced worktree so the next sync does not see local edits", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.fake.serve(diffOf(added("notes.md", "turn one\n")));
        expect((yield* mirror.sync())._tag).toBe("synced");

        // `app.txt` is a file, so the binary write fails after the reset and patch.
        mirror.fake.serve(
          diffOf(added("more.md", "partial\n"), binary("added", "/dev/null", "app.txt/x.bin")),
          { "app.txt/x.bin": LOGO },
        );
        expect(yield* Effect.flip(mirror.sync())).toMatchObject({
          _tag: "MirrorApplyFailedError",
          step: "write",
          path: "app.txt/x.bin",
        });

        mirror.fake.serve(diffOf(added("notes.md", "turn three\n")));
        expect((yield* mirror.sync())._tag).toBe("synced");
        expect(yield* mirror.tree).toEqual({ ...BASE_TREE, "notes.md": "turn three\n" });
      }),
    );
  });

  describe("local edits", () => {
    it.effect("refuses to overwrite local edits, naming them, until the base is restored", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.fake.serve(
          diffOf(modified("app.txt", hunk(1, 1, " one", "-two", "+TWO", " three"))),
        );
        expect((yield* mirror.sync())._tag).toBe("synced");

        yield* mirror.write("app.txt", "my local edit\n");
        yield* mirror.write("scratch.txt", "mine\n");
        mirror.fake.serve(diffOf(added("notes.md", "turn two\n")));
        const error = yield* Effect.flip(mirror.sync());
        expect(error).toMatchObject({
          _tag: "MirrorLocalEditsError",
          worktreePath: mirror.worktree,
          paths: ["app.txt", "scratch.txt"],
          baseCommit: mirror.baseCommit,
        });
        expect(error).not.toHaveProperty("headMoved");
        expect(error.message).toContain("app.txt, scratch.txt");
        expect(yield* mirror.read("app.txt")).toBe("my local edit\n");
        expect(yield* mirror.read("scratch.txt")).toBe("mine\n");

        // The user moves their edits away and restores the base: the mirror resumes.
        yield* git(mirror.worktree, ["reset", "--hard", mirror.baseCommit]);
        yield* git(mirror.worktree, ["clean", "-fd"]);
        expect((yield* mirror.sync())._tag).toBe("synced");
        expect(yield* mirror.tree).toEqual({ ...BASE_TREE, "notes.md": "turn two\n" });
      }),
    );

    it.effect("refuses a local commit even when the content matches", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.fake.serve(diffOf(added("notes.md", "hello\n")));
        expect((yield* mirror.sync())._tag).toBe("synced");

        yield* git(mirror.worktree, ["add", "-A"]);
        yield* git(mirror.worktree, ["commit", "-m", "local commit"]);
        const localHead = yield* git(mirror.worktree, ["rev-parse", "HEAD"]);

        expect(yield* Effect.flip(mirror.sync())).toMatchObject({
          _tag: "MirrorLocalEditsError",
          paths: [],
          headMoved: { expected: mirror.baseCommit, actual: localHead },
        });
        expect(yield* git(mirror.worktree, ["rev-parse", "HEAD"])).toBe(localHead);
      }),
    );

    it.effect("refuses edits made before the first sync", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        yield* mirror.write("lib.txt", "edited before any turn\n");
        mirror.fake.serve(diffOf(added("notes.md", "hello\n")));

        expect(yield* Effect.flip(mirror.sync())).toMatchObject({
          _tag: "MirrorLocalEditsError",
          paths: ["lib.txt"],
        });
        expect(yield* mirror.read("lib.txt")).toBe("edited before any turn\n");
      }),
    );

    it.effect("trusts its own record across calls, but never another task's", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.fake.serve(diffOf(added("notes.md", "hello\n")));
        expect((yield* mirror.sync(TASK))._tag).toBe("synced");

        // Another task finds changes it did not make.
        expect(yield* Effect.flip(mirror.sync(OTHER_TASK))).toMatchObject({
          _tag: "MirrorLocalEditsError",
          paths: ["notes.md"],
        });
        // A fresh call for the same task (as after a restart) finds its record.
        expect((yield* mirror.sync(TASK))._tag).toBe("unchanged");
      }),
    );
  });

  describe("ownership", () => {
    it.effect(
      "holds the repository lock as the owner: a non-owner mutation waits, then is refused",
      () =>
        Effect.gen(function* () {
          const mirror = yield* makeMirror();
          const gitCore = yield* GitCore;
          const applying = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          // Pause the sync inside its mutation, just before `git apply`.
          const paused: GitCoreShape = {
            ...gitCore,
            execute: (input) =>
              input.operation === "AetherMirrorSync.apply"
                ? Deferred.succeed(applying, undefined).pipe(
                    Effect.andThen(Deferred.await(release)),
                    Effect.andThen(gitCore.execute(input)),
                  )
                : gitCore.execute(input),
          };
          mirror.fake.serve(
            diffOf(modified("app.txt", hunk(1, 1, " one", "-two", "+TWO", " three"))),
          );
          const syncing = yield* mirror
            .sync()
            .pipe(Effect.provideService(GitCore, paused), Effect.forkChild);
          yield* Deferred.await(applying);

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

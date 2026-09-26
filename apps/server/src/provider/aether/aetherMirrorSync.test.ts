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
import { GitCommandError } from "../../git/Errors.ts";
import { GitCoreLive } from "../../git/Layers/GitCore.ts";
import { GitCore, type GitCoreShape, type WorktreeOwner } from "../../git/Services/GitCore.ts";
import { AetherTaskId } from "./aetherApiSchemas.ts";
import {
  commandLineBatches,
  fetchAetherBranch,
  syncAetherBranch,
  syncAetherMirror,
  type AetherPushedBranch,
} from "./aetherMirrorSync.ts";
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
const HEAD_BRANCH = "aether/widgets-6a3e1f0c2b4d";
/** A worktree branch as Synara names a new thread's. */
const TEMPORARY_BRANCH = "synara/0a1b2c3d";

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
 * GitCore whose `git <args>` fails, as if Synara stopped right there, when `stopAt` matches: the
 * steps before it happened, the ones after it never do.
 */
const stopAt = (matches: (args: ReadonlyArray<string>) => boolean) =>
  Effect.gen(function* () {
    const gitCore = yield* GitCore;
    const stopping: GitCoreShape = {
      ...gitCore,
      execute: (input) =>
        matches(input.args)
          ? Effect.fail(
              new GitCommandError({
                operation: input.operation,
                command: `git ${input.args.join(" ")}`,
                cwd: input.cwd,
                detail: "Synara stopped here.",
              }),
            )
          : gitCore.execute(input),
    };
    return stopping;
  });

/**
 * A repository on `main` with a linked worktree on its own branch, claimed by the Aether
 * thread for the test's scope, and a fake workspace to serve diffs from.
 */
const makeMirror = (
  options: {
    readonly symlinks?: Readonly<Record<string, string>>;
    /** The worktree's branch; Synara names a new thread's worktree branch `synara/<8 hex>`. */
    readonly branch?: string;
  } = {},
) =>
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
    // Origin is a bare repository the VM pushes to.
    const origin = path.join(root, "origin.git");
    yield* git(root, ["init", "--bare", "--initial-branch=main", origin]);
    yield* git(repo, ["remote", "add", "origin", origin]);
    yield* git(repo, ["push", "-q", "origin", "main"]);
    const worktree = path.join(root, "owned");
    yield* git(repo, ["worktree", "add", "-b", options.branch ?? "feature/owned", worktree]);
    const baseCommit = yield* git(worktree, ["rev-parse", "HEAD"]);
    const claim = yield* gitCore.claimWorktree(worktree, aetherOwner);
    const fake = makeFakeWorkspace();

    const inWorktree = (file: string) => path.join(worktree, file);
    /** The VM's clone of origin, on the task's branch from the base. */
    const vmClone = path.join(root, "vm");
    let vmReady = false;
    const vm = (branch: string) =>
      Effect.gen(function* () {
        if (!vmReady) {
          yield* git(root, ["clone", "-q", origin, vmClone]);
          yield* git(vmClone, ["config", "user.email", "agent@test.com"]);
          yield* git(vmClone, ["config", "user.name", "Agent"]);
          yield* git(vmClone, ["config", "core.autocrlf", "false"]);
          yield* git(vmClone, ["checkout", "-q", "-b", branch, baseCommit]);
          vmReady = true;
        }
        return {
          /** Commits `files` on the task's branch and pushes it; returns the commit. */
          push: (files: Readonly<Record<string, string>>, message: string) =>
            Effect.gen(function* () {
              yield* Effect.promise(async () => {
                for (const [file, content] of Object.entries(files)) {
                  await fs.mkdir(path.dirname(path.join(vmClone, file)), { recursive: true });
                  await fs.writeFile(path.join(vmClone, file), content);
                }
              });
              yield* git(vmClone, ["add", "-A"]);
              yield* git(vmClone, ["commit", "-q", "-m", message]);
              yield* git(vmClone, ["push", "-q", "--force", "origin", branch]);
              return yield* git(vmClone, ["rev-parse", "HEAD"]);
            }),
          /** Drops the branch's last commit locally; the next `push` force-pushes the rewrite. */
          dropLastCommit: git(vmClone, ["reset", "-q", "--hard", "HEAD~1"]),
        };
      });

    return {
      root,
      repo,
      origin,
      worktree,
      baseCommit,
      fake,
      vm,
      /** Reads origin's tip of the branch, as the adapter does before a mirror. */
      fetchBranch: (branch: string = HEAD_BRANCH) =>
        fetchAetherBranch({ claim, taskId: TASK, branch }),
      /** Moves the branch to a tip `fetchBranch` read. */
      syncBranchTo: (pushed: AetherPushedBranch) =>
        syncAetherBranch({ claim, taskId: TASK, baseCommit, pushed }),
      /** Reads origin's tip, then moves the branch to it. */
      syncBranch: (branch: string = HEAD_BRANCH) =>
        fetchAetherBranch({ claim, taskId: TASK, branch }).pipe(
          Effect.flatMap((pushed) => syncAetherBranch({ claim, taskId: TASK, baseCommit, pushed })),
        ),
      /** The branch read's or sync's `AetherMirrorError`; anything else fails the test. */
      branchFailure: (branch: string = HEAD_BRANCH) =>
        fetchAetherBranch({ claim, taskId: TASK, branch }).pipe(
          Effect.flatMap((pushed) => syncAetherBranch({ claim, taskId: TASK, baseCommit, pushed })),
          Effect.flip,
          Effect.flatMap((error) =>
            error._tag === "AetherMirrorError" ? Effect.succeed(error) : Effect.die(error),
          ),
        ),
      head: git(worktree, ["rev-parse", "HEAD"]),
      branchName: git(worktree, ["symbolic-ref", "--short", "HEAD"]),
      /** Origin deletes the branch, as GitHub does when its pull request merges. */
      deleteOnOrigin: (branch: string = HEAD_BRANCH) =>
        git(root, ["--git-dir", origin, "branch", "-D", branch]),
      /** The index is HEAD's tree: nothing staged. */
      indexIsHead: Effect.gen(function* () {
        return (
          (yield* git(worktree, ["write-tree"])) ===
          (yield* git(worktree, ["rev-parse", "HEAD^{tree}"]))
        );
      }),
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
      /** `git status --porcelain` lines, sorted: what the thread's diff panel shows. */
      status: Effect.gen(function* () {
        const result = yield* gitCore.execute({
          operation: "aetherMirrorSync.test",
          cwd: worktree,
          args: ["status", "--porcelain=v1", "--untracked-files=all"],
        });
        return result.stdout
          .split("\n")
          .filter((line) => line.length > 0)
          .toSorted();
      }),
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
        // The index keeps HEAD's tree, so the changes are unstaged (new files untracked); HEAD
        // stays on the base; ignored files are untouched.
        expect(yield* git(mirror.worktree, ["write-tree"])).toBe(
          yield* git(mirror.worktree, ["rev-parse", "HEAD^{tree}"]),
        );
        expect(yield* mirror.status).toEqual([
          " D docs/guide.md",
          " D lib.txt",
          " M app.txt",
          "?? docs/manual.md",
          "?? notes/new.md",
        ]);
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

    it.effect("keeps mirroring over a local commit of exactly the mirrored files", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.serve([added("notes.md", "hello\n")]);
        expect((yield* mirror.sync())._tag).toBe("synced");
        yield* git(mirror.worktree, ["add", "-A"]);
        yield* git(mirror.worktree, ["commit", "-m", "local commit"]);
        const committed = yield* git(mirror.worktree, ["rev-parse", "HEAD"]);

        // The commit holds the files; nothing local would be lost.
        mirror.serve([added("notes.md", "hello\n"), added("more.md", "turn two\n")]);
        expect((yield* mirror.sync())._tag).toBe("synced");
        expect(yield* git(mirror.worktree, ["rev-parse", "HEAD"])).toBe(committed);
        expect(yield* mirror.status).toEqual(["?? more.md"]);
      }),
    );

    it.effect("keeps mirroring over a local commit of other changes, which keeps them", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.serve([added("notes.md", "hello\n")]);
        expect((yield* mirror.sync())._tag).toBe("synced");
        yield* mirror.write("app.txt", "committed locally\n");
        yield* git(mirror.worktree, ["add", "-A"]);
        yield* git(mirror.worktree, ["commit", "-m", "local commit"]);
        const committed = yield* git(mirror.worktree, ["rev-parse", "HEAD"]);

        // A clean checkout of HEAD loses nothing: the edit is in the commit, which stays.
        expect((yield* mirror.sync())._tag).toBe("synced");
        expect(yield* git(mirror.worktree, ["rev-parse", "HEAD"])).toBe(committed);
        expect(yield* git(mirror.worktree, ["show", "HEAD:app.txt"])).toBe("committed locally");
        expect(yield* mirror.read("app.txt")).toBe(BASE["app.txt"]);
        expect(yield* mirror.status).toEqual([" M app.txt"]);
      }),
    );

    it.effect("refuses a local edit on top of a local commit", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.serve([added("notes.md", "hello\n")]);
        expect((yield* mirror.sync())._tag).toBe("synced");
        yield* git(mirror.worktree, ["add", "-A"]);
        yield* git(mirror.worktree, ["commit", "-m", "local commit"]);
        yield* mirror.write("app.txt", "edited after the commit\n");

        expect(yield* mirror.failure()).toMatchObject({
          reason: "local-edits",
          paths: ["app.txt"],
        });
        expect(yield* mirror.read("app.txt")).toBe("edited after the commit\n");
      }),
    );

    it.effect("refuses staged changes even when the working tree holds the mirror", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.serve([added("notes.md", "hello\n")]);
        expect((yield* mirror.sync())._tag).toBe("synced");
        yield* git(mirror.worktree, ["add", "notes.md"]);

        expect(yield* mirror.failure()).toMatchObject({
          reason: "local-edits",
          paths: ["notes.md"],
        });
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

    it.effect("fails loudly on a record with a key it does not know, or a bad origin tip", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror();
        mirror.serve([added("notes.md", "hello\n")]);
        const synced = yield* mirror.sync();
        const recordFile = yield* mirror.recordFile;
        for (const record of [
          { taskId: TASK, target: synced.tree, originTip: "abc" },
          { taskId: TASK, target: synced.tree, origin: { tip: "abc" } },
        ]) {
          yield* Effect.promise(() => fs.writeFile(recordFile, JSON.stringify(record)));
          expect(yield* mirror.failure()).toMatchObject({ reason: "record" });
        }
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

  describe("branch", () => {
    it.effect(
      "puts the worktree on the task's branch, tracking origin, before anything is pushed",
      () =>
        Effect.gen(function* () {
          const mirror = yield* makeMirror({ branch: TEMPORARY_BRANCH });

          expect(yield* mirror.syncBranch()).toEqual({ _tag: "current", head: mirror.baseCommit });
          expect(yield* mirror.branchName).toBe(HEAD_BRANCH);
          expect(yield* mirror.head).toBe(mirror.baseCommit);
          expect(yield* git(mirror.worktree, ["config", `branch.${HEAD_BRANCH}.merge`])).toBe(
            `refs/heads/${HEAD_BRANCH}`,
          );
          expect(yield* git(mirror.worktree, ["config", `branch.${HEAD_BRANCH}.remote`])).toBe(
            "origin",
          );
          // The temporary name is gone.
          expect(yield* git(mirror.repo, ["branch", "--list", TEMPORARY_BRANCH])).toBe("");
          // Again: nothing to do.
          expect(yield* mirror.syncBranch()).toEqual({ _tag: "current", head: mirror.baseCommit });
        }),
    );

    it.effect("fast-forwards to what the VM pushed, keeping the working tree as mirrored", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror({ branch: TEMPORARY_BRANCH });
        const turn = [
          modified("app.txt", hunk(1, 1, " one", "-two", "+TWO", " three")),
          added("notes.md", "not committed yet\n"),
        ];
        mirror.serve(turn);
        const mirrored = yield* mirror.sync();
        const vm = yield* (yield* mirror.vm(HEAD_BRANCH)).push(
          { "app.txt": "one\nTWO\nthree\n" },
          "Capitalize two",
        );

        expect(yield* mirror.syncBranch()).toEqual({
          _tag: "fast-forwarded",
          from: mirror.baseCommit,
          to: vm,
        });
        expect(yield* mirror.head).toBe(vm);
        expect(yield* git(mirror.worktree, ["rev-parse", "--abbrev-ref", "@{upstream}"])).toBe(
          `origin/${HEAD_BRANCH}`,
        );
        // Committed work is a commit; only what the VM has not committed is a change.
        expect(yield* mirror.indexIsHead).toBe(true);
        expect(yield* mirror.status).toEqual(["?? notes.md"]);
        // The working tree did not move, and the mirror carries on from it.
        mirror.serve([...turn, added("more.md", "turn two\n")]);
        expect(yield* mirror.sync()).toMatchObject({ _tag: "synced" });
        expect(mirrored._tag).toBe("synced");
        expect(yield* mirror.status).toEqual(["?? more.md", "?? notes.md"]);
      }),
    );

    it.effect("never moves a branch with local commits origin lacks, and keeps mirroring", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror({ branch: TEMPORARY_BRANCH });
        mirror.serve([added("notes.md", "hello\n")]);
        yield* mirror.sync();
        const vm = yield* mirror.vm(HEAD_BRANCH);
        yield* vm.push({ "notes.md": "hello\n" }, "Add notes");
        expect((yield* mirror.syncBranch())._tag).toBe("fast-forwarded");
        yield* mirror.write("local.md", "mine\n");
        yield* git(mirror.worktree, ["add", "-A"]);
        yield* git(mirror.worktree, ["commit", "-q", "-m", "local commit"]);
        const local = yield* mirror.head;

        expect(yield* mirror.syncBranch()).toMatchObject({ _tag: "held", reason: "ahead" });
        expect(yield* mirror.head).toBe(local);

        // Both sides committed: still not moved.
        yield* vm.push({ "later.md": "later\n" }, "Later");
        expect(yield* mirror.syncBranch()).toMatchObject({ _tag: "held", reason: "diverged" });
        expect(yield* mirror.head).toBe(local);

        // The mirror goes on; the local commit keeps what it holds.
        mirror.serve([added("notes.md", "hello\n"), added("later.md", "later\n")]);
        expect((yield* mirror.sync())._tag).toBe("synced");
        expect(yield* mirror.head).toBe(local);
        expect(yield* git(mirror.worktree, ["show", "HEAD:local.md"])).toBe("mine");
      }),
    );

    it.effect("moves only to the tip it read, never to a push that came after", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror({ branch: TEMPORARY_BRANCH });
        const vm = yield* mirror.vm(HEAD_BRANCH);
        const read = yield* vm.push({ "a.md": "a\n" }, "A");
        const pushed = yield* mirror.fetchBranch();
        expect(pushed.tip).toBe(read);
        // The agent pushes again after the read; the mirror wrote only what the read saw.
        yield* vm.push({ "b.md": "b\n" }, "B");
        mirror.serve([added("a.md", "a\n")]);
        yield* mirror.sync();

        expect(yield* mirror.syncBranchTo(pushed)).toEqual({
          _tag: "fast-forwarded",
          from: mirror.baseCommit,
          to: read,
        });
        expect(yield* mirror.status).toEqual([]);
      }),
    );

    it.effect(
      "says why the working tree looks reverted whenever the branch has local commits",
      () =>
        Effect.gen(function* () {
          const mirror = yield* makeMirror({ branch: TEMPORARY_BRANCH });
          mirror.serve([added("notes.md", "notes\n")]);
          yield* mirror.sync();
          yield* mirror.write("app.txt", "mine\n");
          yield* git(mirror.worktree, ["add", "-A"]);
          yield* git(mirror.worktree, ["commit", "-q", "-m", "Mine"]);
          const local = yield* mirror.head;

          // Nothing pushed yet: the local commit sits on the task base.
          const unpushed = yield* mirror.syncBranch();
          expect(unpushed).toMatchObject({ _tag: "held", reason: "ahead" });
          const detail = unpushed._tag === "held" ? unpushed.detail : "";
          expect(detail).toContain(local.slice(0, 12));
          expect(detail).toContain(mirror.baseCommit.slice(0, 12));
          expect(detail).toContain("reverted");
          expect(yield* mirror.head).toBe(local);
        }),
    );

    it.effect(
      "keeps a branch origin deleted after merging it, noting it once, never as local commits",
      () =>
        Effect.gen(function* () {
          const mirror = yield* makeMirror({ branch: TEMPORARY_BRANCH });
          const vm = yield* mirror.vm(HEAD_BRANCH);
          const pushed = yield* vm.push({ "a.md": "a\n" }, "A");
          mirror.serve([added("a.md", "a\n")]);
          yield* mirror.sync();
          expect(yield* mirror.syncBranch()).toMatchObject({ _tag: "fast-forwarded", to: pushed });
          // The mirror rewrites its record on the next move: it keeps what origin had.
          mirror.serve([added("a.md", "a\n"), added("draft.md", "draft\n")]);
          expect((yield* mirror.sync())._tag).toBe("synced");

          // The pull request merges and GitHub deletes the branch.
          yield* mirror.deleteOnOrigin();
          const noted = yield* mirror.syncBranch();
          expect(noted).toMatchObject({ _tag: "origin-deleted", head: pushed });
          const detail = noted._tag === "origin-deleted" ? noted.detail : "";
          expect(detail).toContain(pushed.slice(0, 12));
          expect(detail).not.toContain("reverted");
          // Once: later syncs say nothing, and HEAD stays where it is.
          expect(yield* mirror.syncBranch()).toEqual({ _tag: "current", head: pushed });
          expect(yield* mirror.head).toBe(pushed);
          expect(yield* mirror.status).toEqual(["?? draft.md"]);

          // A commit of the user's own on top of it still warns, naming both commits.
          yield* git(mirror.worktree, ["add", "-A"]);
          yield* git(mirror.worktree, ["commit", "-q", "-m", "Mine"]);
          const local = yield* mirror.head;
          const held = yield* mirror.syncBranch();
          expect(held).toMatchObject({ _tag: "held", reason: "ahead" });
          const warning = held._tag === "held" ? held.detail : "";
          expect(warning).toContain(local.slice(0, 12));
          expect(warning).toContain(pushed.slice(0, 12));
          expect(warning).toContain("reverted");

          // The agent pushes the branch again: origin has it, and the note may come again.
          const again = yield* vm.push({ "b.md": "b\n" }, "B");
          expect(yield* mirror.syncBranch()).toMatchObject({ _tag: "held", reason: "diverged" });
          expect(again).not.toBe(local);
        }),
    );

    it.effect("never follows a force-push, and keeps mirroring", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror({ branch: TEMPORARY_BRANCH });
        const vm = yield* mirror.vm(HEAD_BRANCH);
        const first = yield* vm.push({ "a.md": "a\n" }, "A");
        mirror.serve([added("a.md", "a\n")]);
        yield* mirror.sync();
        expect((yield* mirror.syncBranch())._tag).toBe("fast-forwarded");
        yield* vm.dropLastCommit;
        const rewritten = yield* vm.push({ "b.md": "b\n" }, "B instead");

        const held = yield* mirror.syncBranch();
        expect(held).toMatchObject({ _tag: "held", reason: "diverged" });
        expect(held._tag === "held" && held.detail).toContain(rewritten.slice(0, 12));
        expect(yield* mirror.head).toBe(first);
        mirror.serve([added("b.md", "b\n")]);
        expect((yield* mirror.sync())._tag).toBe("synced");
        expect(yield* mirror.head).toBe(first);
      }),
    );

    it.effect("never moves the branch over local edits", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror({ branch: TEMPORARY_BRANCH });
        mirror.serve([added("notes.md", "hello\n")]);
        yield* mirror.sync();
        yield* (yield* mirror.vm(HEAD_BRANCH)).push({ "notes.md": "hello\n" }, "Add notes");
        yield* mirror.write("app.txt", "local edit\n");

        expect(yield* mirror.branchFailure()).toMatchObject({
          reason: "local-edits",
          paths: ["app.txt"],
        });
        expect(yield* mirror.head).toBe(mirror.baseCommit);
        expect(yield* mirror.branchName).toBe(TEMPORARY_BRANCH);
        expect(yield* mirror.read("app.txt")).toBe("local edit\n");
      }),
    );

    it.effect("leaves a worktree on a branch Synara did not name, or a branch name taken", () =>
      Effect.gen(function* () {
        const own = yield* makeMirror({ branch: "feature/mine" });
        expect(yield* own.syncBranch()).toMatchObject({ _tag: "held", reason: "other-branch" });
        expect(yield* own.branchName).toBe("feature/mine");

        const taken = yield* makeMirror({ branch: TEMPORARY_BRANCH });
        yield* git(taken.repo, ["commit", "-q", "--allow-empty", "-m", "elsewhere"]);
        yield* git(taken.repo, ["branch", HEAD_BRANCH, "HEAD"]);
        expect(yield* taken.syncBranch()).toMatchObject({ _tag: "held", reason: "branch-exists" });
        expect(yield* taken.branchName).toBe(TEMPORARY_BRANCH);
      }),
    );

    it.effect("reads origin without letting any credential helper or askpass prompt", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror({ branch: TEMPORARY_BRANCH });
        yield* (yield* mirror.vm(HEAD_BRANCH)).push({ "a.md": "a\n" }, "A");
        const gitCore = yield* GitCore;
        const remote: Array<{ readonly args: ReadonlyArray<string>; readonly env: unknown }> = [];
        const recording: GitCoreShape = {
          ...gitCore,
          execute: (input) => {
            if (input.args[0] === "ls-remote" || input.args[0] === "fetch") {
              remote.push({ args: input.args, env: input.env });
            }
            return gitCore.execute(input);
          },
        };

        yield* mirror.fetchBranch().pipe(Effect.provideService(GitCore, recording));
        expect(remote.map((call) => call.args[0])).toEqual(["ls-remote", "fetch"]);
        for (const call of remote) {
          expect(call.env).toEqual({
            GCM_INTERACTIVE: "never",
            GIT_ASKPASS: "",
            GIT_TERMINAL_PROMPT: "0",
            SSH_ASKPASS: "",
            SSH_ASKPASS_REQUIRE: "never",
          });
        }
      }),
    );

    it.effect("fails loudly on a branch name Git refuses, and when origin cannot be read", () =>
      Effect.gen(function* () {
        const mirror = yield* makeMirror({ branch: TEMPORARY_BRANCH });
        expect(yield* mirror.branchFailure("bad..name")).toMatchObject({ reason: "branch" });
        expect(yield* mirror.branchFailure("-x")).toMatchObject({ reason: "branch" });
        yield* git(mirror.repo, [
          "remote",
          "set-url",
          "origin",
          path.join(mirror.root, "gone.git"),
        ]);
        expect(yield* mirror.branchFailure()).toMatchObject({ reason: "fetch" });
        expect(yield* mirror.branchName).toBe(TEMPORARY_BRANCH);
      }),
    );

    describe("crashes", () => {
      it.effect("finishes a fast-forward that stopped after moving the branch", () =>
        Effect.gen(function* () {
          const mirror = yield* makeMirror({ branch: TEMPORARY_BRANCH });
          mirror.serve([added("notes.md", "draft\n")]);
          const mirrored = yield* mirror.sync();
          const pushed = yield* (yield* mirror.vm(HEAD_BRANCH)).push(
            { "notes.md": "draft\n" },
            "N",
          );
          const stopping = yield* stopAt((args) => args[0] === "reset");

          yield* mirror.syncBranch().pipe(Effect.provideService(GitCore, stopping), Effect.flip);
          // Stopped between the branch and the index: HEAD moved, the index did not.
          expect(yield* mirror.head).toBe(pushed);
          expect(yield* mirror.indexIsHead).toBe(false);

          // The next sync of either kind finishes it; nothing reads as a local edit.
          mirror.serve([added("notes.md", "draft\n")]);
          expect(yield* mirror.sync()).toEqual({ _tag: "unchanged", tree: mirrored.tree });
          expect(yield* mirror.indexIsHead).toBe(true);
          expect(yield* mirror.status).toEqual([]);
          expect(yield* mirror.syncBranch()).toEqual({ _tag: "current", head: pushed });
          // The finished fast-forward recorded what origin had: its deletion is no local commit.
          yield* mirror.deleteOnOrigin();
          expect(yield* mirror.syncBranch()).toMatchObject({ _tag: "origin-deleted" });
        }),
      );

      it.effect("leaves nothing half-done when a commit races the fast-forward", () =>
        Effect.gen(function* () {
          const mirror = yield* makeMirror({ branch: TEMPORARY_BRANCH });
          const pushed = yield* (yield* mirror.vm(HEAD_BRANCH)).push({ "a.md": "a\n" }, "A");
          mirror.serve([added("a.md", "a\n")]);
          yield* mirror.sync();
          // The user commits from a terminal between Synara reading HEAD and moving the branch.
          const gitCore = yield* GitCore;
          let local: string | undefined;
          const inWorktree = (args: ReadonlyArray<string>) =>
            gitCore
              .execute({ operation: "aetherMirrorSync.test", cwd: mirror.worktree, args })
              .pipe(Effect.map((result) => result.stdout.trim()));
          const racing: GitCoreShape = {
            ...gitCore,
            execute: (input) =>
              input.args[0] === "update-ref" && input.args.includes(pushed) && local === undefined
                ? inWorktree(["commit", "-q", "--allow-empty", "-m", "Mine"]).pipe(
                    Effect.andThen(inWorktree(["rev-parse", "HEAD"])),
                    Effect.tap((head) => Effect.sync(() => void (local = head))),
                    Effect.andThen(gitCore.execute(input)),
                  )
                : gitCore.execute(input),
          };

          const raced = yield* mirror.syncBranch().pipe(Effect.provideService(GitCore, racing));
          expect(raced).toMatchObject({ _tag: "held", reason: "moved" });
          expect(yield* mirror.head).toBe(local);

          // Nothing is left to finish: the next mirror and branch sync go on as for any local
          // commit, with the warning.
          mirror.serve([added("a.md", "a\n"), added("b.md", "b\n")]);
          expect((yield* mirror.sync())._tag).toBe("synced");
          const held = yield* mirror.syncBranch();
          expect(held).toMatchObject({ _tag: "held", reason: "diverged" });
          expect(held._tag === "held" && held.detail).toContain(local!.slice(0, 12));
          expect(yield* mirror.head).toBe(local);
        }),
      );

      it.effect("records what origin had only once a crashed fast-forward moved the branch", () =>
        Effect.gen(function* () {
          const mirror = yield* makeMirror({ branch: TEMPORARY_BRANCH });
          const pushed = yield* (yield* mirror.vm(HEAD_BRANCH)).push({ "a.md": "a\n" }, "A");
          mirror.serve([added("a.md", "a\n")]);
          yield* mirror.sync();
          const stopping = yield* stopAt(
            (args) => args[0] === "update-ref" && args.includes(pushed),
          );
          yield* mirror.syncBranch().pipe(Effect.provideService(GitCore, stopping), Effect.flip);
          expect(yield* mirror.head).toBe(mirror.baseCommit);

          // Origin deletes the branch before the next sync: HEAD never reached its tip, so
          // there is nothing to follow and nothing of the user's own.
          yield* mirror.deleteOnOrigin();
          expect(yield* mirror.syncBranch()).toEqual({ _tag: "current", head: mirror.baseCommit });
        }),
      );

      it.effect("redoes a fast-forward that stopped before moving the branch", () =>
        Effect.gen(function* () {
          const mirror = yield* makeMirror({ branch: TEMPORARY_BRANCH });
          const pushed = yield* (yield* mirror.vm(HEAD_BRANCH)).push({ "a.md": "a\n" }, "A");
          mirror.serve([added("a.md", "a\n")]);
          yield* mirror.sync();
          const stopping = yield* stopAt(
            (args) => args[0] === "update-ref" && args.includes(pushed),
          );

          yield* mirror.syncBranch().pipe(Effect.provideService(GitCore, stopping), Effect.flip);
          expect(yield* mirror.head).toBe(mirror.baseCommit);

          expect(yield* mirror.syncBranch()).toEqual({
            _tag: "fast-forwarded",
            from: mirror.baseCommit,
            to: pushed,
          });
          expect(yield* mirror.status).toEqual([]);
        }),
      );

      it.effect("refuses to finish a stopped fast-forward once HEAD or the index moved", () =>
        Effect.gen(function* () {
          const mirror = yield* makeMirror({ branch: TEMPORARY_BRANCH });
          mirror.serve([added("a.md", "a\n")]);
          yield* mirror.sync();
          yield* (yield* mirror.vm(HEAD_BRANCH)).push({ "a.md": "a\n" }, "A");
          const stopping = yield* stopAt((args) => args[0] === "reset");
          yield* mirror.syncBranch().pipe(Effect.provideService(GitCore, stopping), Effect.flip);
          yield* git(mirror.worktree, ["add", "-A"]);
          yield* mirror.write("staged.md", "mine\n");
          yield* git(mirror.worktree, ["add", "staged.md"]);

          expect(yield* mirror.branchFailure()).toMatchObject({
            reason: "local-edits",
            paths: ["a.md", "staged.md"],
          });
          expect(yield* mirror.read("staged.md")).toBe("mine\n");

          yield* git(mirror.worktree, ["commit", "-q", "-m", "moved HEAD"]);
          const error = yield* mirror.branchFailure();
          expect(error).toMatchObject({ reason: "local-edits" });
          expect(error.detail).toContain("HEAD moved");
        }),
      );
    });
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

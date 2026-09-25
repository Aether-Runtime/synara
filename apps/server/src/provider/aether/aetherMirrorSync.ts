// FILE: aetherMirrorSync.ts
// Purpose: Mirrors an Aether task's VM working tree into the thread's local worktree at turn end,
// and keeps the worktree's branch on the task's branch as the VM pushes it.
// Layer: Server provider boundary (Aether). The adapter runs both before `turn.completed`.
//
// The worktree's state has three parts, each with one writer:
// - the working tree holds the VM's working tree (the mirror's target), written by the mirror;
// - the branch is the task's `head_branch`, and moves only by fast-forward to what the VM pushed,
//   and only after a mirror that read the VM after origin's tip was read, so the working tree
//   holds everything the new HEAD does;
// - the index holds HEAD's tree, so what the VM has not committed shows as unstaged changes
//   (new files as untracked) against the commits it pushed.
//
// Nothing local is ever overwritten. Under the worktree's claim, both syncs first check that the
// index holds HEAD's tree (nothing staged) and that the working tree, untracked files included,
// is the tree the mirror last recorded (or HEAD's tree: a clean checkout loses nothing). The
// working tree is read into a scratch index seeded from the real one, and the mirror moves it
// with `git read-tree -m -u` on that scratch index, so Git writes every file: it refuses to
// overwrite untracked files, never writes through a symlink, and removes what the target drops.
//
// Crash safety: the record in the worktree's Git directory stores the target tree OID, never a
// fingerprint of the disk. A move records the tree it is writing (`pending`) first, so after a
// crash either tree is accepted. A fast-forward records `fastForward` before it moves the branch
// (`update-ref` with the old value, so a concurrent commit is never lost) and resets the index to
// the new HEAD; after a crash in between, the next sync finishes it when HEAD is at either end
// and the index holds either end's tree.
//
// The record also keeps origin's last tip that the local branch holds (`origin`), written with
// the fast-forward that reached it and kept by every other write. When origin deletes the branch
// (GitHub does when its pull request merges), commits up to that tip are not the user's: Synara
// says once that the branch is gone, and warns about local commits only beyond it.
//
// Known gap until the wire carries modes and blob ids: new symlinks and executables, type changes,
// and mode changes land as regular files (existing files keep their mode). Only the local view is
// affected; the pull request pushed from the VM is exact.

import { randomUUID } from "node:crypto";

import { isTemporaryWorktreeBranch } from "@synara/shared/git";
import {
  Effect,
  Exit,
  FileSystem,
  Path,
  type PlatformError,
  Result,
  Schema,
  type Scope,
} from "effect";

import type { GitCommandError, WorktreeOwnedError } from "../../git/Errors.ts";
import { GitCore, type WorktreeClaim } from "../../git/Services/GitCore.ts";
import type { AetherTaskId } from "./aetherApiSchemas.ts";
import { type AetherMirrorError, mirrorError } from "./aetherMirrorErrors.ts";
import { planAetherMirror } from "./aetherMirrorPatch.ts";
import type { AetherWorkspaceSocket } from "./aetherWorkspaceSocket.ts";

/** The part of the workspace socket the mirror reads from. */
export type AetherMirrorWorkspace = Pick<AetherWorkspaceSocket, "requestGitDiff" | "readFile">;

export type AetherMirrorSyncResult =
  | {
      readonly _tag: "synced";
      /** The tree the working tree now holds. */
      readonly tree: string;
      /** Files the worktree differs from the base by (the cumulative diff, not this turn's). */
      readonly filesChanged: number;
      /** Mode-only changes on the VM, which the wire cannot carry: not mirrored, warn about them. */
      readonly modeOnlyPaths: ReadonlyArray<string>;
    }
  /** The target is the tree the working tree already holds; nothing was written. */
  | { readonly _tag: "unchanged"; readonly tree: string };

export interface AetherMirrorSyncInput {
  readonly workspace: AetherMirrorWorkspace;
  /** The provider's claim on the thread's worktree. The mirror writes only there, as its owner. */
  readonly claim: WorktreeClaim;
  /** Keys the mirror record, so a record another task left is never trusted. */
  readonly taskId: AetherTaskId;
  /** The commit the task started from: the diff's merge-base. */
  readonly baseCommit: string;
}

export type AetherBranchSyncResult =
  /** On the task's branch, which holds everything pushed to it (or nothing is pushed yet). */
  | { readonly _tag: "current"; readonly head: string }
  /** The branch moved forward to what the VM pushed; the working tree did not change. */
  | { readonly _tag: "fast-forwarded"; readonly from: string; readonly to: string }
  /**
   * Origin deleted the branch (GitHub does when its pull request merges) and the local branch
   * holds its last tip, with no commits of its own: said once, then `current`.
   */
  | { readonly _tag: "origin-deleted"; readonly head: string; readonly detail: string }
  /** The branch was left where it is, for the reason `detail` gives; the mirror goes on. */
  | {
      readonly _tag: "held";
      readonly reason: "ahead" | "diverged" | "other-branch" | "branch-exists";
      readonly detail: string;
    };

declare const readFromOrigin: unique symbol;

/**
 * The task's branch as origin had it when `fetchAetherBranch` read it: its name, which Git
 * accepted, and its tip, fetched into the tracking ref (null before the agent pushes). Only
 * `fetchAetherBranch` makes one.
 */
export interface AetherPushedBranch {
  readonly branch: string;
  readonly tip: string | null;
  readonly [readFromOrigin]: true;
}

export interface AetherBranchSyncInput {
  readonly claim: WorktreeClaim;
  readonly taskId: AetherTaskId;
  /** The commit the task started from: what the branch holds before the agent pushes. */
  readonly baseCommit: string;
  /**
   * Origin's tip, read before the mirror that just wrote the working tree read the VM, so the
   * working tree holds everything that tip does: the branch moves to it without the agent's
   * committed changes reading as local reversions. Never one read without a mirror after it.
   */
  readonly pushed: AetherPushedBranch;
}

const RECORD_FILE = "synara-aether-mirror.json";
const REMOTE = "origin";
/** A fetch that takes longer than this is a failure, not a hang (the server never prompts). */
const REMOTE_TIMEOUT_MS = 60_000;
const NO_PROMPT = { GIT_TERMINAL_PROMPT: "0" };

const CommitId = Schema.String.check(Schema.isPattern(/^[0-9a-f]{40}(?:[0-9a-f]{24})?$/u));

/**
 * The tree the last sync wrote, the tree it was writing if it stopped before recording it, the
 * fast-forward it was making if it stopped before the index followed the branch, and what the
 * local branch holds from origin. Parsed strictly: an unknown key is a corrupt record.
 */
const MirrorRecord = Schema.Struct({
  taskId: Schema.String,
  target: Schema.String,
  pending: Schema.optionalKey(Schema.String),
  fastForward: Schema.optionalKey(Schema.Struct({ from: Schema.String, to: Schema.String })),
  /**
   * The last tip of the task's branch on origin that the local branch holds, and whether Synara
   * has said that origin deleted the branch since (GitHub does when its pull request merges).
   * Absent while the local branch holds nothing from origin beyond the task base.
   */
  origin: Schema.optionalKey(Schema.Struct({ tip: CommitId, deleted: Schema.Boolean })),
});
type MirrorRecord = typeof MirrorRecord.Type;
const decodeMirrorRecord = Schema.decodeUnknownExit(Schema.fromJsonString(MirrorRecord));

/** What every rewrite of the record keeps. */
const keptOrigin = (record: MirrorRecord | undefined) =>
  record?.origin === undefined ? {} : { origin: record.origin };

/** Git's checks for names Windows or macOS resolve to `.git`, whatever this platform is. */
const PROTECT_DOTGIT = ["-c", "core.protectNTFS=true", "-c", "core.protectHFS=true"];

const nulList = (output: string) => output.split("\0").filter((entry) => entry.length > 0);
const nulInput = (paths: ReadonlyArray<string>) => paths.map((path) => `${path}\0`).join("");
const subcommand = (args: ReadonlyArray<string>) =>
  args.find((arg, index) => !arg.startsWith("-") && args[index - 1] !== "-c") ?? "git";
const short = (commit: string) => commit.slice(0, 12);

/**
 * Groups argument lists into command lines of at most `budget` characters, so a sync with
 * hundreds of paths stays far below the 32,767 characters Windows allows a command line.
 */
export const commandLineBatches = (
  groups: ReadonlyArray<ReadonlyArray<string>>,
  budget = 8_000,
): Array<Array<string>> => {
  const batches: Array<Array<string>> = [];
  let batch: Array<string> = [];
  let length = 0;
  for (const group of groups) {
    const groupLength = group.reduce((sum, arg) => sum + arg.length + 1, 0);
    if (batch.length > 0 && length + groupLength > budget) {
      batches.push(batch);
      batch = [];
      length = 0;
    }
    batch.push(...group);
    length += groupLength;
  }
  if (batch.length > 0) batches.push(batch);
  return batches;
};

interface GitOptions {
  readonly env?: NodeJS.ProcessEnv;
  readonly stdin?: string;
  readonly timeoutMs?: number;
}

/** What `verify` found: the working tree's tree, and a scratch index holding it. */
interface LocalState {
  readonly tree: string;
  readonly record: MirrorRecord | undefined;
  /** `GIT_INDEX_FILE` for a scratch index that holds `tree`, stat-fresh, until the scope ends. */
  readonly scratch: NodeJS.ProcessEnv;
}

/** Git commands on the claimed worktree, and the record and checks both syncs share. */
const worktreeOps = (claim: WorktreeClaim, taskId: AetherTaskId) =>
  Effect.gen(function* () {
    const gitCore = yield* GitCore;
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const worktree = claim.worktreePath;

    const gitFailure = (error: GitCommandError) =>
      mirrorError("apply-failed", error.detail, { cause: error });
    const platformFailure = (error: PlatformError.PlatformError) =>
      mirrorError("apply-failed", error.message, { cause: error });
    const execute = (args: ReadonlyArray<string>, options: GitOptions = {}) =>
      gitCore
        .execute({
          operation: `AetherMirrorSync.${subcommand(args)}`,
          cwd: worktree,
          args,
          allowNonZeroExit: true,
          ...options,
        })
        .pipe(Effect.mapError(gitFailure));
    const git = (args: ReadonlyArray<string>, options: GitOptions = {}) =>
      execute(args, options).pipe(
        Effect.flatMap((result) =>
          result.code === 0
            ? Effect.succeed(result.stdout)
            : Effect.fail(mirrorError("apply-failed", result.stderr.trim())),
        ),
      );
    /** Exit 0 or 1 of a yes/no command; anything else is a failure. */
    const yes = (args: ReadonlyArray<string>) =>
      execute(args).pipe(
        Effect.flatMap((result) =>
          result.code <= 1
            ? Effect.succeed(result.code === 0)
            : Effect.fail(mirrorError("apply-failed", result.stderr.trim())),
        ),
      );
    /** Git's refusal of a bad path is a path escape; anything else is a failed step. */
    const refused = (stderr: string) => {
      const path = /invalid path '(.*)'|'(.*)' is beyond a symbolic link/iu.exec(stderr);
      return path === null
        ? mirrorError("apply-failed", stderr.trim())
        : mirrorError("path-escape", stderr.trim(), { paths: path.slice(1).filter(Boolean) });
    };
    const gitChecksPaths = (args: ReadonlyArray<string>, options: GitOptions) =>
      execute([...PROTECT_DOTGIT, ...args], options).pipe(
        Effect.flatMap((result) =>
          result.code === 0 ? Effect.void : Effect.fail(refused(result.stderr)),
        ),
      );

    const gitDir = git(["rev-parse", "--absolute-git-dir"]).pipe(Effect.map((dir) => dir.trim()));
    const recordFile = gitDir.pipe(Effect.map((dir) => path.join(dir, RECORD_FILE)));
    const recordFailure = (detail: string, cause?: unknown) =>
      mirrorError("record", detail, cause === undefined ? {} : { cause });
    const readRecord = Effect.gen(function* () {
      const file = yield* recordFile;
      const text = yield* fs.readFileString(file).pipe(
        Effect.map((contents): string | undefined => contents),
        Effect.catchTag("PlatformError", (error) =>
          error.reason._tag === "NotFound"
            ? Effect.succeed(undefined)
            : Effect.fail(recordFailure(`Could not read ${file}.`, error)),
        ),
      );
      if (text === undefined) return undefined;
      const record = decodeMirrorRecord(text, { onExcessProperty: "error" });
      if (Exit.isFailure(record)) return yield* recordFailure(`${file} is corrupt.`);
      // Another task's record says nothing about this one.
      return record.value.taskId === taskId ? record.value : undefined;
    });
    // Written aside and renamed into place: a torn record would fail every later sync.
    const writeRecord = (record: MirrorRecord) =>
      Effect.gen(function* () {
        const file = yield* recordFile;
        const temporary = `${file}.${randomUUID()}.tmp`;
        yield* fs.writeFileString(temporary, JSON.stringify(record)).pipe(
          Effect.andThen(fs.rename(temporary, file)),
          Effect.onError(() => fs.remove(temporary, { force: true }).pipe(Effect.ignore)),
          Effect.mapError((error) => recordFailure(`Could not write ${file}.`, error)),
        );
      });

    const revParse = (rev: string) =>
      git(["rev-parse", "--verify", rev]).pipe(Effect.map((out) => out.trim()));
    /** Paths the index stages against `tree`. */
    const stagedAgainst = (tree: string) =>
      git(["diff-index", "--cached", "--no-renames", "--name-only", "-z", tree]).pipe(
        Effect.map(nulList),
      );

    /**
     * The working tree as a tree, untracked files included and ignored ones not: read into a
     * scratch index seeded from the real one, whose stat cache spares hashing unchanged files.
     * The copy keeps the index's timestamp, so Git still re-reads racily clean entries.
     */
    const snapshot = Effect.gen(function* () {
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "synara-aether-mirror-" });
      const scratch = { GIT_INDEX_FILE: path.join(directory, "index") };
      const index = path.resolve(
        worktree,
        (yield* git(["rev-parse", "--git-path", "index"])).trim(),
      );
      const info = yield* fs.stat(index);
      yield* fs.copyFile(index, scratch.GIT_INDEX_FILE);
      if (info.mtime !== undefined) {
        yield* fs.utimes(scratch.GIT_INDEX_FILE, info.atime ?? info.mtime, info.mtime);
      }
      yield* git(["update-index", "-q", "--really-refresh"], { env: scratch });
      yield* git(["add", "-A", "--", "."], { env: scratch });
      const tree = (yield* git(["write-tree"], { env: scratch })).trim();
      return { tree, scratch };
    }).pipe(Effect.catchTag("PlatformError", (error) => Effect.fail(platformFailure(error))));

    const localEdits = (detail: string, paths: ReadonlyArray<string>) =>
      mirrorError("local-edits", detail, { paths: [...new Set(paths)].toSorted() });

    /** Finishes a fast-forward a crash cut short: the index follows HEAD. */
    const finishFastForward = (record: MirrorRecord) =>
      Effect.gen(function* () {
        const pending = record.fastForward;
        if (pending === undefined) return record;
        const head = yield* revParse("HEAD^{commit}");
        if (head !== pending.from && head !== pending.to) {
          return yield* localEdits(
            `HEAD moved to ${short(head)} while Synara was moving the branch from ${short(pending.from)} to ${short(pending.to)}.`,
            [],
          );
        }
        const staged = yield* stagedAgainst(pending.from);
        if (staged.length > 0 && (yield* stagedAgainst(pending.to)).length > 0) {
          return yield* localEdits("The index holds staged changes.", staged);
        }
        yield* git(["reset", "-q"]);
        // HEAD holds the tip it was moving to only if the branch moved.
        const finished: MirrorRecord = {
          taskId,
          target: record.target,
          ...(head === pending.to
            ? { origin: { tip: pending.to, deleted: false } }
            : keptOrigin(record)),
        };
        yield* writeRecord(finished);
        return finished;
      });

    /**
     * Checks that nothing local would be lost: nothing is staged, and the working tree is the
     * tree the mirror last recorded, the one it was writing, or a clean checkout of HEAD.
     */
    const verify: Effect.Effect<LocalState, AetherMirrorError, Scope.Scope> = Effect.gen(
      function* () {
        const read = yield* readRecord;
        const record = read === undefined ? undefined : yield* finishFastForward(read);
        const staged = yield* stagedAgainst("HEAD");
        if (staged.length > 0) {
          return yield* localEdits("The index holds staged changes.", staged);
        }
        const headTree = yield* revParse("HEAD^{tree}");
        const { tree, scratch } = yield* snapshot;
        const candidates =
          record === undefined
            ? [headTree]
            : [record.target, ...(record.pending === undefined ? [] : [record.pending]), headTree];
        if (candidates.includes(tree)) return { tree, record, scratch };
        const changed = yield* git([
          "diff-tree",
          "-r",
          "-z",
          "--name-only",
          "--no-renames",
          candidates[0]!,
          tree,
        ]);
        return yield* localEdits(
          "The working tree does not hold the tree the mirror last wrote.",
          nulList(changed),
        );
      },
    );

    /** Runs `effect` as the worktree's owner, under the repository's mutation lock. */
    const locked = <A>(effect: Effect.Effect<A, AetherMirrorError, Scope.Scope>) =>
      gitCore
        .withMutation(worktree, Effect.scoped(effect), { claim })
        .pipe(Effect.catchTag("GitCommandError", (error) => Effect.fail(gitFailure(error))));

    return {
      worktree,
      execute,
      git,
      yes,
      refused,
      gitChecksPaths,
      platformFailure,
      writeRecord,
      revParse,
      verify,
      locked,
    };
  });

/** Mirror the task's VM working tree into the claimed worktree. See the module comment. */
export const syncAetherMirror = (
  input: AetherMirrorSyncInput,
): Effect.Effect<
  AetherMirrorSyncResult,
  AetherMirrorError | WorktreeOwnedError,
  GitCore | FileSystem.FileSystem | Path.Path
> =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const ops = yield* worktreeOps(input.claim, input.taskId);
    const { git, execute, refused, gitChecksPaths } = ops;

    // 1. The target tree, built before taking the lock.
    const base = yield* ops.revParse(`${input.baseCommit}^{commit}`);
    const diff = yield* input.workspace
      .requestGitDiff("main")
      .pipe(
        Effect.mapError((error) =>
          mirrorError("transport", `git.diff: ${error.detail}`, { cause: error }),
        ),
      );
    if (diff.baseRef !== base) {
      return yield* mirrorError(
        "base-mismatch",
        `The workspace diff is from ${diff.baseRef}, not the task base ${base}.`,
      );
    }
    const planned = planAetherMirror(diff);
    if (Result.isFailure(planned)) return yield* planned.failure;
    const plan = planned.success;

    /** Stores each binary's bytes as a blob, with the mode it keeps from the base. */
    const storeBinaries = (directory: string, env: NodeJS.ProcessEnv) =>
      Effect.gen(function* () {
        const files: Array<string> = [];
        for (const [index, write] of plan.writes.entries()) {
          const read = yield* input.workspace.readFile(write.path).pipe(
            Effect.mapError((error) =>
              mirrorError("transport", `files.read: ${error.detail}`, {
                paths: [write.path],
                cause: error,
              }),
            ),
          );
          const file = path.join(directory, `blob-${index}`);
          yield* fs.writeFile(file, Buffer.from(read.content, read.encoding));
          files.push(file);
        }
        const oids = (yield* git(["hash-object", "-w", "--no-filters", "--stdin-paths"], {
          stdin: `${files.join("\n")}\n`,
        }))
          .split(/\r?\n/u)
          .filter((oid) => oid.length > 0);
        if (oids.length !== files.length) {
          return yield* mirrorError(
            "apply-failed",
            `git hash-object stored ${oids.length} of ${files.length} files.`,
          );
        }
        const keepModeOf = plan.writes.flatMap((write) => (write.modeOf ? [write.modeOf] : []));
        const baseModes = new Map<string, string>();
        for (const paths of commandLineBatches(keepModeOf.map((file) => [file]))) {
          const listed = yield* git(
            ["--literal-pathspecs", "ls-files", "--stage", "-z", "--", ...paths],
            { env },
          );
          // `<mode> <oid> <stage>\t<path>`
          for (const entry of nulList(listed)) {
            baseModes.set(entry.slice(entry.indexOf("\t") + 1), entry.slice(0, entry.indexOf(" ")));
          }
        }
        return plan.writes.map((write, index) => {
          const executable = write.modeOf && baseModes.get(write.modeOf) === "100755";
          const mode = executable ? "100755" : "100644";
          return ["--cacheinfo", `${mode},${oids[index]},${write.path}`];
        });
      });

    const target = yield* Effect.gen(function* () {
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "synara-aether-mirror-" });
      const env = { GIT_INDEX_FILE: path.join(directory, "index") };
      yield* git(["read-tree", base], { env });
      // Before the removals, which drop the rename sources whose modes binaries keep.
      const binaries = plan.writes.length > 0 ? yield* storeBinaries(directory, env) : [];
      if (plan.removals.length > 0) {
        yield* git(["update-index", "--force-remove", "-z", "--stdin"], {
          env,
          stdin: nulInput(plan.removals),
        });
      }
      if (plan.patch.length > 0) {
        yield* gitChecksPaths(["apply", "--cached", "--whitespace=nowarn", "-"], {
          env,
          stdin: plan.patch,
        });
      }
      // `--cacheinfo`, unlike `--index-info`, fails on a path Git refuses instead of skipping it.
      for (const batch of commandLineBatches(binaries)) {
        yield* gitChecksPaths(["update-index", "--add", ...batch], { env });
      }
      return (yield* git(["write-tree"], { env })).trim();
    }).pipe(
      Effect.scoped,
      Effect.catchTag("PlatformError", (error) => Effect.fail(ops.platformFailure(error))),
    );

    // 2. The working tree, as its owner.

    /**
     * What is on disk where the move would create `file`: the file itself, or a non-directory
     * where one of its directories must go. `read-tree` would replace either.
     */
    const blockingPath = (file: string) =>
      Effect.gen(function* () {
        const segments = file.split("/");
        for (let depth = 1; depth <= segments.length; depth += 1) {
          const prefix = segments.slice(0, depth).join("/");
          const info = yield* fs.stat(path.join(ops.worktree, prefix)).pipe(
            Effect.map((stat): { readonly type: string } | undefined => stat),
            Effect.catchTag("PlatformError", (error) =>
              error.reason._tag === "NotFound"
                ? Effect.succeed(undefined)
                : Effect.fail(ops.platformFailure(error)),
            ),
          );
          if (info === undefined) return undefined;
          if (depth === segments.length || info.type !== "Directory") return prefix;
        }
        return undefined;
      });

    /** Ignored local files where the move adds a file: `read-tree` would refuse or replace them. */
    const refuseIgnoredInTheWay = (from: string) =>
      Effect.gen(function* () {
        const added = nulList(
          yield* git([
            "diff-tree",
            "-r",
            "-z",
            "--name-only",
            "--no-renames",
            "--diff-filter=A",
            from,
            target,
          ]),
        );
        if (added.length === 0) return;
        // Exit 1 means none of them is ignored.
        const ignored = yield* execute(["check-ignore", "-z", "--no-index", "--stdin"], {
          stdin: nulInput(added),
        });
        if (ignored.code > 1) return yield* refused(ignored.stderr);
        const inTheWay = new Set<string>();
        for (const file of nulList(ignored.stdout)) {
          const blocking = yield* blockingPath(file);
          if (blocking !== undefined) inTheWay.add(blocking);
        }
        if (inTheWay.size > 0) {
          return yield* mirrorError(
            "local-edits",
            "Ignored local files are where the mirror adds files.",
            { paths: [...inTheWay].toSorted() },
          );
        }
      });

    return yield* ops.locked(
      Effect.gen(function* () {
        const local = yield* ops.verify;
        const kept = keptOrigin(local.record);
        if (local.tree === target) {
          if (local.record?.target !== target || local.record.pending !== undefined) {
            yield* ops.writeRecord({ taskId: input.taskId, target, ...kept });
          }
          return { _tag: "unchanged", tree: target } as const;
        }
        yield* refuseIgnoredInTheWay(local.tree);
        // Recorded first, so that after a crash the next sync accepts either tree.
        yield* ops.writeRecord({
          taskId: input.taskId,
          target: local.tree,
          pending: target,
          ...kept,
        });
        // On the scratch index: the real one keeps HEAD's tree, so the changes stay unstaged.
        yield* git(["read-tree", "-m", "-u", local.tree, target], { env: local.scratch });
        yield* ops.writeRecord({ taskId: input.taskId, target, ...kept });
        return {
          _tag: "synced",
          tree: target,
          filesChanged: plan.fileCount,
          modeOnlyPaths: plan.modeOnly,
        } as const;
      }),
    );
  });

/**
 * Reads the task's branch from origin: checks Aether's name for it, and fetches its tip into
 * the tracking ref. Read it before mirroring, then hand it to `syncAetherBranch`.
 */
export const fetchAetherBranch = (input: {
  readonly claim: WorktreeClaim;
  readonly taskId: AetherTaskId;
  /** The task's `head_branch`, as Aether names it. */
  readonly branch: string;
}): Effect.Effect<
  AetherPushedBranch,
  AetherMirrorError,
  GitCore | FileSystem.FileSystem | Path.Path
> =>
  Effect.gen(function* () {
    const ops = yield* worktreeOps(input.claim, input.taskId);
    const { execute } = ops;
    const { branch } = input;
    const ref = `refs/heads/${branch}`;
    const tracking = `refs/remotes/${REMOTE}/${branch}`;

    // The name comes from Aether: Git decides whether it is a branch name before it is used.
    const invalid = mirrorError(
      "branch",
      `Aether named the task's branch "${branch}", which is not a branch name.`,
    );
    if (branch.startsWith("-")) return yield* invalid;
    const valid = yield* execute(["check-ref-format", "--branch", branch]);
    if (valid.code !== 0 || valid.stdout.trim() !== branch) return yield* invalid;

    const remoteFailure = (detail: string) =>
      mirrorError("fetch", `Synara could not read ${REMOTE}/${branch}: ${detail}`);
    const listed = yield* execute(["ls-remote", "--exit-code", REMOTE, ref], {
      env: NO_PROMPT,
      timeoutMs: REMOTE_TIMEOUT_MS,
    });
    // Exit 2: origin has no such branch, since the agent has not pushed yet.
    if (listed.code !== 0 && listed.code !== 2) return yield* remoteFailure(listed.stderr.trim());
    const remoteTip =
      listed.code === 2
        ? null
        : (listed.stdout
            .split(/\r?\n/u)
            .map((line) => line.split("\t"))
            .find(([, name]) => name === ref)?.[0] ?? null);
    if (listed.code === 0 && remoteTip === null) {
      return yield* remoteFailure(`ls-remote listed no ${ref}.`);
    }
    if (remoteTip !== null) {
      const known = yield* execute(["rev-parse", "--verify", "-q", tracking]);
      if (known.code !== 0 || known.stdout.trim() !== remoteTip) {
        const fetched = yield* execute(
          ["fetch", "--quiet", "--no-tags", REMOTE, `+${ref}:${tracking}`],
          { env: NO_PROMPT, timeoutMs: REMOTE_TIMEOUT_MS },
        );
        if (fetched.code !== 0) return yield* remoteFailure(fetched.stderr.trim());
      }
    }
    const tip = remoteTip === null ? null : yield* ops.revParse(`${remoteTip}^{commit}`);
    return { branch, tip } as AetherPushedBranch;
  });

/**
 * Puts the claimed worktree on the task's branch and fast-forwards it to origin's tip as
 * `fetchAetherBranch` read it before the mirror, leaving the working tree as the mirror wrote
 * it. See the module comment.
 */
export const syncAetherBranch = (
  input: AetherBranchSyncInput,
): Effect.Effect<
  AetherBranchSyncResult,
  AetherMirrorError | WorktreeOwnedError,
  GitCore | FileSystem.FileSystem | Path.Path
> =>
  Effect.gen(function* () {
    const ops = yield* worktreeOps(input.claim, input.taskId);
    const { git, execute, yes } = ops;
    const { branch, tip: remote } = input.pushed;
    const ref = `refs/heads/${branch}`;
    const base = yield* ops.revParse(`${input.baseCommit}^{commit}`);

    // 2. The branch, as the worktree's owner.
    return yield* ops.locked(
      Effect.gen(function* () {
        const local = yield* ops.verify;
        const head = yield* ops.revParse("HEAD^{commit}");
        const symbolic = yield* execute(["symbolic-ref", "-q", "HEAD"]);
        const checkedOut = symbolic.code === 0 ? symbolic.stdout.trim() : null;
        if (checkedOut !== ref) {
          const temporary =
            checkedOut !== null &&
            checkedOut.startsWith("refs/heads/") &&
            isTemporaryWorktreeBranch(checkedOut.slice("refs/heads/".length));
          if (!temporary) {
            return {
              _tag: "held",
              reason: "other-branch",
              detail: `The worktree is on ${checkedOut === null ? "a detached HEAD" : checkedOut.replace(/^refs\/heads\//u, "")}, not the task's branch ${branch}: Synara leaves it there and does not follow what the agent pushes.`,
            } as const;
          }
          const existing = yield* execute(["rev-parse", "--verify", "-q", ref]);
          if (existing.code === 0 && existing.stdout.trim() !== head) {
            return {
              _tag: "held",
              reason: "branch-exists",
              detail: `A local branch ${branch} already exists at ${short(existing.stdout.trim())}: Synara leaves the worktree on ${checkedOut.slice("refs/heads/".length)}.`,
            } as const;
          }
          // Each step leaves HEAD on a branch that exists: create, switch, then drop the old name.
          if (existing.code !== 0) yield* git(["update-ref", ref, head, ""]);
          yield* git(["symbolic-ref", "HEAD", ref]);
          // Refused only when something else moved the old branch since: it is not ours to drop.
          yield* execute(["update-ref", "-d", checkedOut, head]);
        }
        if ((yield* execute(["config", "--get", `branch.${branch}.merge`])).stdout.trim() !== ref) {
          yield* git(["config", `branch.${branch}.remote`, REMOTE]);
          yield* git(["config", `branch.${branch}.merge`, ref]);
        }
        // Commits of the user's own: the mirror goes on over them, so every edit they made that
        // the agent does not have reads as reverted in the working tree. Always say why.
        const reverted =
          "The mirror goes on, so edits in those commits that the agent does not have read as reverted in the working tree.";
        const known = local.record?.origin;
        /** Records origin's tip once the local branch holds it: its deletion is then no commit of the user's. */
        const holdsOrigin = (tip: string) =>
          known?.tip === tip && !known.deleted
            ? Effect.void
            : ops.writeRecord({
                taskId: input.taskId,
                target: local.tree,
                origin: { tip, deleted: false },
              });
        if (remote === null && known !== undefined) {
          // Origin had the branch and deleted it: the local branch holds what it had, or more.
          if (
            head === known.tip ||
            (yield* yes(["merge-base", "--is-ancestor", head, known.tip]))
          ) {
            if (known.deleted) return { _tag: "current", head } as const;
            yield* ops.writeRecord({
              taskId: input.taskId,
              target: local.tree,
              origin: { tip: known.tip, deleted: true },
            });
            return {
              _tag: "origin-deleted",
              head,
              detail: `${REMOTE}/${branch} no longer exists on origin (GitHub deletes a merged pull request's branch). The local branch keeps its last tip ${short(known.tip)} and has no commits of its own.`,
            } as const;
          }
          return (yield* yes(["merge-base", "--is-ancestor", known.tip, head]))
            ? ({
                _tag: "held",
                reason: "ahead",
                detail: `The local branch ${branch} has commits of its own (local ${short(head)}) beyond ${REMOTE}/${branch}'s last tip ${short(known.tip)}, and origin no longer has the branch: Synara does not move it. ${reverted}`,
              } as const)
            : ({
                _tag: "held",
                reason: "diverged",
                detail: `The local branch ${branch} (${short(head)}) does not contain ${REMOTE}/${branch}'s last tip ${short(known.tip)}, and origin no longer has the branch: Synara does not move it. ${reverted}`,
              } as const);
        }
        if (remote === null) {
          // Never pushed: the local branch should hold the task base and nothing more.
          if (head === base) return { _tag: "current", head } as const;
          return (yield* yes(["merge-base", "--is-ancestor", base, head]))
            ? ({
                _tag: "held",
                reason: "ahead",
                detail: `The local branch ${branch} has commits of its own (${short(head)}) on the task base ${short(base)}, and the agent has pushed nothing yet: Synara does not move it. ${reverted}`,
              } as const)
            : ({
                _tag: "held",
                reason: "diverged",
                detail: `The local branch ${branch} (${short(head)}) does not contain the task base ${short(base)}: Synara does not move it. ${reverted}`,
              } as const);
        }
        if (head === remote) {
          yield* holdsOrigin(remote);
          return { _tag: "current", head } as const;
        }
        if (yield* yes(["merge-base", "--is-ancestor", head, remote])) {
          yield* ops.writeRecord({
            taskId: input.taskId,
            target: local.tree,
            fastForward: { from: head, to: remote },
            ...keptOrigin(local.record),
          });
          // With the old value: a commit made meanwhile makes this fail rather than vanish.
          yield* git(["update-ref", ref, remote, head]);
          // The index follows HEAD; the working tree stays as the mirror wrote it.
          yield* git(["reset", "-q"]);
          yield* ops.writeRecord({
            taskId: input.taskId,
            target: local.tree,
            origin: { tip: remote, deleted: false },
          });
          return { _tag: "fast-forwarded", from: head, to: remote } as const;
        }
        if (yield* yes(["merge-base", "--is-ancestor", remote, head])) {
          yield* holdsOrigin(remote);
          return {
            _tag: "held",
            reason: "ahead",
            detail: `The local branch ${branch} has commits ${REMOTE}/${branch} lacks (local ${short(head)}, pushed ${short(remote)}): Synara does not move it. ${reverted}`,
          } as const;
        }
        return {
          _tag: "held",
          reason: "diverged",
          detail: `${REMOTE}/${branch} (${short(remote)}) does not contain the local branch's tip ${short(head)}: it was force-pushed or rebased, or the local branch has commits of its own. Synara does not move the branch. ${reverted}`,
        } as const;
      }),
    );
  });

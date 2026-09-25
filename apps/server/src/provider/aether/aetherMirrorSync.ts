// FILE: aetherMirrorSync.ts
// Purpose: Mirrors an Aether task's VM working tree into the thread's local worktree at turn end.
// Layer: Server provider boundary (Aether). The adapter runs it before `turn.completed` (P4).
//
// The workspace's cumulative diff is built into a target tree in a scratch index, from the task's
// base commit. Then, as the worktree's owner, the sync checks that the worktree and index still
// hold the tree it last wrote and moves both to the target with `git read-tree -m -u`, so Git
// writes every file: it refuses to overwrite untracked files, never writes through a symlink, and
// removes what the target drops. The index holds the target; HEAD stays on the base commit.
//
// Known gap until the wire carries modes and blob ids: new symlinks and executables, type changes,
// and mode changes land as regular files (existing files keep their mode). Only the local view is
// affected; the pull request pushed from the VM is exact.

import { randomUUID } from "node:crypto";

import { decodeJsonResult } from "@synara/shared/schemaJson";
import { Effect, FileSystem, Path, type PlatformError, Result, Schema } from "effect";

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
      /** The tree the worktree and index now hold. */
      readonly tree: string;
      /** Files the worktree differs from the base by (the cumulative diff, not this turn's). */
      readonly filesChanged: number;
      /** Mode-only changes on the VM, which the wire cannot carry: not mirrored, warn about them. */
      readonly modeOnlyPaths: ReadonlyArray<string>;
    }
  /** The target is the tree the worktree already holds; nothing was written. */
  | { readonly _tag: "unchanged"; readonly tree: string };

export interface AetherMirrorSyncInput {
  readonly workspace: AetherMirrorWorkspace;
  /** The provider's claim on the thread's worktree. The mirror writes only there, as its owner. */
  readonly claim: WorktreeClaim;
  /** Keys the mirror record, so a record another task left is never trusted. */
  readonly taskId: AetherTaskId;
  /** The commit the task started from: the diff's merge-base, and the HEAD the worktree keeps. */
  readonly baseCommit: string;
}

const RECORD_FILE = "synara-aether-mirror.json";

/** The tree the last sync wrote, and the tree it was writing if it stopped before recording it. */
const MirrorRecord = Schema.Struct({
  taskId: Schema.String,
  target: Schema.String,
  pending: Schema.optionalKey(Schema.String),
});
type MirrorRecord = typeof MirrorRecord.Type;
const decodeMirrorRecord = decodeJsonResult(MirrorRecord);

/** Git's checks for names Windows or macOS resolve to `.git`, whatever this platform is. */
const PROTECT_DOTGIT = ["-c", "core.protectNTFS=true", "-c", "core.protectHFS=true"];

const nulList = (output: string) => output.split("\0").filter((entry) => entry.length > 0);
const nulInput = (paths: ReadonlyArray<string>) => paths.map((path) => `${path}\0`).join("");
const subcommand = (args: ReadonlyArray<string>) =>
  args.find((arg, index) => !arg.startsWith("-") && args[index - 1] !== "-c") ?? "git";

interface GitOptions {
  readonly env?: NodeJS.ProcessEnv;
  readonly stdin?: string;
}

/** Mirror the task's VM working tree into the claimed worktree. See the module comment. */
export const syncAetherMirror = (
  input: AetherMirrorSyncInput,
): Effect.Effect<
  AetherMirrorSyncResult,
  AetherMirrorError | WorktreeOwnedError,
  GitCore | FileSystem.FileSystem | Path.Path
> =>
  Effect.gen(function* () {
    const gitCore = yield* GitCore;
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const worktree = input.claim.worktreePath;

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

    // 1. The target tree, built before taking the lock.
    const base = (yield* git(["rev-parse", "--verify", `${input.baseCommit}^{commit}`])).trim();
    const baseTree = (yield* git(["rev-parse", "--verify", `${base}^{tree}`])).trim();
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
        const oids = nulList(
          (yield* git(["hash-object", "-w", "--no-filters", "--stdin-paths"], {
            stdin: `${files.join("\n")}\n`,
          })).replaceAll("\n", "\0"),
        );
        if (oids.length !== files.length) {
          return yield* mirrorError(
            "apply-failed",
            `git hash-object stored ${oids.length} of ${files.length} files.`,
          );
        }
        const keepModeOf = plan.writes.flatMap((write) => (write.modeOf ? [write.modeOf] : []));
        const baseModes = new Map<string, string>();
        if (keepModeOf.length > 0) {
          const listed = yield* git(
            ["--literal-pathspecs", "ls-files", "--stage", "-z", "--", ...keepModeOf],
            { env },
          );
          // `<mode> <oid> <stage>\t<path>`
          for (const entry of nulList(listed)) {
            baseModes.set(entry.slice(entry.indexOf("\t") + 1), entry.slice(0, entry.indexOf(" ")));
          }
        }
        return plan.writes.flatMap((write, index) => {
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
      if (binaries.length > 0) {
        yield* gitChecksPaths(["update-index", "--add", ...binaries], { env });
      }
      return (yield* git(["write-tree"], { env })).trim();
    }).pipe(
      Effect.scoped,
      Effect.catchTag("PlatformError", (error) => Effect.fail(platformFailure(error))),
    );

    // 2. The worktree, as its owner.
    const recordFile = git(["rev-parse", "--absolute-git-dir"]).pipe(
      Effect.map((gitDir) => path.join(gitDir.trim(), RECORD_FILE)),
    );
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
      const record = decodeMirrorRecord(text);
      if (Result.isFailure(record)) return yield* recordFailure(`${file} is corrupt.`);
      // Another task's record says nothing about this one.
      return record.success.taskId === input.taskId ? record.success : undefined;
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

    /** The first candidate tree the worktree and index both hold exactly, or the local edits. */
    const currentTree = (candidates: readonly [string, ...Array<string>]) =>
      Effect.gen(function* () {
        const head = (yield* git(["rev-parse", "--verify", "HEAD^{commit}"])).trim();
        if (head !== base) {
          return yield* mirrorError("local-edits", `HEAD moved from ${base} to ${head}.`);
        }
        yield* git(["update-index", "-q", "--refresh"]);
        const untracked = nulList(yield* git(["ls-files", "-z", "--others", "--exclude-standard"]));
        const changedFrom = (tree: string) =>
          Effect.all(
            [["--cached"], []].map((scope) =>
              git(["diff", ...scope, "--no-ext-diff", "--no-renames", "--name-only", "-z", tree]),
            ),
          ).pipe(Effect.map((outputs) => outputs.flatMap(nulList)));
        for (const tree of new Set(candidates)) {
          if (untracked.length === 0 && (yield* changedFrom(tree)).length === 0) return tree;
        }
        const changed = [...(yield* changedFrom(candidates[0])), ...untracked];
        return yield* mirrorError(
          "local-edits",
          "The worktree and index do not hold the tree the mirror last wrote.",
          { paths: [...new Set(changed)].toSorted() },
        );
      });

    /** Ignored local files where the move adds a file: `read-tree` would overwrite them. */
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
        const ignored = yield* execute(["check-ignore", "-z", "--stdin"], {
          stdin: nulInput(added),
        });
        if (ignored.code > 1) return yield* refused(ignored.stderr);
        const inTheWay: Array<string> = [];
        for (const file of nulList(ignored.stdout)) {
          if (yield* fs.exists(path.join(worktree, file)).pipe(Effect.mapError(platformFailure))) {
            inTheWay.push(file);
          }
        }
        if (inTheWay.length > 0) {
          return yield* mirrorError(
            "local-edits",
            "Ignored local files are where the mirror adds files.",
            {
              paths: inTheWay,
            },
          );
        }
      });

    const move = Effect.gen(function* () {
      const record = yield* readRecord;
      // A clean checkout of the base is always safe to move from: nothing local can be lost.
      const current = yield* currentTree(
        record === undefined
          ? [baseTree]
          : [record.target, ...(record.pending === undefined ? [] : [record.pending]), baseTree],
      );
      if (current === target) {
        if (record?.target !== target || record.pending !== undefined) {
          yield* writeRecord({ taskId: input.taskId, target });
        }
        return { _tag: "unchanged", tree: target } as const;
      }
      yield* refuseIgnoredInTheWay(current);
      // Recorded first, so that after a crash the next sync accepts either tree.
      yield* writeRecord({ taskId: input.taskId, target: current, pending: target });
      yield* git(["read-tree", "-m", "-u", current, target]);
      yield* writeRecord({ taskId: input.taskId, target });
      return {
        _tag: "synced",
        tree: target,
        filesChanged: plan.fileCount,
        modeOnlyPaths: plan.modeOnly,
      } as const;
    });

    return yield* gitCore
      .withMutation(worktree, move, { claim: input.claim })
      .pipe(Effect.catchTag("GitCommandError", (error) => Effect.fail(gitFailure(error))));
  });

// FILE: aetherMirrorSync.ts
// Purpose: Mirrors an Aether task's VM working tree into the thread's local worktree at turn end,
// so Synara's diffs, checkpoints and file viewer read the agent's work from local disk.
// Layer: Server provider boundary (Aether). The adapter runs it before `turn.completed` (P4).
//
// One sync, ordered so that the common failures change nothing:
//   1. Outside the repository lock: read the cumulative diff (merge-base to VM working tree),
//      plan the patch, have Git check every path and `git apply --check` the patch against the
//      task base in scratch indexes, then read every binary file over the workspace socket.
//   2. Inside `GitCore.withMutation`, as the worktree's owner, so no local Git mutation can
//      interleave: verify the worktree is still the tree the last sync recorded; remove the files
//      that sync created; `git reset --hard <base>`; `git apply` the patch from stdin; remove,
//      then write, the binary files; fingerprint the result and record it.
// Reset-then-apply rebuilds the whole state from the base every time, so turn N+1 never needs
// an interdiff and a failed sync heals on the next one.
//
// There is no `git clean`: after the reset it would judge untracked files by the base's ignore
// rules, not the ones the worktree was using, and delete files only the mirrored `.gitignore`
// protects. The record lists the files each sync created instead, and only those are removed.
// Ignored files, and files a mirrored ignore rule stopped ignoring, are never deleted.
//
// The reset target is the commit the caller created the task from. The diff's own `baseRef` names
// a ref on the VM (such as `origin/main`) and is not used. A diff taken against a merge-base that
// moved on the VM usually fails the check; when its hunks happen to apply, the mirror shows the
// agent's changes on the original base.
//
// Local edits: the record lives in the worktree's own Git directory, keyed by task, so a restart
// between a sync and the adapter's own persistence cannot make the mirror's changes look like
// local edits. A worktree that matches neither the record nor a clean checkout of the base fails
// the sync with `MirrorLocalEditsError` and is left untouched. A clean checkout of the base is
// always accepted: nothing can be lost there, and it is how a user resumes after moving local
// edits away.

import { createHash, randomUUID } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import * as nodeFs from "node:fs/promises";
import * as nodePath from "node:path";

import { supportsPosixPermissions } from "@synara/shared/filesystemPlatform";
import { decodeJsonResult } from "@synara/shared/schemaJson";
import { Effect, Exit, FileSystem, Path, Result, Schema } from "effect";

import type { WorktreeOwnedError } from "../../git/Errors.ts";
import { GitCore, type GitCoreShape, type WorktreeClaim } from "../../git/Services/GitCore.ts";
import { writeWorkingTreeTree } from "../../git/workingTreeTree.ts";
import type { AetherTaskId } from "./aetherApiSchemas.ts";
import {
  type AetherMirrorSyncError,
  MirrorApplyFailedError,
  MirrorLocalEditsError,
  MirrorPathEscapeError,
  MirrorTransportError,
} from "./aetherMirrorErrors.ts";
import { type AetherMirrorPlan, mirrorPathEscape, planAetherMirror } from "./aetherMirrorPatch.ts";
import type { AetherGitDiffResult } from "./aetherWorkspaceProtocol.ts";
import type { AetherWorkspaceSocket } from "./aetherWorkspaceSocket.ts";

/** The part of the workspace socket the mirror reads from. */
export type AetherMirrorWorkspace = Pick<AetherWorkspaceSocket, "requestGitDiff" | "readFile">;

/** What the worktree held when a sync finished. */
export interface AetherMirrorFingerprint {
  /** HEAD's commit. */
  readonly head: string;
  /** The worktree's content as a tree: tracked and untracked files, ignored files excluded. */
  readonly tree: string;
}

export type AetherMirrorSyncResult =
  | {
      readonly _tag: "synced";
      readonly fingerprint: AetherMirrorFingerprint;
      /** Files the worktree now differs from the base by (the cumulative diff, not this turn's). */
      readonly filesChanged: number;
      /** Mode-only changes on the VM, which the wire cannot carry: not mirrored, warn about them. */
      readonly modeOnlyPaths: ReadonlyArray<string>;
    }
  /**
   * The worktree holds the same tree as before the sync. Usually nothing was written: the record
   * showed this exact diff already applied. After a failed sync, or when a different diff builds
   * the same tree, the files were rewritten to the same content.
   */
  | { readonly _tag: "unchanged"; readonly fingerprint: AetherMirrorFingerprint };

export interface AetherMirrorSyncInput {
  readonly workspace: AetherMirrorWorkspace;
  /** The provider's claim on the thread's worktree. The mirror writes only there, as its owner. */
  readonly claim: WorktreeClaim;
  /** Keys the mirror record, so a record another task left is never trusted. */
  readonly taskId: AetherTaskId;
  /** The task's base commit. Every sync resets the worktree to it before applying the diff. */
  readonly baseCommit: string;
}

const OPERATION = "AetherMirrorSync";
const RECORD_FILE = "synara-aether-mirror.json";

const MirrorRecord = Schema.Struct({
  taskId: Schema.String,
  baseCommit: Schema.String,
  head: Schema.String,
  tree: Schema.String,
  /** Digest of the diff and binary bytes the tree was built from; null after a failed sync. */
  contentDigest: Schema.NullOr(Schema.String),
  /** Files the sync created, which the next sync removes before its reset. */
  created: Schema.Array(Schema.String),
});
type MirrorRecord = typeof MirrorRecord.Type;
const decodeMirrorRecord = decodeJsonResult(MirrorRecord);

const sameFingerprint = (left: AetherMirrorFingerprint, right: AetherMirrorFingerprint) =>
  left.head === right.head && left.tree === right.tree;

const hasErrorCode = (cause: unknown, code: string) =>
  typeof cause === "object" && cause !== null && "code" in cause && cause.code === code;

const describe = (cause: unknown) => (cause instanceof Error ? cause.message : String(cause));

type FailedStep = MirrorApplyFailedError["step"];

/** The worktree and the services every step uses. */
interface MirrorContext {
  readonly git: GitCoreShape;
  readonly fs: FileSystem.FileSystem;
  readonly path: Path.Path;
  readonly worktree: string;
  readonly failed: (
    step: FailedStep,
    detail: string,
    extra?: { readonly path?: string; readonly cause?: unknown },
  ) => MirrorApplyFailedError;
}

const runGit = (context: MirrorContext, step: FailedStep, args: ReadonlyArray<string>) =>
  context.git
    .execute({ operation: `${OPERATION}.${step}`, cwd: context.worktree, args })
    .pipe(Effect.mapError((error) => context.failed(step, error.detail, { cause: error })));

/** Mirror the task's VM working tree into the claimed worktree. See the module comment. */
export const syncAetherMirror = (
  input: AetherMirrorSyncInput,
): Effect.Effect<
  AetherMirrorSyncResult,
  AetherMirrorSyncError | WorktreeOwnedError,
  GitCore | FileSystem.FileSystem | Path.Path
> =>
  Effect.gen(function* () {
    const worktree = input.claim.worktreePath;
    const context: MirrorContext = {
      git: yield* GitCore,
      fs: yield* FileSystem.FileSystem,
      path: yield* Path.Path,
      worktree,
      failed: (step, detail, extra = {}) =>
        new MirrorApplyFailedError({ worktreePath: worktree, step, detail, ...extra }),
    };

    // 1. The base, then everything the VM has to say, before taking the lock.
    const base = (yield* runGit(context, "check", [
      "rev-parse",
      "--verify",
      `${input.baseCommit}^{commit}`,
    ])).stdout.trim();
    const clean: AetherMirrorFingerprint = {
      head: base,
      tree: (yield* runGit(context, "check", [
        "rev-parse",
        "--verify",
        `${base}^{tree}`,
      ])).stdout.trim(),
    };
    const diff = yield* input.workspace
      .requestGitDiff("main")
      .pipe(
        Effect.mapError(
          (error) =>
            new MirrorTransportError({ request: "git.diff", detail: error.detail, cause: error }),
        ),
      );
    const planned = planAetherMirror(diff);
    if (Result.isFailure(planned)) return yield* planned.failure;
    const plan = planned.success;
    yield* checkPlan(context, base, plan);
    const bytes = new Map<string, Uint8Array>();
    for (const { path: file } of plan.writes) {
      const read = yield* input.workspace.readFile(file).pipe(
        Effect.mapError(
          (error) =>
            new MirrorTransportError({
              request: "files.read",
              path: file,
              detail: error.detail,
              cause: error,
            }),
        ),
      );
      bytes.set(file, Buffer.from(read.content, read.encoding === "base64" ? "base64" : "utf8"));
    }
    const digest = contentDigest(base, diff, bytes);

    // 2. The worktree, as its owner.
    const mirror = Effect.gen(function* () {
      const record = yield* readRecord(context, input.taskId);
      const before = yield* captureFingerprint(context, "inspect");
      const expected = record ?? clean;
      if (!sameFingerprint(before, expected) && !sameFingerprint(before, clean)) {
        return yield* localEdits(context, base, expected, before);
      }
      if (
        record !== undefined &&
        sameFingerprint(before, record) &&
        record.baseCommit === base &&
        record.contentDigest === digest
      ) {
        return { _tag: "unchanged", fingerprint: before } as const;
      }

      // The files the last sync created, which the reset leaves behind as untracked files. Kept
      // current as files are removed and created, so a failed sync records what is really there.
      // Trusted only when the worktree is exactly what that sync left.
      const created = new Set(
        record !== undefined && sameFingerprint(before, record) ? record.created : [],
      );
      const after = yield* Effect.gen(function* () {
        yield* removeCreated(context, created);
        yield* runGit(context, "reset", ["reset", "--hard", "--quiet", base]);
        // `git apply` creates all of its files or none: they are the mirror's once it succeeds.
        yield* Effect.uninterruptible(
          applyPatch(context, plan.patch).pipe(
            Effect.andThen(
              Effect.sync(() => {
                for (const file of plan.patchCreates) created.add(file);
              }),
            ),
          ),
        );
        yield* writeBinaries(context, plan, bytes, created);
        const synced = yield* captureFingerprint(context, "fingerprint");
        yield* writeRecord(context, {
          taskId: input.taskId,
          baseCommit: base,
          ...synced,
          contentDigest: digest,
          created: [...created],
        });
        return synced;
      }).pipe(
        // The worktree may be partly synced. Record what is there, so the next sync (which
        // rebuilds from the base) does not take it for local edits.
        Effect.onExit((exit) =>
          Exit.isSuccess(exit)
            ? Effect.void
            : captureFingerprint(context, "fingerprint").pipe(
                Effect.flatMap((left) =>
                  writeRecord(context, {
                    taskId: input.taskId,
                    baseCommit: base,
                    ...left,
                    contentDigest: null,
                    created: [...created],
                  }),
                ),
                Effect.catch((error) =>
                  Effect.logWarning("Could not record a partly synced Aether mirror", {
                    worktree,
                    detail: error.message,
                  }),
                ),
              ),
        ),
      );
      return sameFingerprint(after, before)
        ? ({ _tag: "unchanged", fingerprint: after } as const)
        : ({
            _tag: "synced",
            fingerprint: after,
            filesChanged: plan.fileCount,
            modeOnlyPaths: plan.modeOnly,
          } as const);
    });

    return yield* context.git
      .withMutation(worktree, mirror, { claim: input.claim })
      .pipe(
        Effect.catchTag("GitCommandError", (error) =>
          Effect.fail(context.failed("lock", error.detail, { cause: error })),
        ),
      );
  });

/** Stable for the same inputs, so an unchanged VM tree skips the reset and apply entirely. */
const contentDigest = (
  base: string,
  diff: AetherGitDiffResult,
  bytes: ReadonlyMap<string, Uint8Array>,
) => {
  const hash = createHash("sha256");
  hash.update(JSON.stringify({ base, files: diff.files }));
  for (const [file, content] of bytes) {
    hash.update(`\0${file}\0${content.byteLength}\0`);
    hash.update(content);
  }
  return hash.digest("hex");
};

/**
 * Before anything is read or written. Git checks every path the plan touches, with its Windows and
 * macOS `.git` protections on whatever the platform: binary files are written with `node:fs`, which
 * would follow a name such as `.git.` or `GIT~1` into the real `.git` on those systems. Then the
 * patch is checked against the base. Both use scratch indexes, so a hostile or stale diff changes
 * nothing.
 */
const checkPlan = (context: MirrorContext, base: string, plan: AetherMirrorPlan) =>
  Effect.gen(function* () {
    const directory = yield* context.fs.makeTempDirectoryScoped({
      prefix: "synara-aether-mirror-",
    });
    const operation = `${OPERATION}.check`;
    const cwd = context.worktree;
    if (plan.paths.length > 0) {
      const protect = ["-c", "core.protectNTFS=true", "-c", "core.protectHFS=true"];
      const blob = (yield* context.git.execute({
        operation,
        cwd,
        args: ["hash-object", "--stdin"],
        stdin: "",
      })).stdout.trim();
      const addPaths = (index: string, paths: ReadonlyArray<string>) =>
        context.git.execute({
          operation,
          cwd,
          args: [...protect, "update-index", "--add", "--replace", "-z", "--index-info"],
          env: { GIT_INDEX_FILE: context.path.join(directory, index) },
          stdin: paths.map((file) => `100644 ${blob}\t${file}\0`).join(""),
        });
      const listPaths = (index: string) =>
        context.git
          .execute({
            operation,
            cwd,
            args: ["ls-files", "-z"],
            env: { GIT_INDEX_FILE: context.path.join(directory, index) },
          })
          .pipe(Effect.map((result) => new Set(result.stdout.split("\0"))));
      // `--index-info` skips a path Git refuses ("Ignoring path"), so see what it kept.
      yield* addPaths("paths", plan.paths);
      const kept = yield* listPaths("paths");
      // A missing path can also be one `--replace` dropped for a file/directory clash in the
      // batch, so each is checked again on its own.
      for (const [position, file] of plan.paths.filter((p) => !kept.has(p)).entries()) {
        yield* addPaths(`path-${position}`, [file]);
        if (!(yield* listPaths(`path-${position}`)).has(file)) {
          return yield* new MirrorPathEscapeError({
            path: file,
            detail: "Git refuses it as a worktree path (it can name '.git' on Windows or macOS)",
          });
        }
      }
    }
    if (plan.patch.length > 0) {
      const env = { GIT_INDEX_FILE: context.path.join(directory, "index") };
      yield* context.git.execute({ operation, cwd, args: ["read-tree", base], env });
      const checked = yield* context.git.execute({
        operation,
        cwd,
        args: ["apply", "--cached", "--check", "--whitespace=nowarn", "-"],
        env,
        stdin: plan.patch,
        allowNonZeroExit: true,
      });
      if (checked.code !== 0) {
        return yield* context.failed(
          "check",
          `The diff does not apply to the task base: ${checked.stderr.trim()}`,
        );
      }
    }
  }).pipe(
    Effect.scoped,
    Effect.catchTags({
      GitCommandError: (error) =>
        Effect.fail(context.failed("check", error.detail, { cause: error })),
      PlatformError: (error) =>
        Effect.fail(context.failed("check", error.message, { cause: error })),
    }),
  );

const applyPatch = (context: MirrorContext, patch: string) =>
  patch.length === 0
    ? Effect.void
    : context.git
        .execute({
          operation: `${OPERATION}.apply`,
          cwd: context.worktree,
          args: ["apply", "--whitespace=nowarn", "-"],
          stdin: patch,
          allowNonZeroExit: true,
        })
        .pipe(
          Effect.mapError((error) => context.failed("apply", error.detail, { cause: error })),
          Effect.flatMap((applied) =>
            applied.code === 0
              ? Effect.void
              : Effect.fail(
                  context.failed(
                    "apply",
                    applied.stderr.trim() || `git apply exited with code ${applied.code}.`,
                  ),
                ),
          ),
        );

const captureFingerprint = (
  context: MirrorContext,
  step: "inspect" | "fingerprint",
): Effect.Effect<AetherMirrorFingerprint, MirrorApplyFailedError> =>
  Effect.gen(function* () {
    const head = yield* runGit(context, step, ["rev-parse", "--verify", "HEAD^{commit}"]);
    const directory = yield* context.fs.makeTempDirectoryScoped({
      prefix: "synara-aether-mirror-",
    });
    const tree = yield* writeWorkingTreeTree({
      git: context.git,
      fs: context.fs,
      path: context.path,
      operation: `${OPERATION}.${step}`,
      cwd: context.worktree,
      indexPath: context.path.join(directory, "index"),
    });
    return { head: head.stdout.trim(), tree };
  }).pipe(
    Effect.scoped,
    Effect.catchTags({
      GitCommandError: (error) => Effect.fail(context.failed(step, error.detail, { cause: error })),
      PlatformError: (error) => Effect.fail(context.failed(step, error.message, { cause: error })),
    }),
  );

/** Names what changed against the state the mirror expected. Never touches the worktree. */
const localEdits = (
  context: MirrorContext,
  base: string,
  expected: AetherMirrorFingerprint,
  actual: AetherMirrorFingerprint,
) =>
  context.git
    .execute({
      operation: `${OPERATION}.inspect`,
      cwd: context.worktree,
      args: ["diff", "--name-only", "--no-renames", "-z", expected.tree, actual.tree],
      allowNonZeroExit: true,
    })
    .pipe(
      // The expected tree can be gone (pruned); the refusal stands without the list.
      Effect.map((listed) =>
        listed.code === 0 ? listed.stdout.split("\0").filter((entry) => entry.length > 0) : [],
      ),
      Effect.orElseSucceed((): ReadonlyArray<string> => []),
      Effect.flatMap((paths) =>
        Effect.fail(
          new MirrorLocalEditsError({
            worktreePath: context.worktree,
            paths,
            ...(actual.head === expected.head
              ? {}
              : { headMoved: { expected: expected.head, actual: actual.head } }),
            baseCommit: base,
          }),
        ),
      ),
    );

const recordPath = (context: MirrorContext) =>
  runGit(context, "inspect", ["rev-parse", "--absolute-git-dir"]).pipe(
    Effect.map((result) => nodePath.join(result.stdout.trim(), RECORD_FILE)),
  );

/** This task's record, or undefined when there is none, or it is another task's or corrupt. */
const readRecord = (context: MirrorContext, taskId: AetherTaskId) =>
  recordPath(context).pipe(
    Effect.flatMap((file) =>
      Effect.tryPromise({
        try: () =>
          nodeFs.readFile(file, "utf8").catch((cause: unknown) => {
            if (hasErrorCode(cause, "ENOENT")) return undefined;
            throw cause;
          }),
        catch: (cause) =>
          context.failed("inspect", `Could not read the mirror record: ${describe(cause)}`, {
            cause,
          }),
      }),
    ),
    Effect.map((text) => {
      if (text === undefined) return undefined;
      const record = decodeMirrorRecord(text);
      return Result.isSuccess(record) && record.success.taskId === taskId
        ? record.success
        : undefined;
    }),
  );

const writeRecord = (context: MirrorContext, record: MirrorRecord) =>
  recordPath(context).pipe(
    Effect.flatMap((file) =>
      Effect.tryPromise({
        try: async () => {
          const temporary = `${file}.${process.pid}.${randomUUID()}.tmp`;
          try {
            await nodeFs.writeFile(temporary, JSON.stringify(record), "utf8");
            await nodeFs.rename(temporary, file);
          } finally {
            await nodeFs.rm(temporary, { force: true });
          }
        },
        catch: (cause) =>
          context.failed("record", `Could not write the mirror record: ${describe(cause)}`, {
            cause,
          }),
      }),
    ),
  );

const O_NOFOLLOW = supportsPosixPermissions() ? fsConstants.O_NOFOLLOW : 0;

const lstatOrUndefined = (target: string) =>
  nodeFs.lstat(target).catch((cause: unknown) => {
    if (hasErrorCode(cause, "ENOENT")) return undefined;
    throw cause;
  });

/**
 * One filesystem step. Uninterruptible, so the `created` bookkeeping it does always matches the
 * disk when a failed or interrupted sync records what it left.
 */
const attemptFs = <A>(
  context: MirrorContext,
  step: "remove" | "write",
  file: string,
  action: () => Promise<A>,
) =>
  Effect.uninterruptible(
    Effect.tryPromise({
      try: action,
      catch: (cause) =>
        cause instanceof MirrorPathEscapeError || cause instanceof MirrorApplyFailedError
          ? cause
          : context.failed(step, describe(cause), { path: file, cause }),
    }),
  );

/** Unlinks a file (or symlink) at `target` if one is there; a directory is an error. */
const unlinkFile = async (target: string) => {
  const stat = await lstatOrUndefined(target);
  if (stat?.isDirectory() === true) throw new Error("It is a directory in the worktree.");
  if (stat !== undefined) await nodeFs.unlink(target);
};

/**
 * Removes the files the last sync created, and the directories above them that are left empty,
 * deleting each from `created` as it goes. Nothing else in the worktree is touched.
 */
const removeCreated = (context: MirrorContext, created: Set<string>) =>
  Effect.gen(function* () {
    if (created.size === 0) return;
    const root = yield* attemptFs(context, "remove", ".", () => nodeFs.realpath(context.worktree));
    // Deleting the entry being visited is safe while iterating a Set.
    for (const file of created) {
      const escape = mirrorPathEscape(file);
      if (escape !== undefined) {
        return yield* new MirrorPathEscapeError({ path: file, detail: escape });
      }
      yield* attemptFs(context, "remove", file, async () => {
        const target = await resolveTarget(context, root, file, "remove", false);
        if (target !== undefined) {
          await unlinkFile(target);
          for (
            let directory = nodePath.dirname(target);
            directory.length > root.length && directory.startsWith(root);
            directory = nodePath.dirname(directory)
          ) {
            // Stops at the first directory that still holds something (or cannot be removed).
            const removed = await nodeFs.rmdir(directory).then(
              () => true,
              () => false,
            );
            if (!removed) break;
          }
        }
        created.delete(file);
      });
    }
  });

/**
 * Removes, then writes: one cumulative diff can name a path as one entry's removal and another's
 * write (a deleted `a` beside a rename to `a`). Every path is resolved before anything changes,
 * so a refusal removes nothing. A file the diff adds must not exist yet (as `git apply` insists
 * for text files): whatever is there, such as an ignored local file, is not the mirror's.
 */
const writeBinaries = (
  context: MirrorContext,
  plan: AetherMirrorPlan,
  bytes: ReadonlyMap<string, Uint8Array>,
  created: Set<string>,
) =>
  Effect.gen(function* () {
    if (plan.removals.length === 0 && plan.writes.length === 0) return;
    const root = yield* attemptFs(context, "write", ".", () => nodeFs.realpath(context.worktree));
    const resolve = (step: "remove" | "write", file: string, create: boolean) =>
      attemptFs(context, step, file, () => resolveTarget(context, root, file, step, create));

    const removals: Array<readonly [string, string | undefined]> = [];
    for (const file of plan.removals) removals.push([file, yield* resolve("remove", file, false)]);
    for (const write of plan.writes) yield* resolve("write", write.path, false);

    for (const [file, target] of removals) {
      if (target !== undefined) yield* attemptFs(context, "remove", file, () => unlinkFile(target));
    }
    for (const write of plan.writes) {
      const file = write.path;
      const target = (yield* resolve("write", file, true))!;
      yield* attemptFs(context, "write", file, async () => {
        const stat = await lstatOrUndefined(target);
        if (write.create && stat !== undefined) {
          throw context.failed("write", "Something is already there in the local worktree.", {
            path: file,
          });
        }
        // A symlink is replaced, never followed. A regular file is truncated in place so it
        // keeps its mode. Anything new is created exclusively, and is the mirror's.
        if (stat?.isSymbolicLink() === true) await nodeFs.unlink(target);
        const handle =
          stat?.isFile() === true
            ? await nodeFs.open(target, fsConstants.O_WRONLY | fsConstants.O_TRUNC | O_NOFOLLOW)
            : await nodeFs.open(
                target,
                fsConstants.O_WRONLY | fsConstants.O_CREAT | fsConstants.O_EXCL | O_NOFOLLOW,
                0o666,
              );
        if (stat === undefined) created.add(file);
        try {
          await handle.writeFile(bytes.get(file)!);
        } finally {
          await handle.close();
        }
      });
    }
  });

/**
 * Where `relative` lives under `root`, refusing any existing parent that is a symlink or not a
 * directory. Binary paths bypass `git apply`'s own checks, and a lexical check cannot see a
 * symlinked directory, so this is what keeps them inside the worktree. With `create`, missing
 * parents are made one at a time and checked again. Without it, a missing parent means there is
 * nothing to find: undefined.
 */
const resolveTarget = async (
  context: MirrorContext,
  root: string,
  relative: string,
  step: "remove" | "write",
  create: boolean,
): Promise<string | undefined> => {
  // Node's own separators: a backslash is a separator on Windows and a name character elsewhere.
  const segments = nodePath.relative(root, nodePath.join(root, relative)).split(nodePath.sep);
  const leaf = segments.pop()!;
  let current = root;
  for (const segment of segments) {
    current = nodePath.join(current, segment);
    let stat = await lstatOrUndefined(current);
    if (stat === undefined) {
      if (!create) return undefined;
      await nodeFs.mkdir(current).catch((cause: unknown) => {
        if (!hasErrorCode(cause, "EEXIST")) throw cause;
      });
      stat = await nodeFs.lstat(current);
    }
    if (stat.isSymbolicLink()) {
      throw new MirrorPathEscapeError({
        path: relative,
        detail: `'${segment}' is a symlink in the local worktree`,
      });
    }
    if (!stat.isDirectory()) {
      throw context.failed(step, `'${segment}' is not a directory in the local worktree.`, {
        path: relative,
      });
    }
  }
  return nodePath.join(current, leaf);
};

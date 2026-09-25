// FILE: workingTreeTree.ts
// Purpose: Writes a worktree's current content (tracked and untracked files; ignored files
// excluded) as a Git tree object, through a throwaway index seeded from the real one.
// Layer: Server Git helper. Used by checkpoint capture and the Aether mirror fingerprint.
import { Effect, type FileSystem, type Path, type PlatformError } from "effect";

import { GitCommandError } from "./Errors.ts";
import type { GitCoreShape } from "./Services/GitCore.ts";

export interface WriteWorkingTreeTreeInput {
  readonly git: GitCoreShape;
  readonly fs: FileSystem.FileSystem;
  readonly path: Path.Path;
  readonly operation: string;
  readonly cwd: string;
  /** The throwaway index file. The caller owns its directory and removes it. */
  readonly indexPath: string;
  readonly env?: NodeJS.ProcessEnv;
}

/** Returns the tree object id. Neither the real index nor the worktree is modified. */
export const writeWorkingTreeTree = (
  input: WriteWorkingTreeTreeInput,
): Effect.Effect<string, GitCommandError | PlatformError.PlatformError> =>
  Effect.gen(function* () {
    const { git, fs, operation, cwd } = input;
    const env: NodeJS.ProcessEnv = { ...input.env, GIT_INDEX_FILE: input.indexPath };

    const workingIndexInfo = yield* seedIndex(input);
    if (workingIndexInfo === null) {
      const head = yield* git.execute({
        operation,
        cwd,
        args: ["rev-parse", "--verify", "HEAD"],
        allowNonZeroExit: true,
      });
      if (head.code === 0) {
        yield* git.execute({ operation, cwd, args: ["read-tree", "HEAD"], env });
      }
    } else {
      // A copied index can describe a rapid same-size rewrite as clean
      // when its cached stat tuple still matches. Really-refresh makes
      // Git verify those racily-clean entries and leaves changed paths
      // for the following add to hash, without discarding the cache for
      // the rest of a large workspace.
      yield* git.execute({
        operation,
        cwd,
        args: ["update-index", "--really-refresh"],
        env,
        allowNonZeroExit: true,
      });

      // Copying or refreshing the temporary index advances its file
      // timestamp. Git uses that timestamp to decide whether an entry
      // whose cached stat tuple still matches is "racily clean" and
      // needs hashing. Restore the working index's original timestamp
      // so rapid same-size rewrites remain newer than (or equal to) the
      // index snapshot and `git add` verifies their contents.
      if (workingIndexInfo.mtime !== undefined) {
        yield* fs.utimes(
          input.indexPath,
          workingIndexInfo.atime ?? workingIndexInfo.mtime,
          workingIndexInfo.mtime,
        );
      }
    }

    yield* git.execute({ operation, cwd, args: ["add", "-A", "--", "."], env });

    const treeOid = (yield* git.execute({
      operation,
      cwd,
      args: ["write-tree"],
      env,
    })).stdout.trim();
    if (treeOid.length === 0) {
      return yield* new GitCommandError({
        operation,
        command: "git write-tree",
        cwd,
        detail: "git write-tree returned an empty tree oid.",
      });
    }
    return treeOid;
  });

/**
 * Copies the real index to `indexPath` and returns its stat, or null when there is none.
 *
 * Preserve Git's stat cache in the throwaway index. Starting every capture from
 * `read-tree HEAD` discards it, forcing `git add -A` to rescan and re-hash the whole worktree.
 */
const seedIndex = (input: WriteWorkingTreeTreeInput) =>
  Effect.gen(function* () {
    const { git, fs, path, operation, cwd } = input;
    const indexPathResult = yield* git.execute({
      operation,
      cwd,
      args: ["rev-parse", "--git-path", "index"],
      allowNonZeroExit: true,
    });
    const indexPathRaw = indexPathResult.stdout.trim();
    if (indexPathResult.code !== 0 || indexPathRaw.length === 0) {
      return null;
    }

    const indexPath = path.isAbsolute(indexPathRaw)
      ? indexPathRaw
      : path.resolve(cwd, indexPathRaw);
    const indexExists = yield* fs.exists(indexPath).pipe(Effect.orElseSucceed(() => false));
    if (!indexExists) {
      return null;
    }
    const indexInfo = yield* fs.stat(indexPath);
    yield* fs.copyFile(indexPath, input.indexPath);
    return indexInfo;
  });

// FILE: WorktreeOwnership.ts
// Purpose: In-memory registry of provider-owned worktrees, shared by claims and GitCore.withMutation.
// Layer: Server Git service
// Exports: makeWorktreeOwnership, built by makeGitCore so claims share its repository lock.
import { isWorkspaceRootWithin, workspaceRootsEqual } from "@synara/shared/threadWorkspace";
import { Effect, FileSystem, Path } from "effect";

import { type GitCommandError, WorktreeOwnedError } from "../Errors.ts";
import type { GitMutationOptions } from "../Services/GitCore.ts";
import type {
  OwnedWorktree,
  WorktreeClaim,
  WorktreeOwner,
  WorktreeOwnershipShape,
} from "../Services/WorktreeOwnership.ts";
import { realpathNearestExisting } from "../../realpathNearestExisting.ts";

interface OwnershipEntry extends OwnedWorktree {
  readonly claims: Set<WorktreeClaim>;
}

export interface WorktreeOwnershipDependencies {
  /** Run `effect` holding the per-repository lock that `GitCore.withMutation` uses. */
  readonly withRepositoryLock: <A, E, R>(
    cwd: string,
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | GitCommandError, R>;
  /** Canonical (realpath) root of the worktree containing `path`. */
  readonly resolveWorktreeRoot: (path: string) => Effect.Effect<string, GitCommandError>;
  /** Branch name → path of the worktree that has it checked out, for the repository at `cwd`. */
  readonly listCheckedOutBranches: (
    cwd: string,
  ) => Effect.Effect<ReadonlyMap<string, string>, GitCommandError>;
}

const pathOptions = { platform: process.platform };

const sameOwner = (left: WorktreeOwner, right: WorktreeOwner) =>
  left.threadId === right.threadId && left.provider === right.provider;

const ownedError = (entry: OwnedWorktree) =>
  new WorktreeOwnedError({
    worktreePath: entry.worktreePath,
    ownerThreadId: entry.owner.threadId,
    provider: entry.owner.provider,
  });

export const makeWorktreeOwnership = Effect.fn("makeWorktreeOwnership")(function* (
  dependencies: WorktreeOwnershipDependencies,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  // Keyed by canonical worktree root. Mutated only synchronously, so readers
  // never observe a half-applied claim or release.
  const entries = new Map<string, OwnershipEntry>();

  const canonicalize = (inputPath: string) =>
    realpathNearestExisting(inputPath).pipe(
      Effect.provideService(FileSystem.FileSystem, fileSystem),
      Effect.provideService(Path.Path, path),
    );

  const findCovering = (canonicalPath: string): OwnershipEntry | null => {
    for (const entry of entries.values()) {
      if (isWorkspaceRootWithin(canonicalPath, entry.worktreePath, pathOptions)) {
        return entry;
      }
    }
    return null;
  };

  const ownerOf: WorktreeOwnershipShape["ownerOf"] = (inputPath) =>
    entries.size === 0
      ? Effect.succeed(null)
      : canonicalize(inputPath).pipe(
          Effect.map((canonicalPath) => {
            const entry = findCovering(canonicalPath);
            return entry ? { worktreePath: entry.worktreePath, owner: entry.owner } : null;
          }),
        );

  const assertNotOwned: WorktreeOwnershipShape["assertNotOwned"] = (inputPath, claim) =>
    entries.size === 0
      ? Effect.void
      : canonicalize(inputPath).pipe(
          Effect.flatMap((canonicalPath) => {
            const entry = findCovering(canonicalPath);
            return entry && !(claim !== undefined && entry.claims.has(claim))
              ? Effect.fail(ownedError(entry))
              : Effect.void;
          }),
        );

  /** The ownership check `GitCore.withMutation` runs under the repository lock. */
  const assertMutationAllowed = (
    cwd: string,
    options: GitMutationOptions | undefined,
  ): Effect.Effect<void, WorktreeOwnedError | GitCommandError> =>
    Effect.gen(function* () {
      if (entries.size === 0) {
        return;
      }
      const claim = options?.claim;
      for (const worktreePath of [cwd, ...(options?.targets?.worktreePaths ?? [])]) {
        yield* assertNotOwned(worktreePath, claim);
      }
      const branches = options?.targets?.branches ?? [];
      if (branches.length === 0) {
        return;
      }
      const checkedOut = yield* dependencies.listCheckedOutBranches(cwd);
      for (const branch of branches) {
        const worktreePath = checkedOut.get(branch);
        if (worktreePath !== undefined) {
          yield* assertNotOwned(worktreePath, claim);
        }
      }
    });

  const release = (claim: WorktreeClaim) =>
    Effect.sync(() => {
      const entry = entries.get(claim.worktreePath);
      if (!entry?.claims.delete(claim)) {
        return;
      }
      if (entry.claims.size === 0) {
        entries.delete(claim.worktreePath);
      }
    });

  const claim: WorktreeOwnershipShape["claim"] = (worktreePath, owner) =>
    Effect.acquireRelease(
      dependencies.resolveWorktreeRoot(worktreePath).pipe(
        Effect.flatMap((root) =>
          dependencies.withRepositoryLock(
            root,
            Effect.suspend(() => {
              let entry: OwnershipEntry | undefined;
              for (const existing of entries.values()) {
                const overlaps =
                  isWorkspaceRootWithin(root, existing.worktreePath, pathOptions) ||
                  isWorkspaceRootWithin(existing.worktreePath, root, pathOptions);
                const sameClaimTarget =
                  workspaceRootsEqual(root, existing.worktreePath, pathOptions) &&
                  sameOwner(existing.owner, owner);
                if (overlaps && !sameClaimTarget) {
                  return Effect.fail(ownedError(existing));
                }
                if (sameClaimTarget) {
                  entry = existing;
                }
              }
              if (!entry) {
                entry = { worktreePath: root, owner, claims: new Set() };
                entries.set(root, entry);
              }
              const granted: WorktreeClaim = Object.freeze({
                _tag: "WorktreeClaim",
                worktreePath: entry.worktreePath,
                owner: entry.owner,
              });
              entry.claims.add(granted);
              return Effect.succeed(granted);
            }),
          ),
        ),
      ),
      release,
    );

  return {
    service: { claim, ownerOf, assertNotOwned } satisfies WorktreeOwnershipShape,
    assertMutationAllowed,
  };
});

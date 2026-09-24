/**
 * WorktreeOwnership - In-process registry of worktrees that a provider owns.
 *
 * A provider that mirrors remote changes into a thread's local worktree (reset
 * to base, then apply the remote diff) claims that worktree for as long as its
 * session runs. While it is owned, `GitCore.withMutation` refuses every other
 * mutation inside it or aimed at it; the owner passes by running its mutations
 * with its claim.
 *
 * A claim is taken under the same per-repository lock as `GitCore.withMutation`
 * (which is why `GitCoreLive` builds this service), so it never interleaves
 * with a mutation run through `withMutation`, including checkpoint restores
 * (revert, edit-and-resend), which run under it. Editor saves only check
 * `assertNotOwned` before writing, without the lock, so a save already in
 * flight when the claim is taken can still land.
 *
 * Ownership is in memory by design and does not survive a server restart: the
 * provider claims the worktree again when its session resumes. It is unrelated
 * to the agent gateway's durable creation proof (`GitCore.recordWorktreeOwnership`).
 *
 * @module WorktreeOwnership
 */
import type { ProviderKind, ThreadId } from "@synara/contracts";
import { ServiceMap } from "effect";
import type { Effect, Scope } from "effect";

import type { GitCommandError, WorktreeOwnedError } from "../Errors.ts";

export interface WorktreeOwner {
  readonly threadId: ThreadId;
  readonly provider: ProviderKind;
}

/** An owned worktree: its canonical (realpath) root and its owner. */
export interface OwnedWorktree {
  readonly worktreePath: string;
  readonly owner: WorktreeOwner;
}

/**
 * Proof of ownership handed to the claimant. Pass it to `GitCore.withMutation`
 * to run the owner's own mutations; it stops working once its scope closes.
 */
export interface WorktreeClaim extends OwnedWorktree {
  readonly _tag: "WorktreeClaim";
}

export interface WorktreeOwnershipShape {
  /**
   * Claim the worktree containing `worktreePath` until the scope closes. Waits
   * for in-flight mutations of the repository. Fails with `WorktreeOwnedError`
   * when a different owner holds it (or a worktree nested in it); the same owner
   * may hold several claims, and ownership ends with the last one.
   */
  readonly claim: (
    worktreePath: string,
    owner: WorktreeOwner,
  ) => Effect.Effect<WorktreeClaim, WorktreeOwnedError | GitCommandError, Scope.Scope>;

  /** The ownership covering `path` (an owned worktree root or any path inside it), if any. */
  readonly ownerOf: (path: string) => Effect.Effect<OwnedWorktree | null>;

  /** Fail with `WorktreeOwnedError` when `path` is covered and `claim` is not its live claim. */
  readonly assertNotOwned: (
    path: string,
    claim?: WorktreeClaim,
  ) => Effect.Effect<void, WorktreeOwnedError>;
}

export class WorktreeOwnership extends ServiceMap.Service<
  WorktreeOwnership,
  WorktreeOwnershipShape
>()("synara/git/Services/WorktreeOwnership") {}

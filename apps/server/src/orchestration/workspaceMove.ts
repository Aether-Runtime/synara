// FILE: workspaceMove.ts
// Purpose: The one rule for moving a thread's workspace under a provider that owns its worktree.
// Layer: Orchestration policy, shared by the decider and the git handoff RPC.

import type { OrchestrationThread } from "@synara/contracts";
import type { ProviderGatedFeature } from "@synara/shared/providerMetadata";

/**
 * The provider capability a workspace move needs, or null when it needs none. A provider that
 * owns its worktree never runs in the local checkout, and once a turn has started never leaves
 * the worktree it started in; before then its worktree can be reset. The decider asks this for
 * thread.meta.update, and the git handoff RPC asks it before any git call, so a handoff is never
 * refused only after git has moved the checkout.
 */
export function workspaceMoveFeature(
  thread: Pick<OrchestrationThread, "session" | "latestTurn">,
  move: { readonly toLocal: boolean; readonly changesWorktree: boolean },
): ProviderGatedFeature | null {
  if (move.toLocal) return "local-workspace";
  return move.changesWorktree && (thread.session !== null || thread.latestTurn !== null)
    ? "worktree-change"
    : null;
}

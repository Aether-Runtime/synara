// FILE: threadEnvironment.ts
// Purpose: Shared helpers for deriving thread environment intent and fork targets.
// Layer: Web domain helpers
// Exports: thread env resolution + `/fork` target planning

import type { ProviderKind, ThreadEnvironmentMode } from "@synara/contracts";
import {
  PROVIDER_DESCRIPTOR_BY_KIND,
  providerIsCloudService,
  providerSupportsFeature,
} from "@synara/shared/providerMetadata";
import {
  isPendingThreadWorktree,
  resolveThreadEnvironmentMode,
  resolveThreadWorkspaceCwd,
  resolveThreadWorkspaceState,
  type ResolvedThreadWorkspaceState,
} from "@synara/shared/threadEnvironment";
import { deriveAssociatedWorktreeMetadata } from "@synara/shared/threadWorkspace";
import type { Thread } from "../types";

export type ForkThreadTarget = "local" | "worktree";

export interface ResolvedForkThreadEnvironment {
  target: ForkThreadTarget;
  envMode: ThreadEnvironmentMode;
  branch: string | null;
  worktreePath: string | null;
  associatedWorktreePath: string | null;
  associatedWorktreeBranch: string | null;
  associatedWorktreeRef: string | null;
}

export {
  isPendingThreadWorktree,
  resolveThreadEnvironmentMode,
  resolveThreadWorkspaceState,
} from "@synara/shared/threadEnvironment";

/**
 * The env mode a thread works in with `provider`. A provider that owns its worktree only runs
 * in a new one, so this overlays "worktree" at render and send time. Never write the result
 * back to an existing draft, project or thread: writers keep the persisted mode, so switching
 * a draft to another provider returns to it. A new Kanban task is created with it, because the
 * board dispatches from the draft's own mode.
 */
export function resolveProviderEnvMode(
  envMode: ThreadEnvironmentMode,
  provider: ProviderKind,
): ThreadEnvironmentMode {
  return providerSupportsFeature(provider, "local-workspace") ? envMode : "worktree";
}

/** The "Work in" row of a cloud provider: the location first, the service running it after. */
export function cloudLocationLabel(cloud: ProviderKind): string {
  return `Cloud (${PROVIDER_DESCRIPTOR_BY_KIND[cloud].displayName})`;
}

/** `provider` when it is a cloud service; null when it runs on this machine. */
export function resolveCloudProvider(provider: ProviderKind): ProviderKind | null {
  return providerIsCloudService(provider) ? provider : null;
}

export interface ThreadEnvironmentPresentation {
  mode: ThreadEnvironmentMode;
  workspaceState: ResolvedThreadWorkspaceState;
  /** The cloud provider the thread runs on; null when it runs on this machine. */
  cloud: ProviderKind | null;
  shortLabel: "Local" | "Worktree" | "Cloud";
  localOptionLabel: "Local project";
  worktreeOptionLabel: "Worktree";
  worktreeBadgeLabel: "Worktree" | "Worktree pending" | null;
  /** The tooltip of a cloud thread's badge; null when it runs on this machine. */
  cloudBadgeLabel: string | null;
}

export function resolveThreadEnvironmentPresentation(input: {
  envMode?: ThreadEnvironmentMode | null | undefined;
  worktreePath?: string | null | undefined;
  cloud?: ProviderKind | null | undefined;
}): ThreadEnvironmentPresentation {
  const mode = resolveThreadEnvironmentMode(input);
  const workspaceState = resolveThreadWorkspaceState(input);
  const cloud = input.cloud ?? null;

  return {
    mode,
    workspaceState,
    cloud,
    shortLabel: cloud !== null ? "Cloud" : mode === "worktree" ? "Worktree" : "Local",
    localOptionLabel: "Local project",
    worktreeOptionLabel: "Worktree",
    worktreeBadgeLabel:
      workspaceState === "worktree-ready"
        ? "Worktree"
        : workspaceState === "worktree-pending"
          ? "Worktree pending"
          : null,
    cloudBadgeLabel:
      cloud === null
        ? null
        : `Runs in the cloud (${PROVIDER_DESCRIPTOR_BY_KIND[cloud].displayName})`,
  };
}

export interface DiffEnvironmentState {
  pending: boolean;
  cwd: string | null;
  disabledReason: string | null;
}

// Diff surfaces stay disabled while a worktree-intended chat is still waiting for its path.
export function resolveDiffEnvironmentState(input: {
  projectCwd?: string | null | undefined;
  envMode?: ThreadEnvironmentMode | null | undefined;
  worktreePath?: string | null | undefined;
}): DiffEnvironmentState {
  const pending = isPendingThreadWorktree(input);
  return {
    pending,
    cwd: pending
      ? null
      : resolveThreadWorkspaceCwd({
          projectCwd: input.projectCwd,
          envMode: input.envMode,
          worktreePath: input.worktreePath,
        }),
    disabledReason: pending
      ? "Diff and summary will be available once the worktree is ready for this chat."
      : null,
  };
}

// Fork planning keeps "local" attached to the current local checkout. For worktree-backed
// threads that means reusing the existing worktree, while "worktree" always plans a new one.
export function resolveForkThreadEnvironment(input: {
  target: ForkThreadTarget;
  activeRootBranch: string | null;
  sourceThread: Pick<
    Thread,
    | "branch"
    | "envMode"
    | "worktreePath"
    | "associatedWorktreePath"
    | "associatedWorktreeBranch"
    | "associatedWorktreeRef"
  >;
}): ResolvedForkThreadEnvironment {
  const sourceEnvMode = resolveThreadEnvironmentMode({
    envMode: input.sourceThread.envMode,
    worktreePath: input.sourceThread.worktreePath,
  });
  const sourceBranch = input.sourceThread.branch ?? input.activeRootBranch;
  const sourceWorktreePath = input.sourceThread.worktreePath ?? null;
  const sourceAssociatedWorktreePath =
    input.sourceThread.associatedWorktreePath ?? sourceWorktreePath;
  const sourceAssociatedWorktreeBranch =
    input.sourceThread.associatedWorktreeBranch ?? sourceBranch;
  const sourceAssociatedWorktreeRef =
    input.sourceThread.associatedWorktreeRef ?? sourceAssociatedWorktreeBranch;

  if (input.target === "worktree") {
    const associatedWorktree = deriveAssociatedWorktreeMetadata({
      associatedWorktreePath: null,
      associatedWorktreeBranch: sourceBranch,
      associatedWorktreeRef: sourceAssociatedWorktreeRef ?? sourceBranch,
    });
    return {
      target: "worktree",
      envMode: "worktree",
      branch: sourceBranch,
      worktreePath: null,
      ...associatedWorktree,
    };
  }

  // Codex-style "Fork Into Local" stays in the current local checkout, which for a
  // worktree-backed thread means reusing that worktree rather than bouncing to root.
  if (sourceEnvMode === "worktree" && sourceWorktreePath) {
    const associatedWorktree = deriveAssociatedWorktreeMetadata({
      branch: sourceBranch,
      worktreePath: sourceWorktreePath,
      associatedWorktreePath: sourceAssociatedWorktreePath,
      associatedWorktreeBranch: sourceAssociatedWorktreeBranch,
      associatedWorktreeRef: sourceAssociatedWorktreeRef,
    });
    return {
      target: "local",
      envMode: "worktree",
      branch: sourceBranch,
      worktreePath: sourceWorktreePath,
      ...associatedWorktree,
    };
  }

  const associatedWorktree = deriveAssociatedWorktreeMetadata({
    associatedWorktreePath: null,
    associatedWorktreeBranch: null,
    associatedWorktreeRef: null,
  });
  return {
    target: "local",
    envMode: "local",
    branch: sourceBranch,
    worktreePath: null,
    ...associatedWorktree,
  };
}

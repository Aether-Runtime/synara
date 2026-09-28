// FILE: KanbanNewTaskDialog.logic.ts
// Purpose: Pure helpers for the kanban new-task composer dialog.
// Layer: Kanban UI logic (no React/store side effects)
// Exports: transcript merge, task preview and workspace gate helpers.

import type { ProviderKind } from "@synara/contracts";
import {
  type ProviderGatedFeature,
  providerFeatureUnavailableMessage,
  providerSupportsFeature,
} from "@synara/shared/providerMetadata";

export function appendKanbanTaskTranscript(current: string, transcript: string): string {
  const trimmed = transcript.trim();
  if (trimmed.length === 0) {
    return current;
  }
  if (current.trim().length === 0) {
    return trimmed;
  }
  return `${current.trimEnd()} ${trimmed}`;
}

export function buildKanbanTaskPreview(input: {
  readonly trimmedPrompt: string;
  readonly firstImageName?: string | null | undefined;
  readonly assistantSelectionCount: number;
}): string {
  if (input.trimmedPrompt.length > 0) {
    return input.trimmedPrompt;
  }
  if (input.firstImageName) {
    return `Image: ${input.firstImageName}`;
  }
  if (input.assistantSelectionCount > 0) {
    return "Referenced assistant selection";
  }
  return "New task";
}

export function truncateKanbanTaskPreview(preview: string, maxLength = 80): string {
  return preview.length > maxLength ? `${preview.slice(0, maxLength)}…` : preview;
}

/**
 * The composer's workspace gate for a new task: where no worktree can exist (the Chats board, a
 * folder without git) a provider needs `non-git-workspace`. `unavailableReason` is what the send
 * would refuse the selected provider with (a cloud there), or null when it can run.
 */
export function resolveKanbanTaskWorkspace(input: {
  readonly provider: ProviderKind;
  readonly isHomeChatContainer: boolean;
  /** Undefined until the project's git status is known. */
  readonly isGitRepo: boolean | undefined;
}): { readonly feature: ProviderGatedFeature | null; readonly unavailableReason: string | null } {
  const feature =
    input.isHomeChatContainer || input.isGitRepo === false ? "non-git-workspace" : null;
  return {
    feature,
    unavailableReason:
      feature !== null && !providerSupportsFeature(input.provider, feature)
        ? providerFeatureUnavailableMessage(input.provider, feature)
        : null,
  };
}

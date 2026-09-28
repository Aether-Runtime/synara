import { ProjectId } from "@synara/contracts";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { useComposerDraftStore } from "../composerDraftStore";
import { resetComposerDraftStore } from "../composerDraftStoreTestFixtures";
import type { SidebarThreadSummary } from "../types";
import { dispatchKanbanDraftThread } from "./kanbanDispatch";
import { createKanbanDraftTask } from "./kanbanTaskCreate";

const nativeApiMocks = vi.hoisted(() => ({
  dispatchCommand: vi.fn(async () => undefined),
}));

vi.mock("../nativeApi", () => ({
  readNativeApi: () => ({
    orchestration: {
      dispatchCommand: nativeApiMocks.dispatchCommand,
    },
  }),
}));

vi.mock("../kanbanUiStore", () => ({
  useKanbanUiStore: {
    getState: () => ({
      markOptimisticDispatch: () => undefined,
      clearOptimisticDispatch: () => undefined,
    }),
  },
}));

describe("Kanban task Debug mode", () => {
  beforeEach(() => {
    resetComposerDraftStore();
    nativeApiMocks.dispatchCommand.mockClear();
  });

  it("preserves Debug in the draft and dispatched turn", async () => {
    const projectId = ProjectId.makeUnsafe("project-kanban-debug");
    const threadId = createKanbanDraftTask({
      projectId,
      prompt: "Investigate the failing task",
      modelSelection: { provider: "codex", model: "gpt-5.4" },
      runtimeMode: "approval-required",
      interactionMode: "debug",
      envMode: "local",
    });

    expect(useComposerDraftStore.getState().getDraftThread(threadId)?.interactionMode).toBe(
      "debug",
    );
    expect(useComposerDraftStore.getState().draftsByThreadId[threadId]?.interactionMode).toBe(
      "debug",
    );

    const result = await dispatchKanbanDraftThread({
      threadId,
      projectId,
      thread: { id: threadId, projectId } as unknown as SidebarThreadSummary,
      defaultProvider: "codex",
      assistantDeliveryMode: "buffered",
    });

    expect(result).toEqual({ kind: "dispatched" });
    expect(nativeApiMocks.dispatchCommand).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "thread.turn.start",
        interactionMode: "debug",
      }),
    );
  });
});

describe("Kanban Cloud task", () => {
  beforeEach(() => {
    resetComposerDraftStore();
    nativeApiMocks.dispatchCommand.mockClear();
  });

  it("sends a Cloud task kept as Local to the chat, which creates its worktree", async () => {
    const projectId = ProjectId.makeUnsafe("project-kanban-cloud");
    const threadId = createKanbanDraftTask({
      projectId,
      prompt: "Fix the checkout flake",
      modelSelection: { provider: "aether", model: "claude-opus-5-5" },
      runtimeMode: "full-access",
      interactionMode: "default",
      // The dialog's Local/Worktree choice stays whatever it was before Cloud was picked.
      envMode: "local",
    });
    expect(useComposerDraftStore.getState().getDraftThread(threadId)?.envMode).toBe("local");

    // No local turn: the chat composer creates the worktree before the first send.
    const result = await dispatchKanbanDraftThread({
      threadId,
      projectId,
      thread: null,
      defaultProvider: "codex",
      assistantDeliveryMode: "buffered",
    });
    expect(result).toEqual({ kind: "open-thread", reason: "worktree-pending" });
    expect(nativeApiMocks.dispatchCommand).not.toHaveBeenCalled();
  });
});

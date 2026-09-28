import { describe, expect, it } from "vitest";

import {
  appendKanbanTaskTranscript,
  buildKanbanTaskPreview,
  resolveKanbanTaskWorkspace,
  truncateKanbanTaskPreview,
} from "./KanbanNewTaskDialog.logic";

describe("KanbanNewTaskDialog logic", () => {
  it("appends voice transcripts without preserving trailing whitespace", () => {
    expect(appendKanbanTaskTranscript("", "  ship it  ")).toBe("ship it");
    expect(appendKanbanTaskTranscript("Draft task  ", "  and test it  ")).toBe(
      "Draft task and test it",
    );
    expect(appendKanbanTaskTranscript("Draft task", "   ")).toBe("Draft task");
  });

  it("builds the same preview fallback order as the dialog", () => {
    expect(
      buildKanbanTaskPreview({
        trimmedPrompt: "Fix reconnect",
        firstImageName: "screen.png",
        assistantSelectionCount: 1,
      }),
    ).toBe("Fix reconnect");
    expect(
      buildKanbanTaskPreview({
        trimmedPrompt: "",
        firstImageName: "screen.png",
        assistantSelectionCount: 1,
      }),
    ).toBe("Image: screen.png");
    expect(
      buildKanbanTaskPreview({
        trimmedPrompt: "",
        firstImageName: null,
        assistantSelectionCount: 1,
      }),
    ).toBe("Referenced assistant selection");
    expect(
      buildKanbanTaskPreview({
        trimmedPrompt: "",
        firstImageName: null,
        assistantSelectionCount: 0,
      }),
    ).toBe("New task");
  });

  it("truncates long previews for toasts", () => {
    expect(truncateKanbanTaskPreview("short", 10)).toBe("short");
    expect(truncateKanbanTaskPreview("abcdefghijkl", 10)).toBe("abcdefghij…");
  });
});

describe("resolveKanbanTaskWorkspace", () => {
  const needsGit = "Aether needs a git repository with a GitHub remote.";

  it("refuses a cloud task where no worktree can exist, with the send's reason", () => {
    for (const where of [
      { isHomeChatContainer: true, isGitRepo: undefined },
      { isHomeChatContainer: false, isGitRepo: false },
    ]) {
      expect(resolveKanbanTaskWorkspace({ provider: "aether", ...where })).toEqual({
        feature: "non-git-workspace",
        unavailableReason: needsGit,
      });
    }
  });

  it("gates only the cloud row for a provider that runs on this machine", () => {
    expect(
      resolveKanbanTaskWorkspace({
        provider: "claudeAgent",
        isHomeChatContainer: false,
        isGitRepo: false,
      }),
    ).toEqual({ feature: "non-git-workspace", unavailableReason: null });
  });

  it("lets a cloud task run in a git repository, and while its status loads", () => {
    for (const isGitRepo of [true, undefined]) {
      expect(
        resolveKanbanTaskWorkspace({ provider: "aether", isHomeChatContainer: false, isGitRepo }),
      ).toEqual({ feature: null, unavailableReason: null });
    }
  });
});

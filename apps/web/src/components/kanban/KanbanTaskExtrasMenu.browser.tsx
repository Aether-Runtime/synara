import "../../index.css";

import { DEFAULT_SERVER_SETTINGS_VIEW, type ProviderKind } from "@synara/contracts";
import { page } from "vitest/browser";
import { expect, it, vi } from "vitest";
import { render } from "vitest-browser-react";

import { resolveCloudLocationRows } from "../BranchToolbar.logic";
import { KanbanTaskExtrasMenu } from "./KanbanTaskExtrasMenu";

const providerSettings = {
  ...DEFAULT_SERVER_SETTINGS_VIEW.providers,
  aether: { ...DEFAULT_SERVER_SETTINGS_VIEW.providers.aether, enabled: true },
};

// The same rows the composer's "Work in" menu offers a draft on `harness`.
function cloudRowsFor(harness: ProviderKind) {
  return resolveCloudLocationRows({
    harness,
    providerStatuses: [
      {
        provider: "aether",
        status: "ready",
        available: true,
        authStatus: "authenticated",
        checkedAt: "2026-09-28T10:00:00.000Z",
      },
    ],
    providerSettings,
    workspaceFeature: null,
  });
}

function props(overrides: Partial<Parameters<typeof KanbanTaskExtrasMenu>[0]> = {}) {
  return {
    interactionMode: "default" as const,
    onInteractionModeChange: vi.fn(),
    envMode: "local" as const,
    onEnvModeChange: vi.fn(),
    cloud: null,
    cloudRows: cloudRowsFor("claudeAgent"),
    canLeaveCloud: true,
    onCloudSelect: vi.fn(),
    ...overrides,
  };
}

it("moves a task on a harness the cloud runs to Cloud", async () => {
  const callbacks = props();
  const screen = await render(<KanbanTaskExtrasMenu {...callbacks} />);
  try {
    await page.getByRole("button", { name: "Task options" }).click();
    await expect
      .element(page.getByRole("menuitemradio", { name: "Local", exact: true }))
      .toHaveAttribute("aria-checked", "true");
    await page.getByRole("menuitemradio", { name: "Cloud (Aether)", exact: true }).click();
    expect(callbacks.onCloudSelect).toHaveBeenCalledExactlyOnceWith("aether");
    expect(callbacks.onEnvModeChange).not.toHaveBeenCalled();
  } finally {
    await screen.unmount();
  }
});

it("offers Cloud disabled with the reason for a harness the cloud does not run", async () => {
  const callbacks = props({ cloudRows: cloudRowsFor("cursor") });
  const screen = await render(<KanbanTaskExtrasMenu {...callbacks} />);
  try {
    await page.getByRole("button", { name: "Task options" }).click();
    const cloudRow = page.getByRole("menuitemradio", { name: /^Cloud \(Aether\)/u });
    await expect.element(cloudRow).toHaveTextContent("Cloud runs Claude and Codex");
    await expect.element(cloudRow).toHaveAttribute("aria-disabled", "true");
  } finally {
    await screen.unmount();
  }
});

it("in Cloud, checks the Cloud row and returns to this machine through Local or Worktree", async () => {
  const callbacks = props({ cloud: "aether", cloudRows: cloudRowsFor("aether") });
  const screen = await render(<KanbanTaskExtrasMenu {...callbacks} />);
  try {
    await page.getByRole("button", { name: "Task options" }).click();
    await expect
      .element(page.getByRole("menuitemradio", { name: "Cloud (Aether)", exact: true }))
      .toHaveAttribute("aria-checked", "true");
    await page.getByRole("menuitemradio", { name: "Worktree", exact: true }).click();
    expect(callbacks.onEnvModeChange).toHaveBeenCalledExactlyOnceWith("worktree");
    expect(callbacks.onCloudSelect).not.toHaveBeenCalled();
  } finally {
    await screen.unmount();
  }
});

it("offers no way back while the task's cloud model has no host", async () => {
  const callbacks = props({
    cloud: "aether",
    cloudRows: cloudRowsFor("aether"),
    canLeaveCloud: false,
  });
  const screen = await render(<KanbanTaskExtrasMenu {...callbacks} />);
  try {
    await page.getByRole("button", { name: "Task options" }).click();
    await expect
      .element(page.getByRole("menuitemradio", { name: "Cloud (Aether)", exact: true }))
      .toBeVisible();
    expect(page.getByRole("menuitemradio", { name: "Local", exact: true }).elements()).toHaveLength(
      0,
    );
    expect(
      page.getByRole("menuitemradio", { name: "Worktree", exact: true }).elements(),
    ).toHaveLength(0);
  } finally {
    await screen.unmount();
  }
});

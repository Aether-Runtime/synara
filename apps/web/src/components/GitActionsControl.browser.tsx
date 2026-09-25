// FILE: GitActionsControl.browser.tsx
// Purpose: The git actions on a thread whose provider owns its branch (Aether): commit, push
//          and pull are unavailable with the reason, and the PR action asks the agent for a
//          pull request, or opens the one the provider reported.
// Layer: Vitest browser tests

import "../index.css";

import { ThreadId, type NativeApi, type OrchestrationThreadPullRequest } from "@synara/contracts";
import { providerFeatureUnavailableMessage } from "@synara/shared/providerMetadata";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { afterEach, describe, expect, it, vi } from "vitest";
import { page, userEvent } from "vitest/browser";
import { cleanup, render } from "vitest-browser-react";

import { useStore } from "~/store";
import GitActionsControl from "./GitActionsControl";
import { OPEN_PULL_REQUEST_REQUEST } from "./GitActionsControl.logic";

const cwd = "/repo/.worktrees/aether";
const threadId = ThreadId.makeUnsafe("thread-aether-git");
const branch = "aether/acme-checkout-640050b7d9ed";
const reported: OrchestrationThreadPullRequest = {
  number: 18,
  title: "Add docs/demo.md with checkout edge cases",
  url: "https://github.com/Aether-Runtime/acme-checkout/pull/18",
  baseBranch: "main",
  headBranch: branch,
  state: "open",
};

const initialStore = useStore.getState();

function mount(lastKnownPr: OrchestrationThreadPullRequest | null) {
  const dispatchCommand = vi.fn().mockResolvedValue({ sequence: 1 });
  const openExternal = vi.fn().mockResolvedValue(undefined);
  vi.stubGlobal("nativeApi", {
    git: {
      listBranches: vi.fn().mockResolvedValue({
        isRepo: true,
        hasOriginRemote: true,
        branches: [{ name: branch, current: true, isDefault: false, worktreePath: cwd }],
      }),
      // The agent pushed more than the local branch holds, with changes it has not committed.
      status: vi.fn().mockResolvedValue({
        branch,
        hasWorkingTreeChanges: true,
        workingTree: { files: [], insertions: 1, deletions: 0 },
        hasUpstream: true,
        upstreamBranch: `origin/${branch}`,
        aheadCount: 0,
        behindCount: 1,
        pr: null,
      }),
      onActionProgress: () => () => undefined,
    },
    shell: { openExternal },
    orchestration: { dispatchCommand },
  } as unknown as NativeApi);
  useStore.setState({
    threadShellById: {
      [threadId]: {
        id: threadId,
        title: "Checkout edge cases",
        branch,
        worktreePath: cwd,
        modelSelection: { provider: "aether", model: "claude-opus-5-5" },
        runtimeMode: "full-access",
        lastKnownPr,
      },
    } as never,
  });
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return {
    dispatchCommand,
    openExternal,
    screen: render(
      <QueryClientProvider client={queryClient}>
        <GitActionsControl gitCwd={cwd} activeThreadId={threadId} />
      </QueryClientProvider>,
    ),
  };
}

describe("GitActionsControl on a thread whose agent owns the branch", () => {
  afterEach(async () => {
    await cleanup();
    useStore.setState(initialStore);
    vi.unstubAllGlobals();
  });

  it("offers no commit, push or pull, saying why, and asks the agent for a PR", async () => {
    const { dispatchCommand } = mount(null);

    await page.getByRole("button", { name: "Git action options" }).click();
    for (const label of ["Commit & push", "Push", "Pull", "Create Branch"]) {
      await expect
        .element(page.getByRole("menuitem", { name: label, exact: true }))
        .toHaveAttribute("data-disabled");
    }
    // No advice to pull: the agent keeps the branch in sync.
    expect(page.getByText("Behind upstream. Pull/rebase first.").query()).toBeNull();

    await page.getByRole("menuitem", { name: "Create PR" }).click();
    await expect.poll(() => dispatchCommand.mock.calls.length).toBe(1);
    // The request is on its way: asking again queues nothing.
    await page.getByRole("button", { name: "Git action options" }).click();
    await expect
      .element(page.getByRole("menuitem", { name: "Create PR", exact: true }))
      .toHaveAttribute("data-disabled");
    await userEvent.keyboard("{Escape}");
    // Its turn runs, then ends with no pull request: the agent can be asked again.
    useStore.setState({
      threadSessionById: { [threadId]: { provider: "aether", status: "running" } } as never,
    });
    // The turn runs for a while: at least one render sees it.
    await new Promise((resolve) => setTimeout(resolve, 50));
    useStore.setState({
      threadSessionById: { [threadId]: { provider: "aether", status: "ready" } } as never,
    });
    await page.getByRole("button", { name: "Git action options" }).click();
    await expect
      .element(page.getByRole("menuitem", { name: "Create PR", exact: true }))
      .not.toHaveAttribute("data-disabled");
    await userEvent.keyboard("{Escape}");
    expect(dispatchCommand).toHaveBeenCalledTimes(1);
    expect(dispatchCommand.mock.calls[0]![0]).toMatchObject({
      type: "thread.turn.start",
      threadId,
      message: { role: "user", text: OPEN_PULL_REQUEST_REQUEST, attachments: [] },
      modelSelection: { provider: "aether", model: "claude-opus-5-5" },
      interactionMode: "default",
      dispatchMode: "queue",
    });

    await page.getByRole("button", { name: "Git action options" }).click();
    await page.getByRole("menuitem", { name: "Pull", exact: true }).hover();
    await expect
      .element(page.getByText(providerFeatureUnavailableMessage("aether", "branch-git")))
      .toBeVisible();
  });

  it("opens the PR the provider reported", async () => {
    const { dispatchCommand, openExternal } = mount(reported);

    await page.getByRole("button", { name: "View PR" }).click();
    await expect.poll(() => openExternal.mock.calls.length).toBe(1);
    expect(openExternal).toHaveBeenCalledWith(reported.url);
    expect(dispatchCommand).not.toHaveBeenCalled();
  });
});

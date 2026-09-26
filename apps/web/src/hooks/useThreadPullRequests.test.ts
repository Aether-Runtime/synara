import type { OrchestrationThreadPullRequest } from "@synara/contracts";
import { describe, expect, it } from "vitest";

import {
  providerReportedPullRequest,
  resolveThreadPullRequestFallback,
} from "./useThreadPullRequests";

const staleOpenPullRequest: OrchestrationThreadPullRequest = {
  number: 841,
  title: "Previous branch pull request",
  url: "https://github.com/acme/synara/pull/841",
  baseBranch: "main",
  headBranch: "feat/previous-branch",
  state: "open",
  isDraft: false,
  mergeability: "mergeable",
  additions: 12,
  deletions: 4,
  changedFiles: 2,
};

describe("resolveThreadPullRequestFallback", () => {
  it("rejects a stale open PR for a dedicated worktree on another branch", () => {
    expect(
      resolveThreadPullRequestFallback({
        branch: "feat/current-branch",
        hasDedicatedWorktree: true,
        lastKnownPr: staleOpenPullRequest,
      }),
    ).toBeNull();
  });
});

const thread = (
  provider: "aether" | "codex",
  lastKnownPr: OrchestrationThreadPullRequest | null,
) => ({
  modelSelection: { provider, model: provider === "aether" ? "claude-opus-5-5" : "gpt-6-sol" },
  session: null,
  lastKnownPr,
});

describe("providerReportedPullRequest", () => {
  const reported: OrchestrationThreadPullRequest = {
    number: 18,
    title: "Add docs/demo.md with checkout edge cases",
    url: "https://github.com/Aether-Runtime/acme-checkout/pull/18",
    baseBranch: "main",
    headBranch: "aether/acme-checkout-640050b7d9ed",
    state: "open",
    checks: { status: "passed", passed: 9, failed: 0, total: 9 },
  };

  it("takes the PR a provider that owns the thread's branch reported, with nothing to look up", () => {
    expect(providerReportedPullRequest(thread("aether", reported))).toMatchObject({
      number: 18,
      url: reported.url,
      state: "open",
    });
    expect(providerReportedPullRequest(thread("aether", null))).toBeNull();
  });

  it("leaves every other provider's PR to git status and gh", () => {
    expect(providerReportedPullRequest(thread("codex", staleOpenPullRequest))).toBeUndefined();
  });
});

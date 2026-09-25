// FILE: aetherPullRequest.test.ts
// Purpose: The thread pull request Synara reports from an Aether task's payload.
// Layer: Server provider tests (Aether).
import { Schema } from "effect";
import { describe, expect, it } from "vitest";

import { AetherTask } from "./aetherApiSchemas.ts";
import { aetherThreadPullRequest } from "./aetherPullRequest.ts";
import { pullRequestBody, reviewSummaryBody, taskBody } from "./testing/aetherApiFixtures.ts";

const task = (extra: Record<string, unknown>) =>
  Schema.decodeUnknownSync(AetherTask)(
    taskBody("awaiting_input", { head_branch: "aether/acme-checkout-640050b7d9ed", ...extra }),
  );

const without = (body: Record<string, unknown>, key: string) =>
  Object.fromEntries(Object.entries(body).filter(([name]) => name !== key));

describe("aetherThreadPullRequest", () => {
  it("reports the task's pull request with its CI and Aether Review's verdict", () => {
    expect(
      aetherThreadPullRequest(
        task({ primary_pull_request: pullRequestBody(), pr_review_summary: reviewSummaryBody() }),
      ),
    ).toEqual({
      _tag: "Reported",
      pullRequest: {
        number: 18,
        title: "Add docs/demo.md with checkout edge cases",
        url: "https://github.com/Aether-Runtime/acme-checkout/pull/18",
        baseBranch: "main",
        headBranch: "aether/acme-checkout-640050b7d9ed",
        state: "open",
        additions: 3,
        deletions: 0,
        checks: { status: "passed", passed: 9, failed: 0, total: 9 },
        review: { status: "completed", verdict: "approve" },
      },
    });
  });

  it("reports no pull request when the task has none", () => {
    expect(aetherThreadPullRequest(task({}))).toEqual({ _tag: "Reported", pullRequest: null });
  });

  it("keeps a CI status or review it does not know as unknown, and leaves out what is absent", () => {
    const reported = aetherThreadPullRequest(
      task({
        primary_pull_request: pullRequestBody({ ci_status: "skipped" }),
        pr_review_summary: reviewSummaryBody({ run_status: "queued", verdict: "BLOCK" }),
      }),
    );
    expect(reported).toMatchObject({
      pullRequest: {
        checks: { status: "unknown" },
        review: { status: "unknown", verdict: "unknown" },
      },
    });

    const bare = aetherThreadPullRequest(
      task({
        primary_pull_request: without(pullRequestBody(), "ci_status"),
        pr_review_summary: without(reviewSummaryBody({ run_status: "reviewing" }), "verdict"),
      }),
    );
    expect(bare._tag === "Reported" && bare.pullRequest).not.toHaveProperty("checks");
    expect(bare).toMatchObject({ pullRequest: { review: { status: "reviewing", verdict: null } } });
  });

  it("ignores a review of another pull request", () => {
    const reported = aetherThreadPullRequest(
      task({
        primary_pull_request: pullRequestBody(),
        pr_review_summary: reviewSummaryBody({
          pull_request_id: "0b0b0b0b-0b0b-4b0b-8b0b-0b0b0b0b0b0b",
        }),
      }),
    );
    expect(reported._tag === "Reported" && reported.pullRequest).not.toHaveProperty("review");
  });

  it("cannot show a pull request, review or base branch it cannot read, and the task still decodes", () => {
    const unreadable = [
      { primary_pull_request: pullRequestBody({ additions: null }) },
      { primary_pull_request: without(pullRequestBody(), "checks_total") },
      { primary_pull_request: pullRequestBody({ pr_number: "18" }) },
      {
        primary_pull_request: pullRequestBody(),
        pr_review_summary: reviewSummaryBody({ run_status: 5 }),
      },
      { primary_pull_request: pullRequestBody(), base_branch: "" },
    ];
    for (const extra of unreadable) {
      const report = aetherThreadPullRequest(task(extra));
      expect(report._tag).toBe("Unreadable");
      expect(report._tag === "Unreadable" && report.detail).toContain(
        "Aether sent pull request data Synara cannot read",
      );
    }
    // Without a pull request, a malformed review or base branch is nothing to show.
    expect(
      aetherThreadPullRequest(task({ pr_review_summary: { verdict: 1 }, base_branch: 7 })),
    ).toEqual({ _tag: "Reported", pullRequest: null });
  });

  it("cannot show a pull request in a state it does not know, or with none", () => {
    expect(
      aetherThreadPullRequest(task({ primary_pull_request: pullRequestBody({ state: "draft" }) })),
    ).toEqual({
      _tag: "Unreadable",
      detail: 'Aether reported pull request #18 in a state Synara does not know ("draft").',
    });
    expect(
      aetherThreadPullRequest(task({ primary_pull_request: without(pullRequestBody(), "state") })),
    ).toEqual({
      _tag: "Unreadable",
      detail: "Aether reported pull request #18 without its state.",
    });
  });
});

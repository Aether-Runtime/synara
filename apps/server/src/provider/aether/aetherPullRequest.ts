// FILE: aetherPullRequest.ts
// Purpose: The thread's pull request, read from an Aether task's payload: the PR the task's
// branch opened, its CI, and Aether Review's latest run on it. Synara shows this instead of
// asking `gh`, so it needs no local GitHub login.
// Layer: Server provider boundary (Aether).

import type {
  OrchestrationThreadPullRequest,
  OrchestrationThreadPullRequestChecks,
  OrchestrationThreadPullRequestReview,
} from "@synara/contracts";

import type {
  AetherPullRequestReview,
  AetherTask,
  AetherTaskPullRequest,
  AetherUnknownValue,
} from "./aetherApiSchemas.ts";

export type AetherPullRequestReport =
  /** The task's pull request, or null while it has none. */
  | { readonly _tag: "Reported"; readonly pullRequest: OrchestrationThreadPullRequest | null }
  /** A pull request Synara cannot show: every PR surface needs its state. */
  | { readonly _tag: "Unreadable"; readonly detail: string };

const isUnknown = (value: unknown): value is AetherUnknownValue =>
  typeof value === "object" && value !== null;

function checksOf(pr: AetherTaskPullRequest): OrchestrationThreadPullRequestChecks | undefined {
  if (pr.ci_status === undefined) return undefined;
  return {
    status: isUnknown(pr.ci_status) ? "unknown" : pr.ci_status,
    passed: pr.checks_passed,
    failed: pr.checks_failed,
    total: pr.checks_total,
  };
}

const REVIEW_STATUS = {
  reviewing: "reviewing",
  awaiting_input: "awaiting-input",
  completed: "completed",
  failed: "failed",
  cancelled: "cancelled",
  superseded: "superseded",
  archived: "archived",
} as const;

const REVIEW_VERDICT = {
  APPROVE: "approve",
  REQUEST_CHANGES: "request-changes",
  COMMENT: "comment",
  DISMISSED: "dismissed",
  SUPERSEDED: "superseded",
} as const;

function reviewOf(review: AetherPullRequestReview): OrchestrationThreadPullRequestReview {
  const { run_status: status, verdict } = review;
  return {
    status: isUnknown(status) ? "unknown" : REVIEW_STATUS[status],
    verdict:
      verdict === undefined ? null : isUnknown(verdict) ? "unknown" : REVIEW_VERDICT[verdict],
  };
}

/** The thread's pull request as the task reports it. */
export function aetherThreadPullRequest(task: AetherTask): AetherPullRequestReport {
  const pr = task.primary_pull_request;
  if (pr === null) return { _tag: "Reported", pullRequest: null };
  const name = `Aether reported pull request #${pr.pr_number}`;
  if (pr.state === undefined) return { _tag: "Unreadable", detail: `${name} without its state.` };
  if (isUnknown(pr.state)) {
    return {
      _tag: "Unreadable",
      detail: `${name} in a state Synara does not know ("${pr.state.raw}").`,
    };
  }
  // The task's pull request is the one its branch opened.
  const headBranch = pr.branch_name ?? task.head_branch;
  if (headBranch === undefined) {
    return { _tag: "Unreadable", detail: `${name} without the branch it comes from.` };
  }
  const title = pr.title.trim();
  if (title.length === 0) return { _tag: "Unreadable", detail: `${name} with a blank title.` };
  const checks = checksOf(pr);
  const review = task.pr_review_summary;
  return {
    _tag: "Reported",
    pullRequest: {
      number: pr.pr_number,
      title,
      url: pr.html_url,
      baseBranch: task.base_branch,
      headBranch,
      state: pr.state,
      additions: pr.additions,
      deletions: pr.deletions,
      ...(checks === undefined ? {} : { checks }),
      ...(review !== null && review.pull_request_id === pr.pull_request_id
        ? { review: reviewOf(review) }
        : {}),
    },
  };
}

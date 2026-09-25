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

import { Cause, Exit, Result, Schema } from "effect";

import {
  AetherPullRequestReview,
  AetherTaskPullRequest,
  type AetherTask,
  type AetherUnknownValue,
} from "./aetherApiSchemas.ts";

const NonEmptyString = Schema.String.check(Schema.isMinLength(1));

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

/** Decodes one display-only part of the task; what it cannot read, it says. */
function decodePart<S extends Schema.Top & { readonly DecodingServices: never }>(
  schema: S,
  field: string,
  raw: unknown,
): Result.Result<S["Type"], string> {
  const decoded = Schema.decodeUnknownExit(schema)(raw);
  return Exit.isSuccess(decoded)
    ? Result.succeed(decoded.value)
    : Result.fail(
        `Aether sent pull request data Synara cannot read (${field}): ${Cause.pretty(decoded.cause)}`,
      );
}

/**
 * The thread's pull request as the task reports it. The task's display-only parts are decoded
 * here, not with the task: one Synara cannot read is `Unreadable`, and the task goes on.
 */
export function aetherThreadPullRequest(task: AetherTask): AetherPullRequestReport {
  const decodedPr = decodePart(
    Schema.NullOr(AetherTaskPullRequest),
    "primary_pull_request",
    task.primary_pull_request,
  );
  if (Result.isFailure(decodedPr)) return { _tag: "Unreadable", detail: decodedPr.failure };
  const pr = decodedPr.success;
  if (pr === null) return { _tag: "Reported", pullRequest: null };
  const baseBranch = decodePart(NonEmptyString, "base_branch", task.base_branch);
  if (Result.isFailure(baseBranch)) return { _tag: "Unreadable", detail: baseBranch.failure };
  const decodedReview = decodePart(
    Schema.NullOr(AetherPullRequestReview),
    "pr_review_summary",
    task.pr_review_summary,
  );
  if (Result.isFailure(decodedReview)) {
    return { _tag: "Unreadable", detail: decodedReview.failure };
  }
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
  const review = decodedReview.success;
  return {
    _tag: "Reported",
    pullRequest: {
      number: pr.pr_number,
      title,
      url: pr.html_url,
      baseBranch: baseBranch.success,
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

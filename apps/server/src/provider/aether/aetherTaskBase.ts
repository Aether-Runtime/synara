// FILE: aetherTaskBase.ts
// Purpose: What a new Aether task starts from: the thread's own worktree, the Aether project of
// its `origin` remote, and the branch on origin whose tip the worktree's HEAD is.
// Layer: Server provider boundary (Aether).
//
// The VM clones the base branch from origin, so the worktree must be clean and its HEAD must be
// that branch's tip on origin: local-only work would never reach the VM, and a newer tip would
// give the VM another merge-base, which the mirror refuses. Synara's worktree creation keeps only
// the commit (`associatedWorktreeRef`), not the branch it came from, so the base branch is the
// project's default branch when its tip is HEAD (Aether's own task branches often sit at a past
// tip of it), and otherwise the one branch on origin at HEAD.

import { Effect } from "effect";

import { GitCore } from "../../git/Services/GitCore.ts";
import { ProviderAdapterRequestError, ProviderAdapterValidationError } from "../Errors.ts";
import type { AetherApiClientShape } from "../Services/AetherApiClient.ts";
import type { AetherApiError } from "./aetherApiErrors.ts";
import type { AetherProjectId } from "./aetherApiSchemas.ts";

export interface AetherTaskBase {
  readonly projectId: AetherProjectId;
  readonly baseBranch: string;
  readonly baseCommit: string;
}

/** `host/owner/repo`, lowercased, from an ssh, scp-like or http(s) remote URL. */
export function repositorySlug(url: string): string | undefined {
  const match =
    /^(?:[a-z][a-z0-9+.-]*:\/\/(?:[^@/]+@)?([^/:]+)(?::\d+)?\/|[^@/:\s]+@([^:/\s]+):)([^/\s]+)\/([^/\s]+?)(?:\.git)?\/?$/iu.exec(
      url.trim(),
    );
  return match === null
    ? undefined
    : `${match[1] ?? match[2]}/${match[3]}/${match[4]}`.toLowerCase();
}

const invalid = (operation: string, issue: string) =>
  new ProviderAdapterValidationError({ provider: "aether", operation, issue });

const git = (
  operation: string,
  cwd: string,
  args: ReadonlyArray<string>,
  allowNonZeroExit = false,
) =>
  Effect.gen(function* () {
    const gitCore = yield* GitCore;
    const result = yield* gitCore
      .execute({ operation: `AetherAdapter.${operation}`, cwd, args, allowNonZeroExit })
      .pipe(
        Effect.mapError(
          (error) =>
            new ProviderAdapterRequestError({
              provider: "aether",
              method: `git ${args[0]}`,
              detail: error.message,
              cause: error,
            }),
        ),
      );
    return result.stdout;
  });

/** Refuses a checkout that is not a linked worktree: Aether threads never run in "Local". */
export const requireAetherWorktree = (cwd: string) =>
  Effect.gen(function* () {
    const [gitDir, commonDir] = (yield* git("worktree", cwd, [
      "rev-parse",
      "--path-format=absolute",
      "--git-dir",
      "--git-common-dir",
    ]))
      .trim()
      .split(/\r?\n/u);
    if (gitDir === commonDir) {
      return yield* invalid(
        "startSession",
        "Aether threads run in their own worktree, and this thread uses the project's main checkout (Local). Start the thread in a new worktree.",
      );
    }
  });

/** Resolves and checks the base of the task this worktree's first turn creates. */
export const resolveAetherTaskBase = (input: {
  readonly cwd: string;
  readonly client: Pick<AetherApiClientShape, "listProjects">;
}): Effect.Effect<
  AetherTaskBase,
  ProviderAdapterValidationError | ProviderAdapterRequestError | AetherApiError,
  GitCore
> =>
  Effect.gen(function* () {
    const { cwd } = input;
    const dirty = (yield* git("status", cwd, [
      "status",
      "--porcelain=v1",
      "-z",
      "--untracked-files=all",
    ]))
      .split("\0")
      .filter((entry) => entry.length > 3)
      .map((entry) => entry.slice(3));
    if (dirty.length > 0) {
      return yield* invalid(
        "sendTurn",
        `The worktree has uncommitted changes (${dirty.slice(0, 5).join(", ")}${dirty.length > 5 ? ", …" : ""}). The Aether VM clones from origin and would not see them: commit and push them, or discard them.`,
      );
    }
    const head = (yield* git("head", cwd, ["rev-parse", "--verify", "HEAD^{commit}"])).trim();
    // As configured: `insteadOf` rewrites change the transport, not the repository.
    const remote = (yield* git(
      "remote",
      cwd,
      ["config", "--get", "remote.origin.url"],
      true,
    )).trim();
    const slug = repositorySlug(remote);
    if (slug === undefined) {
      // Never the URL itself: it can carry credentials.
      return yield* invalid(
        "sendTurn",
        remote === ""
          ? "The worktree's repository has no origin remote, and the Aether VM clones from origin."
          : "Synara cannot read host/owner/repo from the worktree's origin remote.",
      );
    }
    const projects = (yield* input.client.listProjects()).filter(
      (project) => project.repo_url !== undefined && repositorySlug(project.repo_url) === slug,
    );
    if (projects.length !== 1) {
      return yield* invalid(
        "sendTurn",
        projects.length === 0
          ? `No Aether project is connected to ${slug}. Add the repository as a project in Aether (Synara does not create projects), then send again.`
          : `Several Aether projects are connected to ${slug} (${projects.map((project) => project.id).join(", ")}). Keep one, then send again.`,
      );
    }
    const project = projects[0]!;
    const branches = (yield* git("lsRemote", cwd, ["ls-remote", "--heads", "origin"]))
      .split(/\r?\n/u)
      .flatMap((line) => {
        const [commit, ref] = line.split("\t");
        return commit === head && ref?.startsWith("refs/heads/") === true
          ? [ref.slice("refs/heads/".length)]
          : [];
      });
    const shortHead = head.slice(0, 12);
    if (branches.length === 0) {
      return yield* invalid(
        "sendTurn",
        `The worktree's HEAD (${shortHead}) is not the tip of any branch on origin, and the Aether VM clones from origin. Push it, or pull if origin moved on, then send again.`,
      );
    }
    const baseBranch =
      project.default_branch !== undefined && branches.includes(project.default_branch)
        ? project.default_branch
        : branches.length === 1
          ? branches[0]!
          : undefined;
    if (baseBranch === undefined) {
      return yield* invalid(
        "sendTurn",
        `Several branches on origin are at the worktree's HEAD (${shortHead}): ${branches.join(", ")}, and none is the project's default branch. Synara cannot tell which one this worktree was made from, so it cannot pick the task's base branch.`,
      );
    }
    return { projectId: project.id, baseBranch, baseCommit: head };
  });

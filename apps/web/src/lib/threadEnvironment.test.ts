import { describe, expect, it } from "vitest";
import {
  resolveCloudProvider,
  resolveDiffEnvironmentState,
  resolveForkThreadEnvironment,
  resolveProviderEnvMode,
  resolveThreadEnvironmentPresentation,
} from "./threadEnvironment";

describe("threadEnvironment", () => {
  it("overlays a new worktree for a provider that owns its worktree", () => {
    expect(resolveProviderEnvMode("local", "aether")).toBe("worktree");
    expect(resolveProviderEnvMode("local", "codex")).toBe("local");
    expect(resolveProviderEnvMode("worktree", "codex")).toBe("worktree");
  });

  it("presents a cloud thread as Cloud over its worktree overlay", () => {
    expect(
      resolveThreadEnvironmentPresentation({ envMode: "worktree", cloud: "aether" }),
    ).toMatchObject({
      mode: "worktree",
      cloud: "aether",
      shortLabel: "Cloud",
      cloudBadgeLabel: "Runs in the cloud (Aether)",
    });
    expect(resolveThreadEnvironmentPresentation({ envMode: "worktree" })).toMatchObject({
      cloud: null,
      shortLabel: "Worktree",
      cloudBadgeLabel: null,
    });
    expect(resolveThreadEnvironmentPresentation({ envMode: "local" })).toMatchObject({
      cloud: null,
      shortLabel: "Local",
      cloudBadgeLabel: null,
    });
  });

  it("names a provider as the thread's cloud only when it is a cloud service", () => {
    expect(resolveCloudProvider("aether")).toBe("aether");
    expect(resolveCloudProvider("codex")).toBeNull();
  });

  it("keeps a worktree fork into local on the same worktree", () => {
    expect(
      resolveForkThreadEnvironment({
        target: "local",
        activeRootBranch: "main",
        sourceThread: {
          branch: "feature/worktree-branch",
          envMode: "worktree",
          worktreePath: "/repo/.worktrees/feature-worktree-branch",
        },
      }),
    ).toEqual({
      target: "local",
      envMode: "worktree",
      branch: "feature/worktree-branch",
      worktreePath: "/repo/.worktrees/feature-worktree-branch",
      associatedWorktreePath: "/repo/.worktrees/feature-worktree-branch",
      associatedWorktreeBranch: "feature/worktree-branch",
      associatedWorktreeRef: "feature/worktree-branch",
    });
  });

  it("keeps a local fork into local on the root checkout", () => {
    expect(
      resolveForkThreadEnvironment({
        target: "local",
        activeRootBranch: "main",
        sourceThread: {
          branch: "feature/local-branch",
          envMode: "local",
          worktreePath: null,
        },
      }),
    ).toEqual({
      target: "local",
      envMode: "local",
      branch: "feature/local-branch",
      worktreePath: null,
      associatedWorktreePath: null,
      associatedWorktreeBranch: null,
      associatedWorktreeRef: null,
    });
  });

  it("plans a new worktree fork without reusing the source path", () => {
    expect(
      resolveForkThreadEnvironment({
        target: "worktree",
        activeRootBranch: "main",
        sourceThread: {
          branch: "feature/source-branch",
          envMode: "worktree",
          worktreePath: "/repo/.worktrees/source-branch",
        },
      }),
    ).toEqual({
      target: "worktree",
      envMode: "worktree",
      branch: "feature/source-branch",
      worktreePath: null,
      associatedWorktreePath: null,
      associatedWorktreeBranch: "feature/source-branch",
      associatedWorktreeRef: "feature/source-branch",
    });
  });

  it("marks diff state as pending when a worktree chat has no materialized path yet", () => {
    expect(
      resolveDiffEnvironmentState({
        projectCwd: "/repo",
        envMode: "worktree",
        worktreePath: null,
      }),
    ).toEqual({
      pending: true,
      cwd: null,
      disabledReason:
        "Diff and summary will be available once the worktree is ready for this chat.",
    });
  });

  it("resolves diff state to the worktree cwd once the path exists", () => {
    expect(
      resolveDiffEnvironmentState({
        projectCwd: "/repo",
        envMode: "worktree",
        worktreePath: "/repo/.worktrees/feature-x",
      }),
    ).toEqual({
      pending: false,
      cwd: "/repo/.worktrees/feature-x",
      disabledReason: null,
    });
  });
});

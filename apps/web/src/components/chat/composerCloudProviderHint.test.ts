import { describe, expect, it } from "vitest";

import { cloudProviderHintCopy, shouldShowCloudProviderHint } from "./composerCloudProviderHint";

describe("shouldShowCloudProviderHint", () => {
  it("shows only for a cloud provider on a new chat until dismissed", () => {
    const base = { dismissed: false, threadHasMessages: false } as const;
    expect(shouldShowCloudProviderHint({ ...base, provider: "aether" })).toBe(true);
    expect(shouldShowCloudProviderHint({ ...base, provider: "codex" })).toBe(false);
    expect(shouldShowCloudProviderHint({ ...base, provider: "claudeAgent" })).toBe(false);
    expect(shouldShowCloudProviderHint({ ...base, provider: "aether", dismissed: true })).toBe(
      false,
    );
    expect(
      shouldShowCloudProviderHint({ ...base, provider: "aether", threadHasMessages: true }),
    ).toBe(false);
  });

  it("says where the agent runs, what the repo needs and where to get a key", () => {
    expect(cloudProviderHintCopy("aether")).toEqual({
      message:
        "Aether runs this chat in the cloud. The repo must be on GitHub and imported as a project in Aether.",
      apiKeyHref: "https://runaether.dev",
    });
  });
});

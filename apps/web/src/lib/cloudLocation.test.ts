import {
  AETHER_CATALOG,
  DEFAULT_MODEL_BY_PROVIDER,
  MODEL_OPTIONS_BY_PROVIDER,
} from "@synara/contracts";
import { describe, expect, it } from "vitest";

import { mapModelAcrossLocation } from "./cloudLocation";

const cloudClaude = {
  provider: "aether" as const,
  models: AETHER_CATALOG.agents["claude-code"].models,
  defaultModel: AETHER_CATALOG.agents["claude-code"].defaultModel,
  options: undefined,
};
const cloudCodex = {
  provider: "aether" as const,
  models: AETHER_CATALOG.agents.codex.models,
  defaultModel: AETHER_CATALOG.agents.codex.defaultModel,
  options: undefined,
};
const localClaude = {
  provider: "claudeAgent" as const,
  models: MODEL_OPTIONS_BY_PROVIDER.claudeAgent,
  defaultModel: DEFAULT_MODEL_BY_PROVIDER.claudeAgent,
  options: undefined,
};
const localCodex = {
  provider: "codex" as const,
  models: MODEL_OPTIONS_BY_PROVIDER.codex,
  defaultModel: DEFAULT_MODEL_BY_PROVIDER.codex,
  options: undefined,
};

describe("mapModelAcrossLocation", () => {
  it("keeps a slug the target offers", () => {
    expect(
      mapModelAcrossLocation({ from: { model: "claude-opus-5-5", effort: null }, to: cloudClaude }),
    ).toEqual({ model: "claude-opus-5-5", modelOptions: undefined });
    expect(
      mapModelAcrossLocation({ from: { model: "gpt-5.4", effort: null }, to: localCodex }).model,
    ).toBe("gpt-5.4");
  });

  it("falls to the target's default model when the slug has no twin", () => {
    expect(
      mapModelAcrossLocation({ from: { model: "claude-opus-4-8", effort: null }, to: cloudClaude })
        .model,
    ).toBe(AETHER_CATALOG.agents["claude-code"].defaultModel);
    expect(
      mapModelAcrossLocation({ from: { model: "gpt-5.6-sol", effort: null }, to: localCodex })
        .model,
    ).toBe(DEFAULT_MODEL_BY_PROVIDER.codex);
  });

  it("resolves a retired slug through the target's alias table", () => {
    expect(
      mapModelAcrossLocation({ from: { model: "claude-fable-5", effort: null }, to: cloudClaude })
        .model,
    ).toBe("claude-fable-5-1");
  });

  it("carries the effort when the target ladder has it", () => {
    expect(
      mapModelAcrossLocation({ from: { model: "gpt-5.4", effort: "high" }, to: cloudCodex })
        .modelOptions,
    ).toEqual({ reasoningEffort: "high" });
    expect(
      mapModelAcrossLocation({
        from: { model: "claude-opus-5-5", effort: "xhigh" },
        to: localClaude,
      }).modelOptions,
    ).toEqual({ effort: "xhigh" });
  });

  it("leaves the target's default effort when its ladder lacks the level", () => {
    expect(
      mapModelAcrossLocation({
        from: { model: "claude-opus-5-5", effort: "ultrathink" },
        to: localClaude,
      }).modelOptions,
    ).toBeUndefined();
    // Haiku takes no reasoning effort on Aether.
    expect(
      mapModelAcrossLocation({
        from: { model: "claude-haiku-4-5", effort: "high" },
        to: cloudClaude,
      }).modelOptions,
    ).toBeUndefined();
  });

  it("applies a carried effort on top of the target's other options", () => {
    expect(
      mapModelAcrossLocation({
        from: { model: "claude-opus-5-5", effort: "max" },
        to: { ...localClaude, options: { thinking: false, effort: "low" } },
      }).modelOptions,
    ).toEqual({ thinking: false, effort: "max" });
  });
});

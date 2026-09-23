import { describe, expect, it } from "vitest";

import { AETHER_AGENT_FAMILIES, AETHER_CATALOG, resolveAetherAgentFamily } from "./aetherCatalog";
import {
  DEFAULT_MODEL_BY_PROVIDER,
  MODEL_CAPABILITIES_INDEX,
  MODEL_OPTIONS_BY_PROVIDER,
  MODEL_SLUG_ALIASES_BY_PROVIDER,
  PROVIDER_DISPLAY_NAMES,
} from "./model";

const slugsOf = (family: (typeof AETHER_AGENT_FAMILIES)[number]): string[] =>
  AETHER_CATALOG.agents[family].models.map((model) => model.slug);

describe("Aether catalog", () => {
  it("keeps model slugs disjoint between agent families", () => {
    // The slug alone selects the Aether agent family. If a slug ever appears in both
    // families, AetherModelSelection needs an explicit family field instead.
    const codex = new Set(slugsOf("codex"));
    const overlap = slugsOf("claude-code").filter((slug) => codex.has(slug));
    expect(overlap).toEqual([]);
  });

  it("has no duplicate slugs inside a family", () => {
    for (const family of AETHER_AGENT_FAMILIES) {
      const slugs = slugsOf(family);
      expect(new Set(slugs).size).toBe(slugs.length);
    }
  });

  it("references only its own models from defaults and effort groups", () => {
    for (const family of AETHER_AGENT_FAMILIES) {
      const slugs = new Set(slugsOf(family));
      expect(slugs.has(AETHER_CATALOG.agents[family].defaultModel)).toBe(true);
      const effort = AETHER_CATALOG.reasoningEffort[family];
      expect(effort.options).toContain(effort.default);
      for (const group of effort.modelOptions) {
        for (const model of group.models) {
          expect(slugs.has(model)).toBe(true);
        }
        for (const option of group.options) {
          expect(effort.options).toContain(option);
        }
      }
    }
  });

  it("resolves the agent family from the slug", () => {
    expect(resolveAetherAgentFamily("gpt-6-astra")).toBe("codex");
    expect(resolveAetherAgentFamily("claude-opus-5-5")).toBe("claude-code");
    expect(resolveAetherAgentFamily("claude-fable-5")).toBeNull();
  });
});

describe("Aether model tables", () => {
  it("lists every catalog model for the picker", () => {
    expect(MODEL_OPTIONS_BY_PROVIDER.aether.map((model) => model.slug)).toEqual([
      ...slugsOf("codex"),
      ...slugsOf("claude-code"),
    ]);
    expect(PROVIDER_DISPLAY_NAMES.aether).toBe("Aether");
  });

  it("defaults to Claude Opus 5.5", () => {
    expect(DEFAULT_MODEL_BY_PROVIDER.aether).toBe("claude-opus-5-5");
    expect(resolveAetherAgentFamily(DEFAULT_MODEL_BY_PROVIDER.aether)).toBe("claude-code");
  });

  it("points every alias at a catalog slug", () => {
    const slugs = new Set(MODEL_OPTIONS_BY_PROVIDER.aether.map((model) => model.slug));
    for (const target of Object.values(MODEL_SLUG_ALIASES_BY_PROVIDER.aether)) {
      expect(slugs.has(target)).toBe(true);
    }
    // Aether rejects the retired Fable 5 slug; saved selections migrate to 5.1.
    expect(MODEL_SLUG_ALIASES_BY_PROVIDER.aether["claude-fable-5"]).toBe("claude-fable-5-1");
  });

  it("derives reasoning-effort options from the catalog model groups", () => {
    const efforts = (slug: string) =>
      MODEL_CAPABILITIES_INDEX.aether[slug]?.reasoningEffortLevels.map((level) => level.value);
    const defaultEffort = (slug: string) =>
      MODEL_CAPABILITIES_INDEX.aether[slug]?.reasoningEffortLevels.find((level) => level.isDefault)
        ?.value;

    expect(efforts("gpt-6-astra")).toEqual(["low", "medium", "high", "xhigh", "max", "ultra"]);
    expect(efforts("gpt-6-luna")).toEqual(["low", "medium", "high", "xhigh", "max"]);
    expect(efforts("gpt-5.4-mini")).toEqual(["low", "medium", "high", "xhigh"]);
    expect(defaultEffort("gpt-6-astra")).toBe("xhigh");

    expect(efforts("claude-opus-5-5")).toEqual([
      "low",
      "medium",
      "high",
      "xhigh",
      "max",
      "ultrathink",
      "ultracode",
    ]);
    expect(defaultEffort("claude-opus-5-5")).toBe("xhigh");
    // Sonnet 4.6 does not offer the family default, so none is marked.
    expect(efforts("claude-sonnet-4-6")).toEqual(["low", "medium", "high", "ultrathink"]);
    expect(defaultEffort("claude-sonnet-4-6")).toBeUndefined();
    // Haiku 4.5 is outside every effort group.
    expect(efforts("claude-haiku-4-5")).toEqual([]);

    for (const model of MODEL_OPTIONS_BY_PROVIDER.aether) {
      expect(model.capabilities.promptInjectedEffortLevels).toEqual([]);
      expect(model.capabilities.supportsFastMode).toBe(false);
    }
  });
});

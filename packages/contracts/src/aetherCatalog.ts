// FILE: aetherCatalog.ts
// Purpose: Vendored Aether model catalog for the `codex` and `claude-code` agent families.
//
// Source: Aether `packages/domain-types/src/generated/platform-catalog.ts` at Aether main
// `9f0b54b549`, 2026-09-23 (agent and reasoning-effort sections for `codex` and
// `claude-code` only; `runtimeModel` equals `slug` for every entry and is omitted).
//
// Aether exposes no catalog endpoint yet, so this copy is resynced by hand. Aether rejects
// retired slugs, so resync before a rename reaches users and keep an alias for the old slug
// in `MODEL_SLUG_ALIASES_BY_PROVIDER.aether`.

export type AetherAgentFamily = "codex" | "claude-code";

export interface AetherCatalogModel {
  readonly slug: string;
  readonly name: string;
}

export interface AetherCatalogAgent {
  readonly label: string;
  readonly defaultModel: string;
  readonly models: readonly AetherCatalogModel[];
}

export interface AetherCatalogReasoningEffort {
  readonly default: string;
  readonly options: readonly string[];
  readonly selectableOptions: readonly string[];
  /** Per-model-group options. A model outside every group takes no reasoning effort. */
  readonly modelOptions: ReadonlyArray<{
    readonly models: readonly string[];
    readonly options: readonly string[];
  }>;
}

export const AETHER_CATALOG = {
  agents: {
    codex: {
      label: "Codex",
      defaultModel: "gpt-5.6-sol",
      models: [
        { slug: "gpt-6-astra", name: "GPT-6 Astra" },
        { slug: "gpt-6-sol", name: "GPT-6 Sol" },
        { slug: "gpt-6-luna", name: "GPT-6 Luna" },
        { slug: "gpt-5.6-sol", name: "GPT-5.6 Sol" },
        { slug: "gpt-5.6-terra", name: "GPT-5.6 Terra" },
        { slug: "gpt-5.6-luna", name: "GPT-5.6 Luna" },
        { slug: "gpt-5.5", name: "GPT-5.5" },
        { slug: "gpt-5.4", name: "GPT-5.4" },
        { slug: "gpt-5.4-mini", name: "GPT-5.4 Mini" },
        { slug: "gpt-5.3-codex-spark", name: "GPT-5.3 Codex Spark" },
      ],
    },
    "claude-code": {
      label: "Claude Code",
      defaultModel: "claude-opus-5",
      models: [
        { slug: "claude-fable-5-1", name: "Claude Fable 5.1" },
        { slug: "claude-sonnet-5", name: "Claude Sonnet 5" },
        { slug: "claude-opus-5-5", name: "Claude Opus 5.5" },
        { slug: "claude-opus-5", name: "Claude Opus 5" },
        { slug: "claude-sonnet-4-6", name: "Claude Sonnet 4.6" },
        { slug: "claude-haiku-4-5", name: "Claude Haiku 4.5" },
      ],
    },
  },
  reasoningEffort: {
    codex: {
      default: "xhigh",
      options: ["ultra", "max", "xhigh", "high", "medium", "low"],
      selectableOptions: ["ultra", "max", "xhigh", "high", "medium", "low"],
      modelOptions: [
        {
          models: ["gpt-6-astra", "gpt-6-sol", "gpt-5.6-sol", "gpt-5.6-terra"],
          options: ["ultra", "max", "xhigh", "high", "medium", "low"],
        },
        {
          models: ["gpt-6-luna", "gpt-5.6-luna"],
          options: ["max", "xhigh", "high", "medium", "low"],
        },
        {
          models: ["gpt-5.5", "gpt-5.4", "gpt-5.4-mini", "gpt-5.3-codex-spark"],
          options: ["xhigh", "high", "medium", "low"],
        },
      ],
    },
    "claude-code": {
      default: "xhigh",
      options: ["low", "medium", "high", "xhigh", "max", "ultrathink", "ultracode"],
      selectableOptions: ["low", "medium", "high", "xhigh", "max", "ultrathink", "ultracode"],
      modelOptions: [
        {
          models: ["claude-fable-5-1", "claude-opus-5-5", "claude-opus-5", "claude-sonnet-5"],
          options: ["low", "medium", "high", "xhigh", "max", "ultrathink", "ultracode"],
        },
        {
          models: ["claude-sonnet-4-6"],
          options: ["low", "medium", "high", "ultrathink"],
        },
      ],
    },
  },
} as const satisfies {
  readonly agents: Record<AetherAgentFamily, AetherCatalogAgent>;
  readonly reasoningEffort: Record<AetherAgentFamily, AetherCatalogReasoningEffort>;
};

export const AETHER_AGENT_FAMILIES = ["codex", "claude-code"] as const satisfies readonly AetherAgentFamily[];

/**
 * Resolves the Aether agent family that runs `model`. Slugs are disjoint between
 * families today (a contracts test enforces it), so the slug alone identifies the family.
 */
export function resolveAetherAgentFamily(model: string): AetherAgentFamily | null {
  for (const family of AETHER_AGENT_FAMILIES) {
    if (AETHER_CATALOG.agents[family].models.some((entry) => entry.slug === model)) {
      return family;
    }
  }
  return null;
}

import { AETHER_AGENT_FAMILIES, AETHER_CATALOG } from "@synara/contracts";
import { describe, expect, it } from "vitest";

import {
  PROVIDER_DESCRIPTOR_BY_KIND,
  PROVIDER_DESCRIPTORS,
  cloudHostsFor,
  hostedProviderForModel,
  parsePresentedProvider,
  resolvePresentedProvider,
} from "./providerMetadata";

const slugsOf = (models: ReadonlyArray<{ slug: string }>) => models.map((model) => model.slug);

describe("cloud hosts", () => {
  it("gives every CLI provider no hosts", () => {
    for (const descriptor of PROVIDER_DESCRIPTORS) {
      if (descriptor.runtime === "cli") {
        expect(descriptor.hosts).toEqual([]);
      }
    }
  });

  it("hosts every Aether agent family exactly once, with its catalog", () => {
    const { hosts } = PROVIDER_DESCRIPTOR_BY_KIND.aether;
    expect(hosts).toHaveLength(AETHER_AGENT_FAMILIES.length);
    for (const family of AETHER_AGENT_FAMILIES) {
      const agent = AETHER_CATALOG.agents[family];
      const matching = hosts.filter(
        (host) => slugsOf(host.models).join(",") === slugsOf(agent.models).join(","),
      );
      expect(matching).toHaveLength(1);
      expect(matching[0]?.defaultModel).toBe(agent.defaultModel);
    }
  });

  it("resolves every hosted slug to exactly one harness", () => {
    for (const descriptor of PROVIDER_DESCRIPTORS) {
      for (const host of descriptor.hosts) {
        for (const slug of slugsOf(host.models)) {
          expect(hostedProviderForModel(descriptor.kind, slug)).toBe(host.provider);
          expect(
            descriptor.hosts.filter((candidate) => slugsOf(candidate.models).includes(slug)),
          ).toHaveLength(1);
        }
      }
    }
  });

  it("resolves a retired slug through the cloud's alias table", () => {
    expect(hostedProviderForModel("aether", "claude-fable-5")).toBe("claudeAgent");
    expect(hostedProviderForModel("aether", "gpt-5")).toBeNull();
    expect(hostedProviderForModel("codex", "gpt-5.4")).toBeNull();
  });

  it("lists the clouds that run a harness", () => {
    expect(cloudHostsFor("claudeAgent").map((entry) => entry.cloud)).toEqual(["aether"]);
    expect(cloudHostsFor("codex").map((entry) => entry.cloud)).toEqual(["aether"]);
    expect(cloudHostsFor("cursor")).toEqual([]);
  });

  it("presents a selection as its harness and cloud", () => {
    expect(resolvePresentedProvider({ provider: "codex", model: "gpt-5.4" })).toEqual({
      harness: "codex",
      cloud: null,
    });
    expect(resolvePresentedProvider({ provider: "aether", model: "claude-opus-5-5" })).toEqual({
      harness: "claudeAgent",
      cloud: "aether",
    });
    expect(resolvePresentedProvider({ provider: "aether", model: "gpt-6-astra" })).toEqual({
      harness: "codex",
      cloud: "aether",
    });
    expect(() => resolvePresentedProvider({ provider: "aether", model: "gpt-5" })).toThrow(
      'Aether has no host that runs "gpt-5".',
    );
  });

  it("parses a persisted cloud selection no host lists into an unhosted state", () => {
    expect(parsePresentedProvider({ provider: "aether", model: "gpt-5" })).toEqual({
      harness: null,
      cloud: "aether",
    });
    expect(parsePresentedProvider({ provider: "aether", model: "claude-fable-5" })).toEqual({
      harness: "claudeAgent",
      cloud: "aether",
    });
    expect(parsePresentedProvider({ provider: "cursor", model: "auto" })).toEqual({
      harness: "cursor",
      cloud: null,
    });
  });

  it("lets only a CLI provider switch a started thread's model", () => {
    for (const descriptor of PROVIDER_DESCRIPTORS) {
      expect(descriptor.supportsSessionModelSwitch).toBe(descriptor.runtime === "cli");
    }
  });
});

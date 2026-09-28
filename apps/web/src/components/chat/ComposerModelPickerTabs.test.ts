// FILE: ComposerModelPickerTabs.test.ts
// Purpose: Covers which provider tabs the composer model picker offers a selection, on this
//   machine and in the cloud.
// Layer: Component unit tests
// Depends on: ComposerModelPickerTabs pure helpers and Vitest assertions.

import { type ProviderKind, type ServerProviderStatus } from "@synara/contracts";
import { describe, expect, it } from "vitest";

import { resolveComposerModelPickerTabs } from "./ComposerModelPickerTabs";

const OPTIONS: ReadonlyArray<{ value: ProviderKind; label: string }> = [
  { value: "codex", label: "Codex" },
  { value: "claudeAgent", label: "Claude" },
  { value: "cursor", label: "Cursor" },
];

function ready(provider: ProviderKind): ServerProviderStatus {
  return {
    provider,
    status: "ready",
    available: true,
    authStatus: "authenticated",
    checkedAt: "2026-09-28T00:00:00.000Z",
  };
}

// Claude has no status yet: on this machine its tab waits as "Checking".
const PROVIDERS = [ready("codex"), ready("cursor"), ready("aether")];

const tabsFor = (input: Omit<Parameters<typeof resolveComposerModelPickerTabs>[0], "options">) =>
  resolveComposerModelPickerTabs({ options: OPTIONS, ...input }).map((tab) => [
    tab.provider,
    tab.unavailableLabel,
  ]);

describe("resolveComposerModelPickerTabs", () => {
  it.each([
    {
      name: "a local draft: every visible provider, by its status",
      presented: { harness: "codex", cloud: null },
      lockedProvider: null,
      expected: [
        ["codex", null],
        ["claudeAgent", "Checking"],
        ["cursor", null],
      ],
    },
    {
      name: "a started local thread: its provider only",
      presented: { harness: "codex", cloud: null },
      lockedProvider: "codex",
      expected: [["codex", null]],
    },
    {
      name: "a cloud draft: the hosts open whatever the local status, the rest not in Cloud",
      presented: { harness: "codex", cloud: "aether" },
      lockedProvider: null,
      expected: [
        ["claudeAgent", null],
        ["codex", null],
        ["cursor", "Not available in Cloud"],
      ],
    },
    {
      name: "a started cloud thread: the harness running its model",
      presented: { harness: "codex", cloud: "aether" },
      lockedProvider: "aether",
      expected: [["codex", null]],
    },
    {
      name: "a started cloud thread whose model no host lists: every host",
      presented: { harness: null, cloud: "aether" },
      lockedProvider: "aether",
      expected: [
        ["claudeAgent", null],
        ["codex", null],
      ],
    },
  ] as const)("offers $name", ({ presented, lockedProvider, expected }) => {
    expect(tabsFor({ providers: PROVIDERS, presented, lockedProvider })).toEqual(expected);
  });
});

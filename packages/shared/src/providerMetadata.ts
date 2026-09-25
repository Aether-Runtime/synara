// FILE: providerMetadata.ts
// Purpose: Exhaustive non-secret provider identity and presentation metadata.

import { PROVIDER_DISPLAY_NAMES, type ProviderKind } from "@synara/contracts";

export interface ProviderDescriptor {
  readonly kind: ProviderKind;
  readonly displayName: string;
  readonly available: boolean;
  /**
   * True when the provider runtime can inject a user message into a live turn
   * without interrupting it (Codex `turn/steer`, Pi `session.steer`, Claude
   * streaming-input prompt queue). Mirrors the adapter's
   * `supportsTurnSteering` capability so the pure decider and the web client
   * can route steers without a runtime round-trip; keep the two in sync.
   */
  readonly supportsNativeTurnSteering: boolean;
  /**
   * False when the provider's agent cannot reach Synara's local agent gateway (Aether runs in
   * a cloud VM), so it never gets the thread-scoped synara_* tools that thread goals, Computer
   * Use, browser automation and device tools depend on. Mirrors the adapter's
   * `reachesAgentGateway` capability; the adapter registry refuses a mismatch.
   */
  readonly reachesAgentGateway: boolean;
  /**
   * True when the provider owns its thread's worktree and branch (Aether mirrors its cloud VM
   * into it). Its threads always start in a new worktree, and nothing that shares, copies into,
   * or branches from the local checkout applies: Local mode, local↔worktree handoff,
   * copied changes, side chats, automations, and provider handoff. Mirrors the adapter's
   * `managesWorktreeBranch` capability.
   */
  readonly managesWorktreeBranch: boolean;
  /**
   * False when the provider cannot rewind its conversation, so checkpoint revert and
   * edit-and-resend are refused before any file is touched. Mirrors the adapter's
   * `conversationRollback` capability being `"unsupported"`.
   */
  readonly supportsConversationRollback: boolean;
  /**
   * How Synara reaches the provider: a CLI installed and launched on this machine, or a
   * cloud service it connects to with an API key, with nothing installed and no version.
   */
  readonly runtime: "cli" | "cloud";
  /** Synara docs page covering install, sign-in, and verification for this runtime. */
  readonly setupDocsHref: string;
  readonly usage: {
    readonly signInCommand: string;
    readonly learnMoreHref: string;
  } | null;
}

type ExhaustiveProviderDescriptors<Descriptors extends readonly ProviderDescriptor[]> =
  Exclude<ProviderKind, Descriptors[number]["kind"]> extends never ? Descriptors : never;

function defineProviderDescriptors<const Descriptors extends readonly ProviderDescriptor[]>(
  descriptors: ExhaustiveProviderDescriptors<Descriptors>,
): Descriptors {
  return descriptors;
}

export const PROVIDER_DESCRIPTORS = defineProviderDescriptors([
  {
    kind: "codex",
    displayName: PROVIDER_DISPLAY_NAMES.codex,
    available: true,
    runtime: "cli",
    setupDocsHref: "https://trysynara.com/docs/providers/codex",
    supportsNativeTurnSteering: true,
    reachesAgentGateway: true,
    managesWorktreeBranch: false,
    supportsConversationRollback: true,
    usage: {
      signInCommand: "codex login",
      learnMoreHref: "https://platform.openai.com/usage",
    },
  },
  {
    kind: "claudeAgent",
    displayName: PROVIDER_DISPLAY_NAMES.claudeAgent,
    available: true,
    runtime: "cli",
    setupDocsHref: "https://trysynara.com/docs/providers/claude-code",
    supportsNativeTurnSteering: true,
    reachesAgentGateway: true,
    managesWorktreeBranch: false,
    supportsConversationRollback: true,
    usage: {
      signInCommand: "claude",
      learnMoreHref: "https://docs.anthropic.com/en/docs/about-claude/models#rate-limits",
    },
  },
  {
    kind: "cursor",
    displayName: PROVIDER_DISPLAY_NAMES.cursor,
    available: true,
    runtime: "cli",
    setupDocsHref: "https://trysynara.com/docs/providers/cursor",
    supportsNativeTurnSteering: false,
    reachesAgentGateway: true,
    managesWorktreeBranch: false,
    supportsConversationRollback: true,
    usage: {
      signInCommand: "cursor-agent login",
      learnMoreHref: "https://cursor.com/dashboard",
    },
  },
  {
    kind: "antigravity",
    displayName: PROVIDER_DISPLAY_NAMES.antigravity,
    available: true,
    runtime: "cli",
    setupDocsHref: "https://trysynara.com/docs/providers/antigravity",
    supportsNativeTurnSteering: false,
    reachesAgentGateway: true,
    managesWorktreeBranch: false,
    supportsConversationRollback: true,
    usage: {
      signInCommand: "agy",
      learnMoreHref: "https://antigravity.google",
    },
  },
  {
    kind: "grok",
    displayName: PROVIDER_DISPLAY_NAMES.grok,
    available: true,
    runtime: "cli",
    setupDocsHref: "https://trysynara.com/docs/providers/grok",
    supportsNativeTurnSteering: false,
    reachesAgentGateway: true,
    managesWorktreeBranch: false,
    supportsConversationRollback: true,
    usage: {
      signInCommand: "grok login",
      learnMoreHref: "https://console.x.ai",
    },
  },
  {
    kind: "droid",
    displayName: PROVIDER_DISPLAY_NAMES.droid,
    available: true,
    runtime: "cli",
    setupDocsHref: "https://trysynara.com/docs/providers/factory-droid",
    supportsNativeTurnSteering: false,
    reachesAgentGateway: true,
    managesWorktreeBranch: false,
    supportsConversationRollback: true,
    usage: {
      signInCommand: "droid",
      learnMoreHref: "https://docs.factory.ai/pricing",
    },
  },
  {
    kind: "opencode",
    displayName: PROVIDER_DISPLAY_NAMES.opencode,
    available: true,
    runtime: "cli",
    setupDocsHref: "https://trysynara.com/docs/providers/opencode",
    supportsNativeTurnSteering: false,
    reachesAgentGateway: true,
    managesWorktreeBranch: false,
    supportsConversationRollback: true,
    usage: {
      signInCommand: "opencode auth login",
      learnMoreHref: "https://opencode.ai",
    },
  },
  {
    kind: "pi",
    displayName: PROVIDER_DISPLAY_NAMES.pi,
    available: true,
    runtime: "cli",
    setupDocsHref: "https://trysynara.com/docs/providers/pi",
    supportsNativeTurnSteering: true,
    reachesAgentGateway: true,
    managesWorktreeBranch: false,
    supportsConversationRollback: true,
    usage: {
      signInCommand: "pi",
      learnMoreHref: "https://pi.dev",
    },
  },
  {
    kind: "devin",
    displayName: PROVIDER_DISPLAY_NAMES.devin,
    available: true,
    runtime: "cli",
    setupDocsHref: "https://trysynara.com/docs/providers/devin",
    supportsNativeTurnSteering: false,
    reachesAgentGateway: true,
    managesWorktreeBranch: false,
    supportsConversationRollback: true,
    usage: {
      signInCommand: "devin auth login",
      learnMoreHref: "https://app.devin.ai/usage",
    },
  },
  {
    // Cloud provider: signs in with an API key in Settings, not a CLI. Usage/spend is v2.
    kind: "aether",
    displayName: PROVIDER_DISPLAY_NAMES.aether,
    available: true,
    runtime: "cloud",
    setupDocsHref: "https://runaether.dev",
    supportsNativeTurnSteering: false,
    reachesAgentGateway: false,
    managesWorktreeBranch: true,
    supportsConversationRollback: false,
    usage: null,
  },
] as const satisfies readonly ProviderDescriptor[]);

export const PROVIDER_DESCRIPTOR_BY_KIND = Object.fromEntries(
  PROVIDER_DESCRIPTORS.map((descriptor) => [descriptor.kind, descriptor]),
) as Record<ProviderKind, (typeof PROVIDER_DESCRIPTORS)[number]>;

// Accepts plain strings so projection-sourced provider names can be checked
// without casts; unknown providers are simply not steerable.
export const providerSupportsNativeTurnSteering = (kind: string): boolean =>
  PROVIDER_DESCRIPTORS.some(
    (descriptor) => descriptor.kind === kind && descriptor.supportsNativeTurnSteering,
  );

const findProviderDescriptor = (kind: string) =>
  PROVIDER_DESCRIPTORS.find((descriptor) => descriptor.kind === kind);

/** True for a cloud service Synara connects to, rather than a CLI installed locally. */
export const providerIsCloudService = (kind: string): boolean =>
  findProviderDescriptor(kind)?.runtime === "cloud";

// The capability checks below also accept plain strings. An unknown provider is not gated
// here: it cannot start a session, and the adapter registry already fails loudly for it.
export const providerReachesAgentGateway = (kind: string): boolean =>
  findProviderDescriptor(kind)?.reachesAgentGateway ?? true;

export const providerManagesWorktreeBranch = (kind: string): boolean =>
  findProviderDescriptor(kind)?.managesWorktreeBranch ?? false;

export const providerSupportsConversationRollback = (kind: string): boolean =>
  findProviderDescriptor(kind)?.supportsConversationRollback ?? true;

/** A Synara feature a provider capability can rule out. */
export type ProviderGatedFeature =
  | "thread-goal"
  | "computer-control"
  | "sidechat"
  | "automation"
  | "conversation-rollback"
  | "local-workspace"
  | "copy-changes"
  | "provider-handoff";

/**
 * The provider capability a feature needs, as one predicate. The server refuses and the web
 * hides with the same answer, so the two cannot drift apart.
 */
export function providerSupportsFeature(kind: string, feature: ProviderGatedFeature): boolean {
  switch (feature) {
    case "thread-goal":
    case "computer-control":
      return providerReachesAgentGateway(kind);
    case "sidechat":
    case "automation":
    case "local-workspace":
    case "copy-changes":
    case "provider-handoff":
      return !providerManagesWorktreeBranch(kind);
    case "conversation-rollback":
      return providerSupportsConversationRollback(kind);
  }
}

/**
 * The first provider in `providers` that lacks `feature`, or null when every one has it. A
 * thread can run on its bound session's provider, its selection, or a new selection, so
 * gates check all of them; absent entries are skipped.
 */
export function providerLackingFeature(
  providers: ReadonlyArray<string | null | undefined>,
  feature: ProviderGatedFeature,
): string | null {
  return (
    providers.find(
      (provider): provider is string =>
        typeof provider === "string" && !providerSupportsFeature(provider, feature),
    ) ?? null
  );
}

/** Why `feature` is unavailable for `kind`; the server's refusal text. */
export function providerFeatureUnavailableMessage(
  kind: string,
  feature: ProviderGatedFeature,
): string {
  const name = findProviderDescriptor(kind)?.displayName ?? kind;
  switch (feature) {
    case "thread-goal":
      return `Thread goals are unavailable with ${name}: its agent cannot reach Synara's agent gateway, so it could never mark a goal as achieved.`;
    case "computer-control":
      return `Computer Use, browser automation and device tools are unavailable with ${name}: its agent cannot reach Synara's agent gateway.`;
    case "sidechat":
      return `Side chats are unavailable in ${name} threads: a side chat shares the parent's checkout, which ${name} owns and resets on every turn.`;
    case "automation":
      return `Automations cannot run on ${name}: an automation branches from your local HEAD, which ${name} never sees.`;
    case "conversation-rollback":
      return `${name} cannot roll back its conversation, so reverting changes or editing earlier messages is unavailable in this thread.`;
    case "local-workspace":
      return `${name} threads always run in a new worktree that ${name} owns; they cannot work in the local checkout.`;
    case "copy-changes":
      return `${name} threads cannot copy local changes into their worktree: ${name} clones from origin and resets the worktree on every turn.`;
    case "provider-handoff":
      return `Handing a thread off to or from ${name} is not available yet: ${name} needs a clean, pushed worktree that it owns.`;
  }
}

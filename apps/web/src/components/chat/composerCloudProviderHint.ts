// FILE: composerCloudProviderHint.ts
// Purpose: Visibility rule and copy for the first-run composer hint shown when a new chat
//   targets a cloud provider (Aether): what it needs before its first task can run.
// Layer: Chat composer state helpers
// Exports: shouldShowCloudProviderHint, cloudProviderHintCopy

import type { ProviderKind } from "@synara/contracts";
import {
  PROVIDER_DESCRIPTOR_BY_KIND,
  providerIsCloudService,
} from "@synara/shared/providerMetadata";

/** New chats only, until dismissed once; driven by the descriptor, not the provider name. */
export function shouldShowCloudProviderHint(input: {
  readonly provider: ProviderKind;
  readonly dismissed: boolean;
  readonly threadHasMessages: boolean;
}): boolean {
  return !input.dismissed && !input.threadHasMessages && providerIsCloudService(input.provider);
}

export function cloudProviderHintCopy(provider: ProviderKind): {
  readonly message: string;
  readonly apiKeyHref: string;
} {
  const descriptor = PROVIDER_DESCRIPTOR_BY_KIND[provider];
  return {
    message: `${descriptor.displayName} runs this chat in the cloud. The repo must be on GitHub and imported as a project in ${descriptor.displayName}.`,
    apiKeyHref: descriptor.setupDocsHref,
  };
}

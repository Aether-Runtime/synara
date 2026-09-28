// FILE: ComposerCloudProviderHint.tsx
// Purpose: First-run composer tip for Aether, the cloud provider: it runs remotely and needs the
// repo on GitHub imported as a project there. Until Aether has a working API key, the tip also
// links to its site to get one.
// Layer: Chat composer UI
// Exports: ComposerCloudProviderHint

import { PROVIDER_DESCRIPTOR_BY_KIND } from "@synara/shared/providerMetadata";

import { CloudIcon } from "~/lib/icons";
import { COMPOSER_INLINE_ACTION_PILL_CLASS_NAME } from "./composerPickerStyles";
import { COMPOSER_STACKED_PANEL_ICON_CLASS_NAME } from "./composerStackedPanelStyles";
import { ComposerTipStrip } from "./ComposerTipStrip";

export function ComposerCloudProviderHint(props: {
  needsApiKey: boolean;
  onDismiss: () => void;
  attachedToPrevious?: boolean;
}) {
  return (
    <ComposerTipStrip
      data-testid="composer-cloud-provider-hint"
      wrap
      icon={<CloudIcon aria-hidden="true" className={COMPOSER_STACKED_PANEL_ICON_CLASS_NAME} />}
      action={
        props.needsApiKey ? (
          <a
            href={PROVIDER_DESCRIPTOR_BY_KIND.aether.setupDocsHref}
            target="_blank"
            rel="noreferrer"
            className={COMPOSER_INLINE_ACTION_PILL_CLASS_NAME}
          >
            Get an API key
          </a>
        ) : null
      }
      onDismiss={props.onDismiss}
      {...(props.attachedToPrevious !== undefined
        ? { attachedToPrevious: props.attachedToPrevious }
        : {})}
    >
      This chat runs in the cloud on {PROVIDER_DESCRIPTOR_BY_KIND.aether.displayName}. The repo must
      be on GitHub and imported as a project in {PROVIDER_DESCRIPTOR_BY_KIND.aether.displayName}.
    </ComposerTipStrip>
  );
}

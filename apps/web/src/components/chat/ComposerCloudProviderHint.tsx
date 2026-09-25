// FILE: ComposerCloudProviderHint.tsx
// Purpose: First-run composer tip for Aether, the cloud provider: it runs remotely, needs the
// repo on GitHub imported as a project there, and an API key from its site.
// Layer: Chat composer UI
// Exports: ComposerCloudProviderHint

import { PROVIDER_DESCRIPTOR_BY_KIND } from "@synara/shared/providerMetadata";

import { CloudIcon } from "~/lib/icons";
import { COMPOSER_INLINE_ACTION_PILL_CLASS_NAME } from "./composerPickerStyles";
import { COMPOSER_STACKED_PANEL_ICON_CLASS_NAME } from "./composerStackedPanelStyles";
import { ComposerTipStrip } from "./ComposerTipStrip";

export function ComposerCloudProviderHint(props: {
  onDismiss: () => void;
  attachedToPrevious?: boolean;
}) {
  return (
    <ComposerTipStrip
      data-testid="composer-cloud-provider-hint"
      wrap
      icon={<CloudIcon aria-hidden="true" className={COMPOSER_STACKED_PANEL_ICON_CLASS_NAME} />}
      action={
        <a
          href={PROVIDER_DESCRIPTOR_BY_KIND.aether.setupDocsHref}
          target="_blank"
          rel="noreferrer"
          className={COMPOSER_INLINE_ACTION_PILL_CLASS_NAME}
        >
          Get an API key
        </a>
      }
      onDismiss={props.onDismiss}
      {...(props.attachedToPrevious !== undefined
        ? { attachedToPrevious: props.attachedToPrevious }
        : {})}
    >
      Aether runs this chat in the cloud. The repo must be on GitHub and imported as a project in
      Aether.
    </ComposerTipStrip>
  );
}

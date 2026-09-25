// FILE: ComposerCloudProviderHint.tsx
// Purpose: First-run composer tip for a cloud provider: it runs remotely, needs the repo on
// GitHub imported as a project there, and an API key from its site.
// Layer: Chat composer UI
// Exports: ComposerCloudProviderHint

import type { ProviderKind } from "@synara/contracts";

import { CloudIcon } from "~/lib/icons";
import { cloudProviderHintCopy } from "./composerCloudProviderHint";
import { COMPOSER_INLINE_ACTION_PILL_CLASS_NAME } from "./composerPickerStyles";
import { COMPOSER_STACKED_PANEL_ICON_CLASS_NAME } from "./composerStackedPanelStyles";
import { ComposerTipStrip } from "./ComposerTipStrip";

export function ComposerCloudProviderHint(props: {
  provider: ProviderKind;
  onDismiss: () => void;
  attachedToPrevious?: boolean;
}) {
  const copy = cloudProviderHintCopy(props.provider);
  return (
    <ComposerTipStrip
      data-testid="composer-cloud-provider-hint"
      wrap
      icon={<CloudIcon aria-hidden="true" className={COMPOSER_STACKED_PANEL_ICON_CLASS_NAME} />}
      action={
        <a
          href={copy.apiKeyHref}
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
      {copy.message}
    </ComposerTipStrip>
  );
}

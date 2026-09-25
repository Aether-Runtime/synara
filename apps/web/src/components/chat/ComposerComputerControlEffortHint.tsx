// FILE: ComposerComputerControlEffortHint.tsx
// Purpose: Composer tip suggesting Medium effort while a chat drives the desktop, with
// one-click apply and a permanent dismiss.
// Layer: Chat composer UI
// Exports: ComposerComputerControlEffortHint

import { MonitorIcon } from "~/lib/icons";
import {
  COMPUTER_CONTROL_HINT_ACTION_LABEL,
  COMPUTER_CONTROL_HINT_MESSAGE,
} from "./composerComputerControlHint";
import { COMPOSER_INLINE_ACTION_PILL_CLASS_NAME } from "./composerPickerStyles";
import { COMPOSER_STACKED_PANEL_ICON_CLASS_NAME } from "./composerStackedPanelStyles";
import { ComposerTipStrip } from "./ComposerTipStrip";

interface ComposerComputerControlEffortHintProps {
  onApply: () => void;
  onDismiss: () => void;
  attachedToPrevious?: boolean;
}

export function ComposerComputerControlEffortHint({
  onApply,
  onDismiss,
  attachedToPrevious,
}: ComposerComputerControlEffortHintProps) {
  return (
    <ComposerTipStrip
      data-testid="composer-computer-control-effort-hint"
      icon={<MonitorIcon aria-hidden="true" className={COMPOSER_STACKED_PANEL_ICON_CLASS_NAME} />}
      action={
        <button type="button" className={COMPOSER_INLINE_ACTION_PILL_CLASS_NAME} onClick={onApply}>
          {COMPUTER_CONTROL_HINT_ACTION_LABEL}
        </button>
      }
      onDismiss={onDismiss}
      {...(attachedToPrevious !== undefined ? { attachedToPrevious } : {})}
    >
      {COMPUTER_CONTROL_HINT_MESSAGE}
    </ComposerTipStrip>
  );
}

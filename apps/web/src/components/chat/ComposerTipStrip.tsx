// FILE: ComposerTipStrip.tsx
// Purpose: One-line tip strip above the composer: a glyph, a message, an optional action
// and a permanent dismiss. Mounts and unmounts like its sibling stacked panels (live
// changes, goal) rather than animating, so the rail never reserves space for a tip that
// is not showing.
// Layer: Chat composer UI
// Exports: ComposerTipStrip

import type { ReactNode } from "react";

import { XIcon } from "~/lib/icons";
import { IconButton } from "../ui/icon-button";
import { ComposerStackedPanel } from "./ComposerStackedPanel";
import {
  ComposerStackedPanelRow,
  ComposerStackedPanelRowLabel,
  ComposerStackedPanelRowMain,
} from "./ComposerStackedPanelContent";

interface ComposerTipStripProps {
  icon: ReactNode;
  children: ReactNode;
  /** Inline action beside the dismiss button, e.g. a pill button or link. */
  action?: ReactNode;
  onDismiss: () => void;
  attachedToPrevious?: boolean;
  /** Let a longer message wrap instead of truncating to one line. */
  wrap?: boolean;
  "data-testid"?: string;
}

export function ComposerTipStrip({
  icon,
  children,
  action,
  onDismiss,
  attachedToPrevious: attachedToPreviousProp,
  wrap: wrapProp,
  "data-testid": testId,
}: ComposerTipStripProps) {
  const attachedToPrevious = attachedToPreviousProp ?? false;
  const wrap = wrapProp ?? false;
  return (
    <ComposerStackedPanel attachedToPrevious={attachedToPrevious} data-testid={testId}>
      <ComposerStackedPanelRow>
        <ComposerStackedPanelRowMain>
          {icon}
          <ComposerStackedPanelRowLabel {...(wrap ? { className: "whitespace-normal" } : {})}>
            {children}
          </ComposerStackedPanelRowLabel>
        </ComposerStackedPanelRowMain>
        <div className="flex shrink-0 items-center gap-1">
          {action}
          <IconButton variant="ghost" size="icon-chip" label="Dismiss tip" onClick={onDismiss}>
            <XIcon />
          </IconButton>
        </div>
      </ComposerStackedPanelRow>
    </ComposerStackedPanel>
  );
}

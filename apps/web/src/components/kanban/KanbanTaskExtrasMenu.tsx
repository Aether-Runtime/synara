// FILE: KanbanTaskExtrasMenu.tsx
// Purpose: Compact plus-menu for kanban task mode/location toggles.
// Layer: Kanban UI component
// Exports: KanbanTaskExtrasMenu

import type { ProviderInteractionMode, ProviderKind } from "@synara/contracts";

import {
  ComposerPickerMenuPopup,
  ComposerPickerMenuSubPopup,
} from "~/components/chat/ComposerPickerMenuPopup";
import { Button } from "~/components/ui/button";
import {
  Menu,
  MenuRadioGroup,
  MenuRadioItem,
  MenuSeparator,
  MenuSub,
  MenuSubTrigger,
  MenuTrigger,
} from "~/components/ui/menu";
import { CentralIcon } from "~/lib/central-icons";
import {
  BugIcon,
  CloudIcon,
  ListTodoIcon,
  MessageCircleIcon,
  PlusIcon,
  WorktreeIcon,
} from "~/lib/icons";
import { cloudLocationLabel } from "~/lib/threadEnvironment";
import type { DraftThreadEnvMode } from "../../composerDraftStore";
import type { CloudLocationRow } from "../BranchToolbar.logic";

/** A cloud row's glyph and label, with the reason it cannot be used under the label. */
function CloudLocationLabel({ label, detail }: { label: string; detail: string | null }) {
  return (
    <span className="inline-flex items-start gap-2">
      <CloudIcon className="mt-0.5 size-4 shrink-0" aria-hidden />
      <span className="flex min-w-0 flex-col">
        <span>{label}</span>
        {detail ? (
          <span className="text-ui-sm whitespace-normal text-muted-foreground">{detail}</span>
        ) : null}
      </span>
    </span>
  );
}

interface KanbanTaskExtrasMenuProps {
  readonly interactionMode: ProviderInteractionMode;
  readonly onInteractionModeChange: (mode: ProviderInteractionMode) => void;
  readonly envMode: DraftThreadEnvMode;
  readonly onEnvModeChange: (mode: DraftThreadEnvMode) => void;
  /** The cloud provider the task runs on; null when it runs on this machine. */
  readonly cloud: ProviderKind | null;
  /** Cloud providers offered as a location while the task runs on this machine. */
  readonly cloudRows: ReadonlyArray<CloudLocationRow>;
  /** Why the task's cloud cannot run in this workspace; null when it can. */
  readonly cloudUnavailableReason: string | null;
  /** False while the task's cloud model has no host, so there is no harness to return to. */
  readonly canLeaveCloud: boolean;
  readonly onCloudSelect: (cloud: ProviderKind) => void;
}

/**
 * The composer `+` analog: a single chrome icon button hosting the task's quick
 * toggles (Plan mode and the Local/Worktree/Cloud location), mirroring how the
 * composer's ComposerExtrasMenu collapses mode switches behind one `+`.
 */
export function KanbanTaskExtrasMenu({
  interactionMode,
  onInteractionModeChange,
  envMode,
  onEnvModeChange,
  cloud,
  cloudRows,
  cloudUnavailableReason,
  canLeaveCloud,
  onCloudSelect,
}: KanbanTaskExtrasMenuProps) {
  return (
    <Menu>
      <MenuTrigger
        render={
          <Button
            size="icon-sm"
            variant="chrome"
            className="shrink-0 rounded-md"
            aria-label="Task options"
          />
        }
      >
        <PlusIcon aria-hidden="true" className="size-4" />
      </MenuTrigger>
      <ComposerPickerMenuPopup align="start">
        <MenuSub>
          <MenuSubTrigger>Mode</MenuSubTrigger>
          <ComposerPickerMenuSubPopup>
            <MenuRadioGroup
              value={interactionMode}
              onValueChange={(value) => {
                if (value === "default" || value === "plan" || value === "debug") {
                  onInteractionModeChange(value);
                }
              }}
            >
              <MenuRadioItem value="default">
                <span className="inline-flex items-center gap-2">
                  <MessageCircleIcon className="size-4 shrink-0" />
                  Default
                </span>
              </MenuRadioItem>
              <MenuRadioItem value="plan">
                <span className="inline-flex items-center gap-2">
                  <ListTodoIcon className="size-4 shrink-0" />
                  Plan
                </span>
              </MenuRadioItem>
              <MenuRadioItem value="debug">
                <span className="inline-flex items-center gap-2">
                  <BugIcon className="size-4 shrink-0" />
                  Debug
                </span>
              </MenuRadioItem>
            </MenuRadioGroup>
          </ComposerPickerMenuSubPopup>
        </MenuSub>
        <MenuSeparator />
        {/* The location is the selection's provider, as in the composer's "Work in" menu:
            Cloud moves the task to the cloud host of its harness, Local and Worktree bring it
            back to that harness on this machine. */}
        <MenuRadioGroup
          value={cloud ?? envMode}
          onValueChange={(value) => {
            if (value === "local" || value === "worktree") {
              onEnvModeChange(value);
              return;
            }
            // Only an enabled row offered on this machine moves the task to a cloud.
            const row = cloudRows.find(
              (candidate) =>
                cloud === null && candidate.cloud === value && candidate.disabledReason === null,
            );
            if (row) {
              onCloudSelect(row.cloud);
            }
          }}
        >
          {cloud === null || canLeaveCloud ? (
            <>
              <MenuRadioItem value="local">
                <span className="inline-flex items-center gap-2">
                  <CentralIcon name="macbook-air" className="size-4 shrink-0" />
                  Local
                </span>
              </MenuRadioItem>
              <MenuRadioItem value="worktree">
                <span className="inline-flex items-center gap-2">
                  <WorktreeIcon className="size-4 shrink-0" aria-hidden />
                  Worktree
                </span>
              </MenuRadioItem>
            </>
          ) : null}
          {cloud !== null ? (
            <MenuRadioItem value={cloud}>
              <CloudLocationLabel
                label={cloudLocationLabel(cloud)}
                detail={cloudUnavailableReason}
              />
            </MenuRadioItem>
          ) : (
            cloudRows.map((row) => (
              <MenuRadioItem
                key={row.cloud}
                value={row.cloud}
                disabled={row.disabledReason !== null}
              >
                <CloudLocationLabel label={row.label} detail={row.disabledReason} />
              </MenuRadioItem>
            ))
          )}
        </MenuRadioGroup>
      </ComposerPickerMenuPopup>
    </Menu>
  );
}

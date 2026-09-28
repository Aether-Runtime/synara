// Purpose: Shared Local/Worktree/Cloud chip and menu for the full and floating composers.
import type { ProviderKind, ThreadEnvironmentMode } from "@synara/contracts";
import type { ReactNode } from "react";
import { CheckIcon, ChevronDownIcon, CloudIcon, HandoffIcon, WorktreeIcon } from "~/lib/icons";
import { CentralIcon } from "~/lib/central-icons";
import { cloudLocationLabel, type ThreadEnvironmentPresentation } from "~/lib/threadEnvironment";
import type { CloudLocationRow } from "../BranchToolbar.logic";
import { COMPOSER_TOOLBAR_PICKER_TRIGGER_CLASS_NAME } from "./composerPickerStyles";
import { ComposerPickerMenuPopup } from "./ComposerPickerMenuPopup";
import {
  ENVIRONMENT_ROW_CLASS_NAME,
  ENVIRONMENT_ROW_ICON_CLASS_NAME,
  EnvironmentRowBody,
  EnvironmentRowChevron,
} from "./environment/EnvironmentRow";
import { Menu, MenuGroup, MenuGroupLabel, MenuItem, MenuTrigger } from "../ui/menu";

/** Leading glyph treatment shared by every "Work in" menu row (16px, muted). */
const ENV_MENU_ICON_CLASS_NAME = "size-3.5 text-muted-foreground";

const NO_CLOUD_ROWS: ReadonlyArray<CloudLocationRow> = [];

/** The glyph of where a thread works: this machine's checkout, a worktree, or the cloud. */
export function EnvironmentModeGlyph({
  presentation,
  className,
}: {
  presentation: Pick<ThreadEnvironmentPresentation, "mode" | "cloud">;
  className: string;
}) {
  if (presentation.cloud !== null) {
    return <CloudIcon className={className} />;
  }
  return presentation.mode === "local" ? (
    <CentralIcon name="macbook-air" className={className} />
  ) : (
    <WorktreeIcon className={className} />
  );
}

/**
 * One row of the "Work in" menu: `[glyph] [label …grows] [✓ when selected]`, with an optional
 * detail line under the label saying why a disabled row cannot be chosen.
 * Centralizes the icon/label/check treatment so the local, worktree, cloud, and handoff
 * entries stay on one grid instead of repeating the same class strings per row.
 */
function WorkInMenuItem({
  icon,
  label,
  detail,
  selected: selectedProp,
  disabled: disabledProp,
  onSelect,
}: {
  icon: ReactNode;
  label: ReactNode;
  detail?: string | null;
  selected?: boolean;
  disabled?: boolean;
  onSelect?: (() => void) | undefined;
}) {
  const selected = selectedProp ?? false;
  const disabled = disabledProp ?? false;
  return (
    <MenuItem disabled={disabled} {...(onSelect ? { onClick: onSelect } : {})}>
      {icon}
      <span className="flex min-w-0 flex-1 flex-col">
        <span className="truncate">{label}</span>
        {detail ? (
          <span className="text-ui-sm whitespace-normal text-muted-foreground">{detail}</span>
        ) : null}
      </span>
      {selected ? (
        <CheckIcon className="size-3.5 shrink-0 text-[var(--color-text-foreground)]" />
      ) : null}
    </MenuItem>
  );
}

interface ComposerEnvironmentPickerProps {
  environmentPresentation: ThreadEnvironmentPresentation;
  onEnvModeChange: (mode: ThreadEnvironmentMode) => void;
  canSwitchToWorktree: boolean;
  /** False hides the Local row, for a provider that only runs in its own worktree. */
  canSwitchToLocal?: boolean;
  /** Cloud providers offered as a location while the thread works on this machine. */
  cloudRows?: ReadonlyArray<CloudLocationRow> | undefined;
  onCloudSelect?: ((cloud: ProviderKind) => void) | undefined;
  canHandoffToLocal?: boolean;
  canHandoffToWorktree?: boolean;
  onHandoffToLocal?: (() => void) | undefined;
  onHandoffToWorktree?: (() => void) | undefined;
  handoffBusy?: boolean | undefined;
  isPanel?: boolean;
  disabled?: boolean;
  onOpenChange?: ((open: boolean) => void) | undefined;
  children?: ReactNode;
}

export function ComposerEnvironmentPicker({
  environmentPresentation,
  onEnvModeChange,
  canSwitchToWorktree,
  canSwitchToLocal = true,
  cloudRows = NO_CLOUD_ROWS,
  onCloudSelect,
  canHandoffToLocal = false,
  canHandoffToWorktree = false,
  onHandoffToLocal,
  onHandoffToWorktree,
  handoffBusy = false,
  isPanel = false,
  disabled = false,
  onOpenChange,
  children,
}: ComposerEnvironmentPickerProps) {
  const envGlyph = (className: string) => (
    <EnvironmentModeGlyph presentation={environmentPresentation} className={className} />
  );
  return (
    <Menu onOpenChange={onOpenChange}>
      <MenuTrigger
        disabled={disabled}
        render={
          <button
            type="button"
            className={
              isPanel ? ENVIRONMENT_ROW_CLASS_NAME : COMPOSER_TOOLBAR_PICKER_TRIGGER_CLASS_NAME
            }
          />
        }
      >
        {isPanel ? (
          <EnvironmentRowBody
            icon={envGlyph(ENVIRONMENT_ROW_ICON_CLASS_NAME)}
            label={environmentPresentation.shortLabel}
            trailing={<EnvironmentRowChevron />}
          />
        ) : (
          <>
            {envGlyph("size-3.5")}
            {environmentPresentation.shortLabel}
            <ChevronDownIcon className="size-3 opacity-60" />
          </>
        )}
      </MenuTrigger>
      <ComposerPickerMenuPopup
        data-composer-environment-menu="true"
        align="start"
        side={isPanel ? "bottom" : "top"}
        sideOffset={6}
        className="w-60 min-w-60"
      >
        <MenuGroup>
          <MenuGroupLabel>Work in</MenuGroupLabel>
          {environmentPresentation.cloud !== null ? (
            // In the cloud, Local and New worktree are the ways back to the local harness.
            <>
              {canSwitchToLocal ? (
                <WorkInMenuItem
                  icon={<CentralIcon name="macbook-air" className={ENV_MENU_ICON_CLASS_NAME} />}
                  label={environmentPresentation.localOptionLabel}
                  onSelect={() => onEnvModeChange("local")}
                />
              ) : null}
              {canSwitchToWorktree ? (
                <WorkInMenuItem
                  icon={<WorktreeIcon className={ENV_MENU_ICON_CLASS_NAME} />}
                  label="New worktree"
                  onSelect={() => onEnvModeChange("worktree")}
                />
              ) : null}
              <WorkInMenuItem
                icon={<CloudIcon className={ENV_MENU_ICON_CLASS_NAME} />}
                label={cloudLocationLabel(environmentPresentation.cloud)}
                selected
              />
            </>
          ) : (
            <>
              {environmentPresentation.mode === "local" ? (
                <WorkInMenuItem
                  icon={<CentralIcon name="macbook-air" className={ENV_MENU_ICON_CLASS_NAME} />}
                  label={environmentPresentation.localOptionLabel}
                  selected
                />
              ) : canSwitchToLocal ? (
                <WorkInMenuItem
                  icon={<CentralIcon name="macbook-air" className={ENV_MENU_ICON_CLASS_NAME} />}
                  label={environmentPresentation.localOptionLabel}
                  onSelect={() => onEnvModeChange("local")}
                />
              ) : null}
              {canSwitchToWorktree ? (
                <WorkInMenuItem
                  icon={<WorktreeIcon className={ENV_MENU_ICON_CLASS_NAME} />}
                  label="New worktree"
                  onSelect={() => onEnvModeChange("worktree")}
                />
              ) : null}
              {environmentPresentation.mode === "worktree" && !canHandoffToLocal ? (
                <WorkInMenuItem
                  icon={<WorktreeIcon className={ENV_MENU_ICON_CLASS_NAME} />}
                  label={environmentPresentation.worktreeOptionLabel}
                  selected
                />
              ) : null}
              {canHandoffToWorktree && onHandoffToWorktree ? (
                <WorkInMenuItem
                  icon={<WorktreeIcon className={ENV_MENU_ICON_CLASS_NAME} />}
                  label="Hand off to new worktree"
                  disabled={handoffBusy}
                  onSelect={() => onHandoffToWorktree()}
                />
              ) : null}
              {canHandoffToLocal && onHandoffToLocal ? (
                <WorkInMenuItem
                  icon={<HandoffIcon className={ENV_MENU_ICON_CLASS_NAME} />}
                  label="Hand off to local"
                  disabled={handoffBusy}
                  onSelect={() => onHandoffToLocal()}
                />
              ) : null}
              {onCloudSelect
                ? cloudRows.map((row) => (
                    <WorkInMenuItem
                      key={row.cloud}
                      icon={<CloudIcon className={ENV_MENU_ICON_CLASS_NAME} />}
                      label={row.label}
                      detail={row.disabledReason}
                      disabled={row.disabledReason !== null}
                      onSelect={() => onCloudSelect(row.cloud)}
                    />
                  ))
                : null}
            </>
          )}
        </MenuGroup>

        {children}
      </ComposerPickerMenuPopup>
    </Menu>
  );
}

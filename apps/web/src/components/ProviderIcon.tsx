/**
 * ProviderIcon - shared provider glyphs for chat, sidebar, and picker surfaces.
 *
 * Centralizes provider-to-icon mapping so new providers do not need repeated
 * branching across every UI surface.
 */
import { type ProviderKind } from "@synara/contracts";
import { parsePresentedProvider } from "@synara/shared/providerMetadata";
import type { ReactNode, SVGProps } from "react";

import { CentralIcon } from "~/lib/central-icons";
import { CloudIcon } from "~/lib/icons";
import { cn } from "~/lib/utils";
import {
  AntigravityIcon,
  ClaudeAI,
  CursorIcon,
  DevinIcon,
  DroidIcon,
  GrokIcon,
  type Icon,
  OpenAI,
  OpenCodeIcon,
  PiIcon,
} from "./Icons";

export type ProviderIconTone = "default" | "header";

// The bundled SVG has a dark outer fill, so dark mode swaps to the reversed Central asset.
// React's SVGProps has no `title`, so accept it via an explicit prop type and forward it
// only to CentralIcon (an HTML span, which supports `title`); the light-mode SVG conveys
// its accessible name through aria-label instead.
const OpenCodeProviderIcon = ({
  className,
  style,
  title,
  role,
  "aria-hidden": ariaHidden,
  "aria-label": ariaLabel,
  ...svgProps
}: SVGProps<SVGSVGElement> & { title?: string }) => {
  const centralIconLabel =
    ariaHidden === true || ariaHidden === "true" || typeof ariaLabel !== "string"
      ? undefined
      : ariaLabel;

  return (
    <>
      <OpenCodeIcon
        {...svgProps}
        aria-hidden={ariaHidden}
        aria-label={ariaLabel}
        role={role}
        className={cn(className, "dark:hidden")}
        style={style}
      />
      <CentralIcon
        name="opencode"
        label={centralIconLabel}
        title={title}
        className={cn(className, "hidden dark:inline-block dark:text-foreground/90")}
        style={style}
      />
    </>
  );
};

export const PROVIDER_ICON_COMPONENT_BY_PROVIDER: Record<ProviderKind, Icon> = {
  codex: OpenAI,
  claudeAgent: ClaudeAI,
  cursor: CursorIcon,
  devin: DevinIcon,
  antigravity: AntigravityIcon,
  grok: GrokIcon,
  droid: DroidIcon,
  opencode: OpenCodeProviderIcon,
  pi: PiIcon,
  aether: CloudIcon,
};

export function providerIconToneClassName(
  provider: ProviderKind | null | undefined,
  tone: ProviderIconTone = "default",
): string {
  if (provider === "opencode") {
    return "text-muted-foreground/70";
  }
  if (provider === "codex") {
    return tone === "header" ? "text-muted-foreground/85" : "text-foreground";
  }
  return "text-foreground";
}

export type ProviderIconProps = Omit<SVGProps<SVGSVGElement>, "ref"> & {
  readonly provider: ProviderKind | null | undefined;
  readonly fallback?: ReactNode;
  readonly tone?: ProviderIconTone;
};

export function ProviderIcon({
  provider,
  fallback: fallbackProp,
  tone: toneProp,
  className,
  "aria-hidden": ariaHiddenProp,
  ...svgProps
}: ProviderIconProps) {
  const fallback = fallbackProp ?? null;
  const tone = toneProp ?? "default";
  const ariaHidden = ariaHiddenProp ?? true;
  if (provider === null || provider === undefined) {
    return fallback;
  }

  const Icon = PROVIDER_ICON_COMPONENT_BY_PROVIDER[provider];
  return (
    <Icon
      aria-hidden={ariaHidden}
      {...svgProps}
      className={cn(providerIconToneClassName(provider, tone), className)}
    />
  );
}

/**
 * The avatar of a model selection: the icon of the harness that runs it, so a cloud selection
 * shows the host's harness (Claude, Codex) rather than the cloud. A persisted cloud model no
 * host lists has no harness and shows the cloud's own icon.
 */
export function ProviderAvatar({
  selection,
  ...iconProps
}: Omit<ProviderIconProps, "provider" | "fallback"> & {
  readonly selection: { readonly provider: ProviderKind; readonly model: string };
}) {
  const presented = parsePresentedProvider(selection);
  return (
    <ProviderIcon
      {...iconProps}
      provider={presented.harness === null ? presented.cloud : presented.harness}
    />
  );
}

export function ProviderOptionLabel({
  provider,
  label,
  className,
  iconClassName,
}: {
  provider: ProviderKind;
  label: ReactNode;
  className?: string;
  iconClassName?: string;
}) {
  return (
    <span className={cn("flex min-w-0 items-center gap-2", className)}>
      <ProviderIcon provider={provider} className={cn("size-3.5", iconClassName)} />
      <span className="min-w-0 truncate">{label}</span>
    </span>
  );
}

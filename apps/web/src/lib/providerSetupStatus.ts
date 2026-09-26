import type { ServerProviderStatus } from "@synara/contracts";

/** The provider runs, but its sign-in or credentials are missing or were rejected. */
export function providerNeedsSignIn(status: ServerProviderStatus): boolean {
  return status.available && status.authStatus === "unauthenticated";
}

/** Installation/auth health is independent of permission to run background work. */
export function providerSetupStatusLabel(input: {
  readonly status: ServerProviderStatus | undefined;
  readonly reconciled: boolean;
  readonly disabled: boolean;
}): string {
  if (input.disabled) return "Disabled · enable to check setup";
  if (!input.reconciled || !input.status) return "Checking setup";
  const status = input.status;
  // Missing CLIs and failed probes both report available=false. The server's
  // message supplies the specific diagnosis alongside this label in Settings.
  if (!status.available) return "Unavailable";
  if (providerNeedsSignIn(status)) return "Needs sign-in";
  if (status.status !== "ready") return "Needs attention";
  if (status.authStatus === "unknown") return "Installed · sign-in not verified";
  return "Connected";
}

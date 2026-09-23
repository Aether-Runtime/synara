import "../../index.css";

import type { ServerProviderStatus } from "@synara/contracts";
import { PROVIDER_DESCRIPTORS } from "@synara/shared/providerMetadata";
import { page, userEvent } from "vitest/browser";
import { beforeEach, expect, it, vi } from "vitest";
import { render } from "vitest-browser-react";

const harness = vi.hoisted(() => ({
  statuses: [] as ServerProviderStatus[],
  reconciled: true,
  refresh: vi.fn(),
}));

vi.mock("@tanstack/react-query", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@tanstack/react-query")>()),
  useQueryClient: () => ({}),
  useQuery: () => ({ data: { providers: harness.statuses }, isPending: false }),
}));
vi.mock("~/lib/serverReactQuery", async (importOriginal) => ({
  ...(await importOriginal<typeof import("~/lib/serverReactQuery")>()),
  serverConfigQueryOptions: () => ({}),
  serverSettingsQueryOptions: () => ({}),
  hasReconciledServerProviderStatuses: () => harness.reconciled,
  serverQueryKeys: { config: () => ["config"] },
}));
vi.mock("~/hooks/useProviderStatusesForLocalConfig", () => ({
  useProviderStatusesForLocalConfig: () => harness.statuses,
}));
vi.mock("~/hooks/useProviderStatusRefresh", () => ({
  useRefreshProviderStatusesNow: () => harness.refresh,
}));

import { AppSettingsSchema } from "~/appSettings";
import { ProvidersSettingsPanel, type ProvidersSettingsPanelProps } from "./ProvidersSettingsPanel";

const defaults = AppSettingsSchema.makeUnsafe({});
const props = {
  defaults,
  settings: { ...defaults, disabledProviders: ["grok" as const] },
  updateSettings: vi.fn(),
  updateSettingsAndWait: vi.fn(async () => {}),
  active: true,
  resetEpoch: 0,
};

beforeEach(() => {
  harness.reconciled = true;
  harness.refresh.mockReset();
  harness.statuses = PROVIDER_DESCRIPTORS.map(({ kind }) => ({
    provider: kind,
    status: kind === "opencode" ? "error" : "ready",
    available: kind !== "opencode",
    authStatus: kind === "claudeAgent" ? "unauthenticated" : "authenticated",
    checkedAt: "2026-09-16T21:46:18.000Z",
    ...(kind === "opencode"
      ? { message: "OpenCode CLI (`opencode`) is not installed or not on PATH." }
      : {}),
  }));
});

function activityRow(provider: string) {
  return page
    .getByRole("switch", { name: `Disable ${provider}`, exact: true })
    .element()
    .closest('[data-slot="settings-row"]')!;
}

it("shows installation and auth beside activity switches with visible setup guides", async () => {
  await render(<ProvidersSettingsPanel {...props} />);
  expect(activityRow("OpenCode").textContent).toContain("Unavailable");
  expect(activityRow("OpenCode").textContent).toContain("not installed or not on PATH");
  expect(activityRow("Claude").textContent).toContain("Needs sign-in");
  expect(activityRow("Codex").textContent).toContain("Connected");
  expect(
    page
      .getByRole("switch", { name: "Enable Grok", exact: true })
      .element()
      .closest('[data-slot="settings-row"]')?.textContent,
  ).toContain("Disabled · enable to check setup");
  // Permission to run remains enabled even if the CLI is missing.
  await expect
    .element(page.getByRole("switch", { name: "Disable OpenCode", exact: true }))
    .toBeChecked();
  for (const descriptor of PROVIDER_DESCRIPTORS) {
    const guide = page.getByRole("link", { name: `${descriptor.displayName} setup guide` });
    await expect.element(guide).toBeVisible();
    expect(guide.element().getAttribute("href")).toBe(descriptor.setupDocsHref);
  }
});

it("does not report cached provider health as connected before reconciliation", async () => {
  harness.reconciled = false;
  await render(<ProvidersSettingsPanel {...props} />);
  expect(activityRow("Codex").textContent).toContain("Checking setup");
  expect(activityRow("Codex").textContent).not.toContain("Connected");
});

it("allows rechecking setup after installing externally and blocks duplicate refreshes", async () => {
  let finish!: () => void;
  harness.refresh.mockImplementation(
    () =>
      new Promise<void>((resolve) => {
        finish = resolve;
      }),
  );
  await render(<ProvidersSettingsPanel {...props} />);
  await page.getByRole("button", { name: "Refresh status", exact: true }).click();
  expect(harness.refresh).toHaveBeenCalledOnce();
  await expect
    .element(page.getByRole("button", { name: "Checking setup", exact: true }))
    .toBeDisabled();
  finish();
  await expect
    .element(page.getByRole("button", { name: "Refresh status", exact: true }))
    .toBeEnabled();
});

type UpdateSettings = ProvidersSettingsPanelProps["updateSettings"];

async function editAetherBaseUrl(value: string, updateSettings: UpdateSettings) {
  await render(<ProvidersSettingsPanel {...props} updateSettings={updateSettings} />);
  await page.getByRole("button", { name: "Aether", exact: true }).click();
  await page.getByRole("textbox", { name: /Aether API base URL/ }).fill(value);
  await userEvent.tab();
}

it.each([
  ["", "The Aether API base URL is required."],
  ["not a url", "The Aether API base URL is not a valid URL."],
  [
    "http://api.runaether.dev",
    "The Aether API base URL must use https:// (http:// only for localhost, 127.0.0.1 or [::1]).",
  ],
])("shows the Aether base URL %j as an inline error and never saves it", async (value, issue) => {
  const updateSettings = vi.fn<UpdateSettings>();
  await editAetherBaseUrl(value, updateSettings);

  await expect.element(page.getByRole("alert")).toHaveTextContent(issue);
  expect(updateSettings).not.toHaveBeenCalledWith(
    expect.objectContaining({ aetherApiBaseUrl: expect.anything() }),
  );
});

it("saves a valid Aether base URL", async () => {
  const updateSettings = vi.fn<UpdateSettings>();
  await editAetherBaseUrl("https://proxy.example.com/aether", updateSettings);

  expect(updateSettings).toHaveBeenCalledWith({
    aetherApiBaseUrl: "https://proxy.example.com/aether",
  });
  expect(page.getByRole("alert").elements()).toHaveLength(0);
});

it("clears a stale base URL alert when the draft returns to the stored value", async () => {
  const updateSettings = vi.fn<UpdateSettings>();
  await editAetherBaseUrl("http://api.runaether.dev", updateSettings);
  await expect.element(page.getByRole("alert")).toBeInTheDocument();

  // Back to the stored value and away within the debounce: nothing to commit, but the
  // field is valid again, so blur re-validates and the alert goes away.
  await page.getByRole("textbox", { name: /Aether API base URL/ }).fill(defaults.aetherApiBaseUrl);
  await userEvent.tab();

  await expect.element(page.getByRole("alert")).not.toBeInTheDocument();
  expect(updateSettings).not.toHaveBeenCalledWith(
    expect.objectContaining({ aetherApiBaseUrl: expect.anything() }),
  );
});

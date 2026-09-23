import {
  DEFAULT_SERVER_SETTINGS,
  ProviderSessionStartInput,
  ServerSettings,
} from "@synara/contracts";
import { Schema } from "effect";
import { describe, expect, it } from "vitest";
import { providerStartOptionsFromServerSettings } from "./serverSettings";

const decodeProviderSessionStartInput = Schema.decodeUnknownSync(ProviderSessionStartInput);

describe("providerStartOptionsFromServerSettings", () => {
  it("omits blank launch settings from provider session input", () => {
    const settings = {
      ...DEFAULT_SERVER_SETTINGS,
      providers: {
        codex: {
          ...DEFAULT_SERVER_SETTINGS.providers.codex,
          binaryPath: "",
          homePath: "",
        },
        claudeAgent: {
          ...DEFAULT_SERVER_SETTINGS.providers.claudeAgent,
          binaryPath: "",
        },
        cursor: {
          ...DEFAULT_SERVER_SETTINGS.providers.cursor,
          binaryPath: "",
          apiEndpoint: "",
        },
        antigravity: {
          ...DEFAULT_SERVER_SETTINGS.providers.antigravity,
          binaryPath: "",
        },
        grok: {
          ...DEFAULT_SERVER_SETTINGS.providers.grok,
          binaryPath: "",
        },
        droid: {
          ...DEFAULT_SERVER_SETTINGS.providers.droid,
          binaryPath: "",
        },
        opencode: {
          ...DEFAULT_SERVER_SETTINGS.providers.opencode,
          binaryPath: "",
          serverUrl: "",
        },
        pi: {
          ...DEFAULT_SERVER_SETTINGS.providers.pi,
          binaryPath: "",
          agentDir: "",
        },
        devin: {
          ...DEFAULT_SERVER_SETTINGS.providers.devin,
          binaryPath: "",
        },
        // Aether's base URL cannot be blank: the settings schema rejects it (aetherApi.ts).
        aether: DEFAULT_SERVER_SETTINGS.providers.aether,
      },
    };

    const providerOptions = providerStartOptionsFromServerSettings(settings);

    expect(() =>
      decodeProviderSessionStartInput({
        threadId: "thread-1",
        provider: "codex",
        providerOptions,
        runtimeMode: "full-access",
      }),
    ).not.toThrow();
    expect(providerOptions.codex).toEqual({});
    expect(providerOptions.claudeAgent).toEqual({ enableArtifacts: false });
    expect(providerOptions.cursor).toEqual({});
    expect(providerOptions.antigravity).toEqual({});
    expect(providerOptions.grok).toEqual({});
    expect(providerOptions.droid).toEqual({});
    expect(providerOptions.opencode).toEqual({ experimentalWebSockets: false });
    expect(providerOptions.pi).toEqual({});
    expect(providerOptions.devin).toEqual({});
    expect(providerOptions.aether).toEqual({ apiBaseUrl: "https://api.runaether.dev" });
  });

  it("preserves configured launch settings", () => {
    const settings = {
      ...DEFAULT_SERVER_SETTINGS,
      providers: {
        ...DEFAULT_SERVER_SETTINGS.providers,
        codex: {
          ...DEFAULT_SERVER_SETTINGS.providers.codex,
          binaryPath: "/custom/bin/codex",
          homePath: "/custom/codex-home",
        },
        opencode: {
          ...DEFAULT_SERVER_SETTINGS.providers.opencode,
          binaryPath: "/custom/bin/opencode",
          serverUrl: "http://127.0.0.1:4096",
          experimentalWebSockets: true,
        },
        devin: {
          ...DEFAULT_SERVER_SETTINGS.providers.devin,
          binaryPath: "/custom/bin/devin",
        },
      },
    };

    const providerOptions = providerStartOptionsFromServerSettings(settings);

    expect(providerOptions.codex).toEqual({
      binaryPath: "/custom/bin/codex",
      homePath: "/custom/codex-home",
    });
    expect(providerOptions.opencode).toEqual({
      binaryPath: "/custom/bin/opencode",
      serverUrl: "http://127.0.0.1:4096",
      experimentalWebSockets: true,
    });
    expect(providerOptions.devin).toEqual({ binaryPath: "/custom/bin/devin" });
  });

  it("passes Aether's parsed base URL through unchanged", () => {
    const providerOptions = providerStartOptionsFromServerSettings(
      Schema.decodeUnknownSync(ServerSettings)({
        providers: { aether: { apiBaseUrl: "https://proxy.example.com/aether/" } },
      }),
    );

    expect(providerOptions.aether).toEqual({ apiBaseUrl: "https://proxy.example.com/aether/" });
  });

  it("returns an entry for every provider, including Aether's default API base URL", () => {
    const providerOptions = providerStartOptionsFromServerSettings(DEFAULT_SERVER_SETTINGS);

    expect(Object.keys(providerOptions).toSorted()).toEqual(
      Object.keys(DEFAULT_SERVER_SETTINGS.providers).toSorted(),
    );
    expect(providerOptions.aether).toEqual({ apiBaseUrl: "https://api.runaether.dev" });
  });
});

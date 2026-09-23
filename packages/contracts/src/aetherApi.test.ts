import { Schema } from "effect";
import { describe, expect, it } from "vitest";

import {
  AetherApiBaseUrl,
  aetherApiBaseUrlIssue,
  DEFAULT_AETHER_API_BASE_URL,
  resolveAetherApiUrl,
} from "./aetherApi";
import { ServerSettings, ServerSettingsPatch } from "./settings";

const decodeSettings = Schema.decodeUnknownExit(ServerSettings);
const decodePatch = Schema.decodeUnknownExit(ServerSettingsPatch);

const settingsWithBaseUrl = (apiBaseUrl: unknown) => ({ providers: { aether: { apiBaseUrl } } });

describe("Aether API base URL contract", () => {
  it.each([
    ["", "The Aether API base URL is required."],
    ["   ", "The Aether API base URL is required."],
    ["not a url", "The Aether API base URL is not a valid URL."],
    ["api.runaether.dev", "The Aether API base URL is not a valid URL."],
    ["http://api.runaether.dev", "The Aether API base URL must use https://."],
    ["ftp://api.runaether.dev", "The Aether API base URL must use https://."],
    [
      "https://user:secret@api.runaether.dev",
      "The Aether API base URL must not contain credentials.",
    ],
    [
      "https://api.runaether.dev/?token=x",
      "The Aether API base URL must not contain a query or fragment.",
    ],
  ])("rejects %j", (value, issue) => {
    expect(aetherApiBaseUrlIssue(value)).toBe(issue);

    const settings = decodeSettings(settingsWithBaseUrl(value));
    const patch = decodePatch(settingsWithBaseUrl(value));
    expect(settings._tag).toBe("Failure");
    expect(patch._tag).toBe("Failure");
    // Loud and specific, never a silent fallback to the default.
    expect(String(settings._tag === "Failure" ? settings.cause : "")).toContain(issue);
    expect(String(patch._tag === "Failure" ? patch.cause : "")).toContain(issue);
  });

  it.each([
    "https://api.runaether.dev",
    "https://proxy.example.com/aether/",
    "  https://api.runaether.dev  ",
  ])("accepts and stores %j", (value) => {
    const decoded = decodeSettings(settingsWithBaseUrl(value));

    expect(decoded._tag).toBe("Success");
    expect(decoded._tag === "Success" ? decoded.value.providers.aether.apiBaseUrl : null).toBe(
      value.trim(),
    );
    expect(decodePatch(settingsWithBaseUrl(value))._tag).toBe("Success");
  });

  it("defaults only when the value is absent", () => {
    const decoded = Schema.decodeUnknownSync(ServerSettings)({});

    expect(decoded.providers.aether.apiBaseUrl).toBe(DEFAULT_AETHER_API_BASE_URL);
  });

  it("resolves endpoint paths against the base URL, keeping any base path", () => {
    expect(
      String(
        resolveAetherApiUrl(AetherApiBaseUrl.makeUnsafe(DEFAULT_AETHER_API_BASE_URL), "/profile"),
      ),
    ).toBe("https://api.runaether.dev/profile");
    expect(
      String(
        resolveAetherApiUrl(
          AetherApiBaseUrl.makeUnsafe("https://proxy.example.com/aether"),
          "profile",
        ),
      ),
    ).toBe("https://proxy.example.com/aether/profile");
  });
});

// FILE: aetherApi.ts
// Purpose: The Aether API base URL, parsed once at the settings boundary.
//
// Settings decoding and patching are the only places a raw base URL is interpreted: blank or
// invalid input is rejected with a validation error, never replaced by the default. Every
// reader (health check, session start options) receives the branded, already-valid value and
// builds endpoint URLs with `resolveAetherApiUrl`, so there is no second interpretation.

import { Schema } from "effect";

import { TrimmedString } from "./baseSchemas";

export const DEFAULT_AETHER_API_BASE_URL = "https://api.runaether.dev";

/** Why `value` is not a usable Aether API base URL, or `undefined` when it is. */
export function aetherApiBaseUrlIssue(value: string): string | undefined {
  if (value.trim().length === 0) {
    return "The Aether API base URL is required.";
  }
  let url: URL;
  try {
    url = new URL(value.trim());
  } catch {
    return "The Aether API base URL is not a valid URL.";
  }
  if (url.protocol !== "https:") {
    return "The Aether API base URL must use https://.";
  }
  if (url.username || url.password) {
    return "The Aether API base URL must not contain credentials.";
  }
  if (url.search || url.hash) {
    return "The Aether API base URL must not contain a query or fragment.";
  }
  return undefined;
}

const aetherApiBaseUrlCheck = Schema.makeFilter<string>((value) => aetherApiBaseUrlIssue(value));

/** A patch value: validated, but carries no brand so clients can send a plain string. */
export const AetherApiBaseUrlInput = TrimmedString.check(
  Schema.isMaxLength(4096),
  aetherApiBaseUrlCheck,
);

/** A stored, validated `https:` base URL. Only settings decoding produces one. */
export const AetherApiBaseUrl = AetherApiBaseUrlInput.pipe(Schema.brand("AetherApiBaseUrl"));
export type AetherApiBaseUrl = typeof AetherApiBaseUrl.Type;

/** Resolves an API path against the base URL, keeping any base path prefix. */
export function resolveAetherApiUrl(apiBaseUrl: AetherApiBaseUrl, path: string): URL {
  return new URL(
    path.replace(/^\/+/u, ""),
    apiBaseUrl.endsWith("/") ? apiBaseUrl : `${apiBaseUrl}/`,
  );
}

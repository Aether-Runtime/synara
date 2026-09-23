// FILE: aetherHealth.ts
// Purpose: Network-only Aether health check: `GET {apiBaseUrl}/profile` with the stored API key.
// Layer: Server provider health. Aether runs in the cloud, so there is no executable
// lookup and no version probe.

import {
  resolveAetherApiUrl,
  TrimmedNonEmptyString,
  type AetherApiBaseUrl,
  type ServerProviderStatus,
} from "@synara/contracts";
import {
  decodeOutboundJson,
  OutboundHttpError,
  outboundHttp,
  type OutboundHttpRequest,
  type OutboundHttpResponse,
} from "@synara/shared/outboundHttp";
import { OutboundPolicyError } from "@synara/shared/outboundHttpPolicy";
import { Effect, Schema } from "effect";

const PROVIDER = "aether" as const;
const AETHER_HEALTH_TIMEOUT_MS = 10_000;

/** The part of Aether's `GET /profile` response Synara reads. */
const AetherProfile = Schema.Struct({ email: TrimmedNonEmptyString });
const decodeAetherProfile = Schema.decodeUnknownExit(AetherProfile);

export type AetherHttpRequest = (request: OutboundHttpRequest) => Promise<OutboundHttpResponse>;

const defaultAetherHttpRequest: AetherHttpRequest = (request) => outboundHttp.request(request);

export const makeCheckAetherProviderStatus = (input: {
  /** Already validated by the settings schema; this check never re-interprets it. */
  readonly apiBaseUrl: AetherApiBaseUrl;
  /** Reads the API key from the server secret store; `null` when none is saved. */
  readonly readApiKey: Effect.Effect<string | null, unknown>;
  readonly request?: AetherHttpRequest;
}): Effect.Effect<ServerProviderStatus> =>
  Effect.gen(function* () {
    const checkedAt = new Date().toISOString();
    const status = (
      fields: Pick<ServerProviderStatus, "status" | "available" | "authStatus" | "message"> &
        Partial<Pick<ServerProviderStatus, "authType" | "authLabel">>,
    ): ServerProviderStatus => ({ provider: PROVIDER, checkedAt, ...fields });

    const apiKey = yield* input.readApiKey.pipe(Effect.result);
    if (apiKey._tag === "Failure") {
      return status({
        status: "error",
        available: false,
        authStatus: "unknown",
        message: "Could not read the saved Aether API key from Synara's secret store.",
      });
    }
    // A cloud provider has nothing to install: without a key it is available but signed out,
    // like a rejected key, so Settings reads "Needs sign-in" rather than "Unavailable".
    if (!apiKey.success) {
      return status({
        status: "warning",
        available: true,
        authStatus: "unauthenticated",
        message: "Aether is not configured. Add an Aether API key in Settings → Providers.",
      });
    }

    const url = resolveAetherApiUrl(input.apiBaseUrl, "/profile");
    const request = input.request ?? defaultAetherHttpRequest;
    const response = yield* Effect.tryPromise(() =>
      request({
        policy: {
          service: "aether-health",
          allowedOrigins: [url.origin],
          timeoutMs: AETHER_HEALTH_TIMEOUT_MS,
          maxRequestBytes: 1024,
          maxResponseBytes: 256 * 1024,
          // The bearer key must never follow a redirect to another origin.
          maxRedirects: 0,
          maxConcurrent: 2,
          maxQueued: 4,
          requirePublicAddress: true,
        },
        url,
        method: "GET",
        headers: { authorization: `Bearer ${apiKey.success}`, accept: "application/json" },
      }),
    ).pipe(Effect.result);

    if (response._tag === "Failure") {
      const cause = response.failure.cause;
      return status({
        status: "error",
        available: false,
        authStatus: "unknown",
        message:
          cause instanceof OutboundPolicyError
            ? "The Aether API base URL must be a public https:// address."
            : cause instanceof OutboundHttpError && cause.code === "timeout"
              ? "Timed out reaching the Aether API."
              : "Could not reach the Aether API.",
      });
    }

    const { status: httpStatus } = response.success;
    if (httpStatus === 401) {
      return status({
        status: "error",
        available: true,
        authStatus: "unauthenticated",
        message:
          "Aether rejected the API key (HTTP 401). Save a valid key in Settings → Providers.",
      });
    }
    if (httpStatus < 200 || httpStatus >= 300) {
      return status({
        status: "error",
        available: false,
        authStatus: "unknown",
        message: `Aether API health check failed: GET /profile returned HTTP ${httpStatus}.`,
      });
    }

    let body: unknown;
    try {
      body = decodeOutboundJson(response.success, { maxDepth: 16, maxNodes: 10_000 });
    } catch {
      body = undefined;
    }
    const profile = decodeAetherProfile(body);
    if (profile._tag === "Failure") {
      return status({
        status: "error",
        available: false,
        authStatus: "unknown",
        message: "Aether API health check failed: GET /profile returned an unexpected response.",
      });
    }

    return status({
      status: "ready",
      available: true,
      authStatus: "authenticated",
      authType: "apiKey",
      authLabel: profile.value.email,
      message: `Connected to Aether as ${profile.value.email}.`,
    });
  });

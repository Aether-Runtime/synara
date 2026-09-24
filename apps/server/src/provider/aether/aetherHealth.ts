// FILE: aetherHealth.ts
// Purpose: Network-only Aether health check: `GET {apiBaseUrl}/profile` with the stored API key.
// Layer: Server provider health. Aether runs in the cloud, so there is no executable
// lookup and no version probe. The request and the profile decode are the REST client's
// (aetherApiClient.ts); this module only turns the outcome into a provider status.

import type { AetherApiBaseUrl, ServerProviderStatus } from "@synara/contracts";
import { Effect } from "effect";

import { makeAetherApiClient, type AetherHttpRequest } from "./aetherApiClient.ts";
import { aetherApiErrorStatus, type AetherApiRequestError } from "./aetherApiErrors.ts";

const PROVIDER = "aether" as const;
const AETHER_HEALTH_TIMEOUT_MS = 10_000;

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

    const client = makeAetherApiClient({
      connection: Effect.succeed({ apiBaseUrl: input.apiBaseUrl, apiKey: apiKey.success }),
      timeoutMs: AETHER_HEALTH_TIMEOUT_MS,
      ...(input.request === undefined ? {} : { request: input.request }),
    });
    const profile = yield* client.getProfile().pipe(Effect.result);

    if (profile._tag === "Failure") {
      const error = profile.failure;
      if (error._tag === "AetherApiUnauthenticatedError") {
        return status({
          status: "error",
          available: true,
          authStatus: "unauthenticated",
          message:
            "Aether rejected the API key (HTTP 401). Save a valid key in Settings → Providers.",
        });
      }
      return status({
        status: "error",
        available: false,
        authStatus: "unknown",
        message: failureMessage(error),
      });
    }

    return status({
      status: "ready",
      available: true,
      authStatus: "authenticated",
      authType: "apiKey",
      authLabel: profile.success.email,
      message: `Connected to Aether as ${profile.success.email}.`,
    });
  });

/** Stable, user-facing text: never the transport detail or the server's message. */
function failureMessage(error: AetherApiRequestError): string {
  if (error._tag === "AetherApiTimeoutError") return "Timed out reaching the Aether API.";
  if (error._tag === "AetherApiTransportError") {
    return error.reason === "policy"
      ? "The Aether API base URL must be a public https:// address, or http:// on localhost."
      : "Could not reach the Aether API.";
  }
  const httpStatus = aetherApiErrorStatus(error);
  if (httpStatus === undefined || (httpStatus >= 200 && httpStatus <= 299)) {
    return "Aether API health check failed: GET /profile returned an unexpected response.";
  }
  return `Aether API health check failed: GET /profile returned HTTP ${httpStatus}.`;
}

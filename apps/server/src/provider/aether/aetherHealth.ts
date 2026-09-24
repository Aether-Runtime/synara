// FILE: aetherHealth.ts
// Purpose: Network-only Aether health check: `GET {apiBaseUrl}/profile` with the stored API key.
// Layer: Server provider health. Aether runs in the cloud, so there is no executable lookup and
// no version probe. The request goes through the `AetherApiClient` service; this module only
// turns its outcome into a provider status.

import type { ServerProviderStatus } from "@synara/contracts";
import { OutboundPolicyError } from "@synara/shared/outboundHttpPolicy";
import { Effect } from "effect";

import type { AetherApiClientShape } from "../Services/AetherApiClient.ts";

const PROVIDER = "aether" as const;

export const checkAetherProviderStatus = (
  client: Pick<AetherApiClientShape, "getProfile">,
): Effect.Effect<ServerProviderStatus> =>
  Effect.gen(function* () {
    const checkedAt = new Date().toISOString();
    const status = (
      fields: Pick<ServerProviderStatus, "status" | "available" | "authStatus" | "message"> &
        Partial<Pick<ServerProviderStatus, "authType" | "authLabel">>,
    ): ServerProviderStatus => ({ provider: PROVIDER, checkedAt, ...fields });
    const failed = (message: string) =>
      status({ status: "error", available: false, authStatus: "unknown", message });

    const profile = yield* client.getProfile().pipe(Effect.result);
    if (profile._tag === "Success") {
      const { email } = profile.success;
      return status({
        status: "ready",
        available: true,
        authStatus: "authenticated",
        authType: "apiKey",
        authLabel: email,
        message: `Connected to Aether as ${email}.`,
      });
    }

    // Stable, user-facing text: never the transport detail or the server's message.
    const error = profile.failure;
    switch (error._tag) {
      case "AetherApiNotConfiguredError":
        // A cloud provider has nothing to install: without a key it is available but signed
        // out, like a rejected key, so Settings reads "Needs sign-in" rather than "Unavailable".
        return error.reason === "missing-api-key"
          ? status({
              status: "warning",
              available: true,
              authStatus: "unauthenticated",
              message: "Aether is not configured. Add an Aether API key in Settings → Providers.",
            })
          : failed(error.message);
      case "AetherApiTimeoutError":
        return failed("Timed out reaching the Aether API.");
      case "AetherApiTransportError":
        return failed(
          error.cause instanceof OutboundPolicyError
            ? "The Aether API base URL must be a public https:// address, or http:// on localhost."
            : "Could not reach the Aether API.",
        );
      case "AetherApiHttpError":
        return error.status === 401
          ? status({
              status: "error",
              available: true,
              authStatus: "unauthenticated",
              message:
                "Aether rejected the API key (HTTP 401). Save a valid key in Settings → Providers.",
            })
          : failed(`Aether API health check failed: GET /profile returned HTTP ${error.status}.`);
      case "AetherApiPaymentRequiredError":
        return failed("Aether API health check failed: GET /profile returned HTTP 402.");
      case "AetherApiConflictError":
        return failed("Aether API health check failed: GET /profile returned HTTP 409.");
      case "AetherApiDecodeError":
        return failed(
          error.status >= 200 && error.status <= 299
            ? "Aether API health check failed: GET /profile returned an unexpected response."
            : `Aether API health check failed: GET /profile returned HTTP ${error.status}.`,
        );
    }
  });

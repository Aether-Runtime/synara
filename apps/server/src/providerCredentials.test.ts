import { Effect, Layer } from "effect";
import { describe, expect, it } from "vitest";

import {
  PROVIDER_SECRETS,
  ProviderCredentials,
  resolveProviderSecret,
  type ProviderCredentialsShape,
} from "./providerCredentials";

describe("resolveProviderSecret", () => {
  it("reads ProviderCredentials from the Effect service context", async () => {
    const credentials: ProviderCredentialsShape = {
      getSecret: (provider) => Effect.succeed(provider === "aether" ? "aether-key" : "secret"),
      replaceSecret: () => Effect.void,
      isSecretConfigured: () => Effect.succeed(true),
    };
    const provide = Effect.provide(Layer.succeed(ProviderCredentials, credentials));

    expect(await Effect.runPromise(resolveProviderSecret("opencode").pipe(provide))).toBe("secret");
    expect(await Effect.runPromise(resolveProviderSecret("aether").pipe(provide))).toBe(
      "aether-key",
    );
  });

  it("keeps the OpenCode secret name stable across the generalization", () => {
    expect(PROVIDER_SECRETS.opencode.secretName).toBe("provider-opencode-server-password");
    expect(PROVIDER_SECRETS.aether.secretName).toBe("provider-aether-api-key");
  });
});

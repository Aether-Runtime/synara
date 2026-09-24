import { AetherApiBaseUrl } from "@synara/contracts";
import type { OutboundHttpRequest, OutboundHttpResponse } from "@synara/shared/outboundHttp";
import { OutboundHttpError } from "@synara/shared/outboundHttp";
import { Effect } from "effect";
import { describe, expect, it } from "vitest";

import type { AetherHttpRequest } from "./aetherApiClient.ts";
import { makeCheckAetherProviderStatus } from "./aetherHealth.ts";

const API_BASE_URL = AetherApiBaseUrl.makeUnsafe("https://api.runaether.dev");

function fakeHttp(respond: (request: OutboundHttpRequest) => OutboundHttpResponse | Error) {
  const requests: OutboundHttpRequest[] = [];
  const request: AetherHttpRequest = async (input) => {
    requests.push(input);
    const result = respond(input);
    if (result instanceof Error) throw result;
    return result;
  };
  return { request, requests };
}

function jsonResponse(status: number, body: unknown): OutboundHttpResponse {
  return {
    status,
    headers: new Headers({ "content-type": "application/json" }),
    body: new TextEncoder().encode(JSON.stringify(body)),
    url: `${API_BASE_URL}/profile`,
  };
}

/** A `ProfileResponse` as the OpenAPI declares it. */
function profileBody(email: string, extra: Record<string, unknown> = {}) {
  return {
    id: "user_1",
    email,
    display_name: "Dev",
    onboarding_completed: true,
    created_at: "2026-09-01T00:00:00Z",
    updated_at: "2026-09-01T00:00:00Z",
    ...extra,
  };
}

const check = (input: {
  readonly apiKey: string | null;
  readonly http: ReturnType<typeof fakeHttp>;
  readonly apiBaseUrl?: AetherApiBaseUrl;
}) =>
  Effect.runPromise(
    makeCheckAetherProviderStatus({
      apiBaseUrl: input.apiBaseUrl ?? API_BASE_URL,
      readApiKey: Effect.succeed(input.apiKey),
      request: input.http.request,
    }),
  );

describe("Aether health check", () => {
  it("reports not configured without calling the API when no key is saved", async () => {
    const http = fakeHttp(() => jsonResponse(200, profileBody("unused@example.com")));

    const status = await check({ apiKey: null, http });

    expect(http.requests).toEqual([]);
    // Nothing to install: available but signed out, the same shape as a rejected key.
    expect(status).toMatchObject({
      provider: "aether",
      status: "warning",
      available: true,
      authStatus: "unauthenticated",
      message: "Aether is not configured. Add an Aether API key in Settings → Providers.",
    });
  });

  it("calls GET /profile with the bearer key, pinned to the configured origin", async () => {
    const http = fakeHttp(() =>
      jsonResponse(200, profileBody("dev@example.com", { added_by_a_newer_server: true })),
    );

    const status = await check({ apiKey: "aether-secret", http });

    expect(http.requests).toHaveLength(1);
    const [request] = http.requests;
    expect(String(request?.url)).toBe("https://api.runaether.dev/profile");
    expect(request?.method).toBe("GET");
    expect(new Headers(request?.headers).get("authorization")).toBe("Bearer aether-secret");
    expect(request?.policy).toMatchObject({
      allowedOrigins: ["https://api.runaether.dev"],
      maxRedirects: 0,
      requirePublicAddress: true,
    });
    expect(status).toMatchObject({
      provider: "aether",
      status: "ready",
      available: true,
      authStatus: "authenticated",
      authType: "apiKey",
      authLabel: "dev@example.com",
      message: "Connected to Aether as dev@example.com.",
    });
    expect(status.version).toBeUndefined();
    expect(JSON.stringify(status)).not.toContain("aether-secret");
  });

  it("reports unauthenticated on HTTP 401", async () => {
    const http = fakeHttp(() => jsonResponse(401, { error: "invalid api key" }));

    const status = await check({ apiKey: "revoked", http });

    expect(status).toMatchObject({
      status: "error",
      available: true,
      authStatus: "unauthenticated",
      message: "Aether rejected the API key (HTTP 401). Save a valid key in Settings → Providers.",
    });
  });

  it("reports other HTTP statuses as errors with a stable message", async () => {
    const http = fakeHttp(() => jsonResponse(500, { error: "internal detail that may change" }));

    const status = await check({ apiKey: "aether-secret", http });

    expect(status).toMatchObject({
      status: "error",
      available: false,
      authStatus: "unknown",
      message: "Aether API health check failed: GET /profile returned HTTP 500.",
    });
  });

  it("rejects a 2xx profile that does not decode", async () => {
    const http = fakeHttp(() => jsonResponse(200, profileBody("")));

    const status = await check({ apiKey: "aether-secret", http });

    expect(status).toMatchObject({
      status: "error",
      available: false,
      message: "Aether API health check failed: GET /profile returned an unexpected response.",
    });
  });

  it("reports network failures without leaking transport detail", async () => {
    const timeout = fakeHttp(
      () => new OutboundHttpError("timeout", "Outbound request exceeded its 10000ms deadline."),
    );
    const refused = fakeHttp(() => new Error("connect ECONNREFUSED 10.0.0.1:443"));

    expect(await check({ apiKey: "k", http: timeout })).toMatchObject({
      status: "error",
      available: false,
      message: "Timed out reaching the Aether API.",
    });
    expect(await check({ apiKey: "k", http: refused })).toMatchObject({
      status: "error",
      available: false,
      message: "Could not reach the Aether API.",
    });
  });

  it("keeps a base path prefix from the configured URL", async () => {
    const http = fakeHttp(() => jsonResponse(200, profileBody("dev@example.com")));

    await check({
      apiKey: "k",
      http,
      apiBaseUrl: AetherApiBaseUrl.makeUnsafe("https://proxy.example.com/aether/"),
    });

    expect(String(http.requests[0]?.url)).toBe("https://proxy.example.com/aether/profile");
    expect(http.requests[0]?.policy.allowedOrigins).toEqual(["https://proxy.example.com"]);
  });

  it("reaches a local Aether API over loopback http, and only that one over http", async () => {
    const http = fakeHttp(() => jsonResponse(200, profileBody("dev@example.com")));

    await check({
      apiKey: "k",
      http,
      apiBaseUrl: AetherApiBaseUrl.makeUnsafe("http://localhost:8787"),
    });
    await check({ apiKey: "k", http });

    expect(String(http.requests[0]?.url)).toBe("http://localhost:8787/profile");
    expect(http.requests[0]?.policy).toMatchObject({
      allowedOrigins: ["http://localhost:8787"],
      allowLoopbackHttp: true,
      requirePublicAddress: true,
    });
    expect(http.requests[1]?.policy.allowLoopbackHttp).toBe(false);
  });

  it("reports a failing secret store without calling the API", async () => {
    const http = fakeHttp(() => jsonResponse(200, profileBody("dev@example.com")));
    const unreadable = await Effect.runPromise(
      makeCheckAetherProviderStatus({
        apiBaseUrl: API_BASE_URL,
        readApiKey: Effect.fail(new Error("EACCES")),
        request: http.request,
      }),
    );
    expect(unreadable).toMatchObject({
      status: "error",
      available: false,
      message: "Could not read the saved Aether API key from Synara's secret store.",
    });
    expect(http.requests).toEqual([]);
  });
});

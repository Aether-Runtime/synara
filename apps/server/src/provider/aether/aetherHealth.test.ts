import type { OutboundHttpRequest, OutboundHttpResponse } from "@synara/shared/outboundHttp";
import { OutboundHttpError } from "@synara/shared/outboundHttp";
import { Effect } from "effect";
import { describe, expect, it } from "vitest";

import {
  makeCheckAetherProviderStatus,
  resolveAetherApiUrl,
  type AetherHttpRequest,
} from "./aetherHealth.ts";

const API_BASE_URL = "https://api.runaether.dev";

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

const check = (input: {
  readonly apiKey: string | null;
  readonly http: ReturnType<typeof fakeHttp>;
  readonly apiBaseUrl?: string;
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
    const http = fakeHttp(() => jsonResponse(200, { email: "unused@example.com" }));

    const status = await check({ apiKey: null, http });

    expect(http.requests).toEqual([]);
    expect(status).toMatchObject({
      provider: "aether",
      status: "warning",
      available: false,
      authStatus: "unauthenticated",
      message: "Aether is not configured. Add an Aether API key in Settings → Providers.",
    });
  });

  it("calls GET /profile with the bearer key, pinned to the configured origin", async () => {
    const http = fakeHttp(() => jsonResponse(200, { email: "dev@example.com", name: "Dev" }));

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
    const http = fakeHttp(() => jsonResponse(200, { email: "" }));

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

  it("reports a malformed base URL and a failing secret store", async () => {
    const http = fakeHttp(() => jsonResponse(200, { email: "dev@example.com" }));

    expect(await check({ apiKey: "k", http, apiBaseUrl: "not a url" })).toMatchObject({
      status: "error",
      message: "The Aether API base URL is not a valid URL.",
    });
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

describe("resolveAetherApiUrl", () => {
  it("keeps a base path prefix and tolerates trailing slashes", () => {
    expect(String(resolveAetherApiUrl("https://api.runaether.dev", "/profile"))).toBe(
      "https://api.runaether.dev/profile",
    );
    expect(String(resolveAetherApiUrl("https://proxy.example.com/aether/", "/profile"))).toBe(
      "https://proxy.example.com/aether/profile",
    );
  });
});

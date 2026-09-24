// FILE: aetherApiErrors.ts
// Purpose: The Aether REST client's error union.
// Layer: Server provider boundary (Aether).
//
// Every failed call is one of these tagged errors; nothing is thrown as a string or replaced by
// a fallback value. Error bodies are decoded only where a caller branches on them (402, 409).
// Otherwise the body's `error` text is kept as `serverMessage` when the body is a readable error
// body, and left out when it is not (a proxy's HTML 502 is still a 502).

import { Schema } from "effect";

import { AetherConflictBody } from "./aetherApiSchemas.ts";

/** No request was made: no API key is saved, or the key or settings could not be read. */
export class AetherApiNotConfiguredError extends Schema.TaggedErrorClass<AetherApiNotConfiguredError>()(
  "AetherApiNotConfiguredError",
  {
    reason: Schema.Literals(["missing-api-key", "unreadable-api-key", "unreadable-settings"]),
    cause: Schema.optional(Schema.Defect),
  },
) {
  override get message(): string {
    return this.reason === "missing-api-key"
      ? "No Aether API key is saved. Add one in Settings → Providers."
      : this.reason === "unreadable-api-key"
        ? "Could not read the saved Aether API key from Synara's secret store."
        : "Could not read the Aether API base URL from Synara's settings.";
  }
}

/** No HTTP response: DNS, connection, TLS, the outbound policy, a redirect, an oversized body. */
export class AetherApiTransportError extends Schema.TaggedErrorClass<AetherApiTransportError>()(
  "AetherApiTransportError",
  {
    endpoint: Schema.String,
    cause: Schema.Defect,
  },
) {
  override get message(): string {
    const cause = this.cause instanceof Error ? `: ${this.cause.message}` : "";
    return `Aether API request failed before a response (${this.endpoint})${cause}`;
  }
}

/** The request, including its response body, did not finish within the request timeout. */
export class AetherApiTimeoutError extends Schema.TaggedErrorClass<AetherApiTimeoutError>()(
  "AetherApiTimeoutError",
  {
    endpoint: Schema.String,
    timeoutMs: Schema.Number,
  },
) {
  override get message(): string {
    return `Aether API request timed out after ${this.timeoutMs}ms (${this.endpoint}).`;
  }
}

/** A body did not match its schema, or was not JSON: a contract break. */
export class AetherApiDecodeError extends Schema.TaggedErrorClass<AetherApiDecodeError>()(
  "AetherApiDecodeError",
  {
    endpoint: Schema.String,
    status: Schema.Number,
    /** Effect's issue message, which names the failing path. */
    detail: Schema.String,
    cause: Schema.Defect,
  },
) {
  override get message(): string {
    return `Aether API returned an unexpected payload (${this.endpoint}, HTTP ${this.status}): ${this.detail}`;
  }
}

/** Any non-2xx status without its own error (401, 403, 404, 422, 5xx, …). */
export class AetherApiHttpError extends Schema.TaggedErrorClass<AetherApiHttpError>()(
  "AetherApiHttpError",
  {
    endpoint: Schema.String,
    status: Schema.Number,
    /** The error body's `error`; absent when the body is not a readable error body. */
    serverMessage: Schema.optional(Schema.String),
  },
) {
  override get message(): string {
    return `Aether API returned HTTP ${this.status} (${this.endpoint})${
      this.serverMessage === undefined ? "." : `: ${this.serverMessage}`
    }`;
  }
}

/** 402: the workspace runtime was not admitted for the account's plan. */
export class AetherApiPaymentRequiredError extends Schema.TaggedErrorClass<AetherApiPaymentRequiredError>()(
  "AetherApiPaymentRequiredError",
  {
    endpoint: Schema.String,
    code: Schema.String,
    tier: Schema.String,
    vmSize: Schema.String,
    serverMessage: Schema.String,
  },
) {
  override get message(): string {
    return `Aether requires payment (${this.endpoint}, ${this.code}, tier ${this.tier}, VM ${this.vmSize}): ${this.serverMessage}`;
  }
}

/** 409 from an endpoint that declares one, with its decoded body (see `AetherConflictBodies`). */
export class AetherApiConflictError extends Schema.TaggedErrorClass<AetherApiConflictError>()(
  "AetherApiConflictError",
  {
    endpoint: Schema.String,
    body: AetherConflictBody,
  },
) {
  override get message(): string {
    return `Aether API conflict (${this.endpoint}): ${this.body.error}`;
  }
}

/** Every error the Aether REST client produces. */
export type AetherApiError =
  | AetherApiNotConfiguredError
  | AetherApiTransportError
  | AetherApiTimeoutError
  | AetherApiDecodeError
  | AetherApiHttpError
  | AetherApiPaymentRequiredError
  | AetherApiConflictError;

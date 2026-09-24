// FILE: aetherApiErrors.ts
// Purpose: The Aether REST client's error union, and the formatter that turns a schema decode
// failure into a detail naming the offending field.
// Layer: Server provider boundary (Aether).
//
// Every failure of an Aether API call is one of these tagged errors; nothing is thrown as a
// string or replaced by a fallback value. The status decides the error. Error bodies are decoded
// only where the client branches on them (402, 403, 409); for other statuses the body's
// `error` text is kept as `serverMessage` when it decodes, and left out when it does not (a
// proxy's HTML 502 is still a 502).

import { Schema, SchemaAST, SchemaIssue } from "effect";

// ---------------------------------------------------------------------------
// Configuration, transport, timeout, encode and decode
// ---------------------------------------------------------------------------

/** No request was made: the API key is missing or unreadable, or settings could not be read. */
export class AetherApiNotConfiguredError extends Schema.TaggedErrorClass<AetherApiNotConfiguredError>()(
  "AetherApiNotConfiguredError",
  {
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect),
  },
) {
  override get message(): string {
    return `Aether API is not configured: ${this.detail}`;
  }
}

/**
 * The request did not produce an HTTP response: DNS, connection, TLS, the outbound policy
 * (`reason: "policy"`), a redirect (never followed), or an oversized response. `reason` is the
 * outbound error code.
 */
export class AetherApiTransportError extends Schema.TaggedErrorClass<AetherApiTransportError>()(
  "AetherApiTransportError",
  {
    endpoint: Schema.String,
    reason: Schema.String,
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect),
  },
) {
  override get message(): string {
    return `Aether API transport error (${this.endpoint}, ${this.reason}): ${this.detail}`;
  }
}

/** The exchange (request, response body and decode) did not finish within the deadline. */
export class AetherApiTimeoutError extends Schema.TaggedErrorClass<AetherApiTimeoutError>()(
  "AetherApiTimeoutError",
  {
    endpoint: Schema.String,
    timeoutMs: Schema.Number,
    cause: Schema.optional(Schema.Defect),
  },
) {
  override get message(): string {
    return `Aether API request timed out after ${this.timeoutMs}ms (${this.endpoint}).`;
  }
}

/** The caller's request did not match the request schema; nothing was sent. */
export class AetherApiEncodeError extends Schema.TaggedErrorClass<AetherApiEncodeError>()(
  "AetherApiEncodeError",
  {
    endpoint: Schema.String,
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect),
  },
) {
  override get message(): string {
    return `Aether API request is invalid (${this.endpoint}): ${this.detail}`;
  }
}

/**
 * A response body did not match its schema: a contract break, reported with the field that
 * failed. `status` is the HTTP status of the response that failed to decode.
 */
export class AetherApiDecodeError extends Schema.TaggedErrorClass<AetherApiDecodeError>()(
  "AetherApiDecodeError",
  {
    endpoint: Schema.String,
    status: Schema.Number,
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect),
  },
) {
  override get message(): string {
    return `Aether API returned an unexpected payload (${this.endpoint}, HTTP ${this.status}): ${this.detail}`;
  }
}

// ---------------------------------------------------------------------------
// HTTP statuses
// ---------------------------------------------------------------------------

/** 401: the API key is missing, revoked or wrong. */
export class AetherApiUnauthenticatedError extends Schema.TaggedErrorClass<AetherApiUnauthenticatedError>()(
  "AetherApiUnauthenticatedError",
  {
    endpoint: Schema.String,
    status: Schema.Literal(401),
    serverMessage: Schema.optional(Schema.String),
  },
) {
  override get message(): string {
    return `Aether rejected the API key (${this.endpoint}, HTTP 401).`;
  }
}

/** 402: the workspace runtime was not admitted for the account's plan. */
export class AetherApiPaymentRequiredError extends Schema.TaggedErrorClass<AetherApiPaymentRequiredError>()(
  "AetherApiPaymentRequiredError",
  {
    endpoint: Schema.String,
    status: Schema.Literal(402),
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

export const AetherForbiddenReason = Schema.Union([
  /** The workspace runtime was not admitted (like 402, reported as 403 by some routes). */
  Schema.Struct({
    kind: Schema.Literal("admission"),
    code: Schema.String,
    tier: Schema.String,
    vmSize: Schema.String,
  }),
  /** The organization refused the caller. */
  Schema.Struct({
    kind: Schema.Literal("org"),
    code: Schema.Literals([
      "org_membership_required",
      "org_role_insufficient",
      "org_suspended",
      "account_deleting",
    ]),
  }),
  /** The generic error body, where the route declares it. */
  Schema.Struct({ kind: Schema.Literal("other"), code: Schema.optional(Schema.String) }),
]);
export type AetherForbiddenReason = typeof AetherForbiddenReason.Type;

/** 403: runtime admission or organization authorization refused the request. */
export class AetherApiForbiddenError extends Schema.TaggedErrorClass<AetherApiForbiddenError>()(
  "AetherApiForbiddenError",
  {
    endpoint: Schema.String,
    status: Schema.Literal(403),
    reason: AetherForbiddenReason,
    serverMessage: Schema.String,
  },
) {
  override get message(): string {
    return `Aether refused the request (${this.endpoint}, HTTP 403, ${this.reason.kind}): ${this.serverMessage}`;
  }
}

/** 404: the task, project, turn or workspace does not exist or is not visible to this key. */
export class AetherApiNotFoundError extends Schema.TaggedErrorClass<AetherApiNotFoundError>()(
  "AetherApiNotFoundError",
  {
    endpoint: Schema.String,
    status: Schema.Literal(404),
    serverMessage: Schema.optional(Schema.String),
  },
) {
  override get message(): string {
    return `Aether resource not found (${this.endpoint}, HTTP 404).`;
  }
}

/** 408: Aether gave up waiting for the request (not the client deadline). */
export class AetherApiRequestTimeoutError extends Schema.TaggedErrorClass<AetherApiRequestTimeoutError>()(
  "AetherApiRequestTimeoutError",
  {
    endpoint: Schema.String,
    status: Schema.Literal(408),
    serverMessage: Schema.optional(Schema.String),
  },
) {
  override get message(): string {
    return `Aether timed out waiting for the request (${this.endpoint}, HTTP 408).`;
  }
}

/** 413: the request body (prompt, attachments) is larger than Aether accepts. */
export class AetherApiPayloadTooLargeError extends Schema.TaggedErrorClass<AetherApiPayloadTooLargeError>()(
  "AetherApiPayloadTooLargeError",
  {
    endpoint: Schema.String,
    status: Schema.Literal(413),
    serverMessage: Schema.optional(Schema.String),
  },
) {
  override get message(): string {
    return `Aether rejected the request as too large (${this.endpoint}, HTTP 413).`;
  }
}

/** 400 or 422: Aether's request validation failed. `errors` locates each problem. */
export class AetherApiValidationError extends Schema.TaggedErrorClass<AetherApiValidationError>()(
  "AetherApiValidationError",
  {
    endpoint: Schema.String,
    status: Schema.Literals([400, 422]),
    serverMessage: Schema.optional(Schema.String),
    errors: Schema.Array(
      Schema.Struct({
        location: Schema.optional(Schema.String),
        message: Schema.optional(Schema.String),
      }),
    ),
  },
) {
  override get message(): string {
    return `Aether rejected the request (${this.endpoint}, HTTP ${this.status})${
      this.serverMessage === undefined ? "." : `: ${this.serverMessage}`
    }`;
  }
}

/** 5xx: Aether (or a gateway in front of it) failed. */
export class AetherApiServerError extends Schema.TaggedErrorClass<AetherApiServerError>()(
  "AetherApiServerError",
  {
    endpoint: Schema.String,
    status: Schema.Number,
    serverMessage: Schema.optional(Schema.String),
  },
) {
  override get message(): string {
    return `Aether API server error (${this.endpoint}, HTTP ${this.status}).`;
  }
}

/** A status the endpoint does not declare (for example 429, or 409 on a read). */
export class AetherApiUnexpectedStatusError extends Schema.TaggedErrorClass<AetherApiUnexpectedStatusError>()(
  "AetherApiUnexpectedStatusError",
  {
    endpoint: Schema.String,
    status: Schema.Number,
    serverMessage: Schema.optional(Schema.String),
  },
) {
  override get message(): string {
    return `Aether API returned an undeclared status (${this.endpoint}, HTTP ${this.status}).`;
  }
}

// ---------------------------------------------------------------------------
// 409 conflicts, one discriminated union per endpoint
// ---------------------------------------------------------------------------

export const AetherCreateTaskConflict = Schema.Struct({
  /** The idempotency key was already used for a different request; nothing was created. */
  kind: Schema.Literal("idempotency_key_reused"),
});
export type AetherCreateTaskConflict = typeof AetherCreateTaskConflict.Type;

/** 409 from `POST /tasks`. */
export class AetherApiCreateTaskConflictError extends Schema.TaggedErrorClass<AetherApiCreateTaskConflictError>()(
  "AetherApiCreateTaskConflictError",
  {
    endpoint: Schema.String,
    status: Schema.Literal(409),
    conflict: AetherCreateTaskConflict,
    serverMessage: Schema.String,
  },
) {
  override get message(): string {
    return `Aether refused to create the task (${this.conflict.kind}): ${this.serverMessage}`;
  }
}

export const AetherRespondConflict = Schema.Union([
  /** The answered tool is no longer the pending one. `currentToolId` names the pending one. */
  Schema.Struct({
    kind: Schema.Literal("stale_pending_input"),
    currentToolId: Schema.optional(Schema.String),
  }),
  /** The task's creator was deleted; nobody can continue the task. */
  Schema.Struct({ kind: Schema.Literal("task_creator_deleted") }),
  /** The response does not fit the pending input, whose kind is named. */
  Schema.Struct({
    kind: Schema.Literal("awaiting_input_kind"),
    awaitingInputKind: Schema.Literals(["message", "questions", "plan"]),
    code: Schema.optional(Schema.String),
  }),
  /** The generic error body the route also declares. */
  Schema.Struct({ kind: Schema.Literal("other"), code: Schema.optional(Schema.String) }),
]);
export type AetherRespondConflict = typeof AetherRespondConflict.Type;

/** 409 from `POST /tasks/{taskID}/respond`. */
export class AetherApiRespondConflictError extends Schema.TaggedErrorClass<AetherApiRespondConflictError>()(
  "AetherApiRespondConflictError",
  {
    endpoint: Schema.String,
    status: Schema.Literal(409),
    conflict: AetherRespondConflict,
    serverMessage: Schema.String,
  },
) {
  override get message(): string {
    return `Aether refused the response (${this.conflict.kind}): ${this.serverMessage}`;
  }
}

export const AetherStopConflict = Schema.Union([
  /** The task only has queued work; stopping it must discard the queue explicitly. */
  Schema.Struct({ kind: Schema.Literal("queued_stop_requires_discard") }),
  Schema.Struct({ kind: Schema.Literal("other"), code: Schema.optional(Schema.String) }),
]);
export type AetherStopConflict = typeof AetherStopConflict.Type;

/** 409 from `POST /tasks/{taskID}/stop`. */
export class AetherApiStopConflictError extends Schema.TaggedErrorClass<AetherApiStopConflictError>()(
  "AetherApiStopConflictError",
  {
    endpoint: Schema.String,
    status: Schema.Literal(409),
    conflict: AetherStopConflict,
    serverMessage: Schema.String,
  },
) {
  override get message(): string {
    return `Aether refused to stop the task (${this.conflict.kind}): ${this.serverMessage}`;
  }
}

export const AetherConnectConflict = Schema.Union([
  /** A lifecycle operation is settling; ask again after `retryAfterMs`. */
  Schema.Struct({
    kind: Schema.Literal("transitional"),
    workspaceId: Schema.String,
    retryAfterMs: Schema.Number,
  }),
  /** Not running; a connect with `start: true` would start it. */
  Schema.Struct({ kind: Schema.Literal("startable"), workspaceId: Schema.String }),
  /** Not running, and connecting cannot change that. */
  Schema.Struct({
    kind: Schema.Literal("not_connectable"),
    workspaceId: Schema.String,
    displayState: Schema.String,
  }),
  /** The task's creator was deleted. This body carries a `code`, not a `kind`. */
  Schema.Struct({ kind: Schema.Literal("task_creator_deleted") }),
]);
export type AetherConnectConflict = typeof AetherConnectConflict.Type;

/** 409 from `POST /workspaces/{workspaceID}/connect`. */
export class AetherApiConnectConflictError extends Schema.TaggedErrorClass<AetherApiConnectConflictError>()(
  "AetherApiConnectConflictError",
  {
    endpoint: Schema.String,
    status: Schema.Literal(409),
    conflict: AetherConnectConflict,
    serverMessage: Schema.String,
  },
) {
  override get message(): string {
    return `Aether cannot connect the workspace (${this.conflict.kind}): ${this.serverMessage}`;
  }
}

// ---------------------------------------------------------------------------
// Unions
// ---------------------------------------------------------------------------

/** Errors any operation can fail with. */
export type AetherApiRequestError =
  | AetherApiNotConfiguredError
  | AetherApiTransportError
  | AetherApiTimeoutError
  | AetherApiEncodeError
  | AetherApiDecodeError
  | AetherApiUnauthenticatedError
  | AetherApiPaymentRequiredError
  | AetherApiForbiddenError
  | AetherApiNotFoundError
  | AetherApiRequestTimeoutError
  | AetherApiPayloadTooLargeError
  | AetherApiValidationError
  | AetherApiServerError
  | AetherApiUnexpectedStatusError;

export type AetherApiConflictError =
  | AetherApiCreateTaskConflictError
  | AetherApiRespondConflictError
  | AetherApiStopConflictError
  | AetherApiConnectConflictError;

/** Every error the Aether REST client produces. */
export type AetherApiError = AetherApiRequestError | AetherApiConflictError;

/** The HTTP status behind an error, or `undefined` when no response arrived. */
export function aetherApiErrorStatus(error: AetherApiError): number | undefined {
  switch (error._tag) {
    case "AetherApiNotConfiguredError":
    case "AetherApiTransportError":
    case "AetherApiTimeoutError":
    case "AetherApiEncodeError":
      return undefined;
    default:
      return error.status;
  }
}

// ---------------------------------------------------------------------------
// Decode issue formatting
// ---------------------------------------------------------------------------

const standardFormatter = SchemaIssue.makeFormatterStandardSchemaV1();

interface IssueLine {
  readonly path: ReadonlyArray<PropertyKey>;
  readonly message: string;
}

/**
 * Formats a schema issue as `message at ["a"]["b"]` lines. Unlike Effect's default formatter,
 * a union that fails on its discriminator (an unknown `status`, `kind`, `variant`, …) names that
 * field instead of printing every member it expected.
 */
export function formatAetherSchemaIssue(issue: SchemaIssue.Issue): string {
  return issueLines(issue, [])
    .map((line) =>
      line.path.length === 0
        ? line.message
        : `${line.message} at ${line.path.map((key) => `[${JSON.stringify(String(key))}]`).join("")}`,
    )
    .join("; ");
}

function issueLines(issue: SchemaIssue.Issue, path: ReadonlyArray<PropertyKey>): IssueLine[] {
  switch (issue._tag) {
    case "Pointer":
      return issueLines(issue.issue, [...path, ...issue.path]);
    case "Composite":
      return issue.issues.flatMap((inner) => issueLines(inner, path));
    case "Encoding":
      return issueLines(issue.issue, path);
    case "AnyOf": {
      const discriminator = discriminatorLine(issue, path);
      if (discriminator !== undefined) return [discriminator];
      if (issue.issues.length > 0) {
        // Report only the members the input's literal keys select (the `success: true` variant
        // for a `success: true` frame), not every member that was tried.
        const selected = issue.issues.filter(
          (inner) => inner._tag === "Composite" && literalsMatch(inner.ast, issue.actual),
        );
        return (selected.length > 0 ? selected : issue.issues).flatMap((inner) =>
          issueLines(inner, path),
        );
      }
      break;
    }
    default:
      break;
  }
  return standardFormatter(issue).issues.map((line) => ({
    path: [
      ...path,
      ...(line.path ?? []).map((segment) =>
        typeof segment === "object" && segment !== null ? segment.key : segment,
      ),
    ],
    message: line.message,
  }));
}

/**
 * When every member of a failed union is an object with a literal-typed key (the
 * discriminator), and the input's value for it is missing or not one of the members' literals,
 * that is the failure to report.
 */
function discriminatorLine(
  issue: SchemaIssue.AnyOf,
  path: ReadonlyArray<PropertyKey>,
): IssueLine | undefined {
  const input = issue.actual;
  if (typeof input !== "object" || input === null || Array.isArray(input)) return undefined;
  const record = input as Record<PropertyKey, unknown>;
  for (const [key, { literals, required }] of discriminators(issue.ast)) {
    if (!Object.hasOwn(record, key)) {
      if (required) return { path: [...path, key], message: "Missing key" };
      continue;
    }
    const value = record[key];
    if (!literals.some((literal) => literal === value)) {
      return {
        path: [...path, key],
        message: `Expected ${literals.map((literal) => JSON.stringify(literal)).join(" | ")}, got ${JSON.stringify(value)}`,
      };
    }
  }
  return undefined;
}

interface Discriminator {
  readonly literals: ReadonlyArray<SchemaAST.LiteralValue>;
  readonly required: boolean;
}

function discriminators(union: SchemaAST.Union): Map<PropertyKey, Discriminator> {
  const members = flattenUnion(union);
  const result = new Map<PropertyKey, Discriminator>();
  if (members.length === 0 || members.some((member) => member._tag !== "Objects")) return result;
  const objects = members as ReadonlyArray<SchemaAST.Objects>;
  const first = objects[0];
  if (first === undefined) return result;
  for (const { name } of first.propertySignatures) {
    const literals: SchemaAST.LiteralValue[] = [];
    let required = true;
    let everyMemberHasLiteral = true;
    for (const member of objects) {
      const property = member.propertySignatures.find((candidate) => candidate.name === name);
      const type = property === undefined ? undefined : SchemaAST.toEncoded(property.type);
      if (property === undefined || type === undefined || type._tag !== "Literal") {
        everyMemberHasLiteral = false;
        break;
      }
      if (SchemaAST.isOptional(property.type)) required = false;
      if (!literals.includes(type.literal)) literals.push(type.literal);
    }
    if (everyMemberHasLiteral) result.set(name, { literals, required });
  }
  return result;
}

/** Whether every literal-typed key of an object member agrees with the input. */
function literalsMatch(member: SchemaAST.AST, input: unknown): boolean {
  const encoded = SchemaAST.toEncoded(member);
  if (encoded._tag !== "Objects") return false;
  if (typeof input !== "object" || input === null || Array.isArray(input)) return false;
  const record = input as Record<PropertyKey, unknown>;
  return encoded.propertySignatures.every((property) => {
    const type = SchemaAST.toEncoded(property.type);
    if (type._tag !== "Literal") return true;
    if (!Object.hasOwn(record, property.name)) return SchemaAST.isOptional(property.type);
    return record[property.name] === type.literal;
  });
}

function flattenUnion(ast: SchemaAST.AST): ReadonlyArray<SchemaAST.AST> {
  const encoded = SchemaAST.toEncoded(ast);
  return encoded._tag === "Union" ? encoded.types.flatMap(flattenUnion) : [encoded];
}

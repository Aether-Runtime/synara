// FILE: aetherWorkspaceProtocol.ts
// Purpose: The subset of Aether's workspace WebSocket protocol Synara speaks: git diff, files
// read, the user-activity keepalive, and the server's error frame.
// Layer: Server provider boundary (Aether). Source of truth: Aether's `workspace-protocol`
// messages and `domain-types/git` (2026-09-24). Terminal, agent, ports and the other file and
// git operations are out of scope.
//
// Frames the client sends are encoded strictly (the workspace service rejects unknown keys).
// Frames it receives follow the REST rules: unknown keys are ignored, and a missing required
// field or an unknown discriminator value (`success`, file `kind`, line `kind`) fails the
// decode naming the field.

import { NonNegativeInt, PositiveInt } from "@synara/contracts";
import { Schema } from "effect";

const NonEmptyString = Schema.String.check(Schema.isMinLength(1));

/**
 * The workspace's interactive idle lease is renewed only by `user_activity`, and the protocol
 * expects at most one ping per this interval; the idle monitor tolerates one missed interval.
 */
export const AETHER_ACTIVITY_PING_INTERVAL_MS = 30_000;

// ---------------------------------------------------------------------------
// Git diff
// ---------------------------------------------------------------------------

export const AetherGitDiffMode = Schema.Literals(["main", "lastCommit"]);
export type AetherGitDiffMode = typeof AetherGitDiffMode.Type;

const diffLineFields = {
  text: Schema.String,
  noTrailingNewline: Schema.optionalKey(Schema.Boolean),
} as const;

/** One diff line, numbered only on the sides where it exists. */
export const AetherDiffLine = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("add"),
    oldLine: Schema.Null,
    newLine: PositiveInt,
    ...diffLineFields,
  }),
  Schema.Struct({
    kind: Schema.Literal("del"),
    oldLine: PositiveInt,
    newLine: Schema.Null,
    ...diffLineFields,
  }),
  Schema.Struct({
    kind: Schema.Literal("context"),
    oldLine: PositiveInt,
    newLine: PositiveInt,
    ...diffLineFields,
  }),
]);
export type AetherDiffLine = typeof AetherDiffLine.Type;

export const AetherGitDiffHunk = Schema.Struct({
  header: Schema.String,
  oldStart: NonNegativeInt,
  oldCount: NonNegativeInt,
  newStart: NonNegativeInt,
  newCount: NonNegativeInt,
  lines: Schema.Array(AetherDiffLine),
});
export type AetherGitDiffHunk = typeof AetherGitDiffHunk.Type;

export const AetherGitFileStatus = Schema.Literals(["added", "modified", "deleted", "renamed"]);
export type AetherGitFileStatus = typeof AetherGitFileStatus.Type;

const diffPathFields = {
  oldPath: Schema.String,
  newPath: Schema.String,
  displayPath: NonEmptyString,
  status: AetherGitFileStatus,
} as const;

const diffContentFields = {
  hunks: Schema.Array(AetherGitDiffHunk),
  additions: NonNegativeInt,
  deletions: NonNegativeInt,
} as const;

/**
 * One changed file, by `kind`: `text` has hunks; `binary` has no usable text (its bytes are read
 * over the files channel), though a type change can carry text hunks too; `too_large` exceeded
 * `maxBytes` and has no hunks at all.
 */
export const AetherGitDiffFile = Schema.Union([
  Schema.Struct({ ...diffPathFields, kind: Schema.Literal("text"), ...diffContentFields }),
  Schema.Struct({ ...diffPathFields, kind: Schema.Literal("binary"), ...diffContentFields }),
  Schema.Struct({ ...diffPathFields, kind: Schema.Literal("too_large"), maxBytes: PositiveInt }),
]);
export type AetherGitDiffFile = typeof AetherGitDiffFile.Type;

export const AetherGitDiffResult = Schema.Struct({
  baseRef: Schema.String,
  files: Schema.Array(AetherGitDiffFile),
});
export type AetherGitDiffResult = typeof AetherGitDiffResult.Type;

export const AetherGitDiffRequest = Schema.Struct({
  channel: Schema.Literal("git"),
  type: Schema.Literal("diff"),
  requestId: NonEmptyString,
  mode: AetherGitDiffMode,
});

export const AetherGitDiffResponse = Schema.Union([
  Schema.Struct({
    channel: Schema.Literal("git"),
    type: Schema.Literal("diff"),
    requestId: NonEmptyString,
    success: Schema.Literal(true),
    diff: AetherGitDiffResult,
  }),
  Schema.Struct({
    channel: Schema.Literal("git"),
    type: Schema.Literal("diff"),
    requestId: NonEmptyString,
    success: Schema.Literal(false),
    error: NonEmptyString,
  }),
]);

// ---------------------------------------------------------------------------
// Files read
// ---------------------------------------------------------------------------

export const AetherFileReadRequest = Schema.Struct({
  channel: Schema.Literal("files"),
  type: Schema.Literal("read"),
  requestId: NonEmptyString,
  path: NonEmptyString,
});

export const AetherFileErrorCode = Schema.Literals([
  "NOT_FOUND",
  "NOT_INITIALIZED",
  "PERMISSION_DENIED",
  "IS_DIRECTORY",
  "IS_FILE",
  "FILE_TOO_LARGE",
  "BINARY_FILE",
  "INVALID_PATH",
  "PATH_EXISTS",
  "INTERNAL_ERROR",
]);
export type AetherFileErrorCode = typeof AetherFileErrorCode.Type;

export const AetherFileRead = Schema.Struct({
  channel: Schema.Literal("files"),
  type: Schema.Literal("read"),
  requestId: NonEmptyString,
  success: Schema.Literal(true),
  path: NonEmptyString,
  /** UTF-8 text, or base64 bytes when `encoding` is `base64`. */
  content: Schema.String,
  encoding: Schema.Literals(["utf8", "base64"]),
  size: NonNegativeInt,
  modified: Schema.String,
  isBinary: Schema.Boolean,
});
export type AetherFileRead = typeof AetherFileRead.Type;

export const AetherFileReadResponse = Schema.Union([
  AetherFileRead,
  Schema.Struct({
    channel: Schema.Literal("files"),
    type: Schema.Literal("error"),
    requestId: NonEmptyString,
    success: Schema.Literal(false),
    error: NonEmptyString,
    code: AetherFileErrorCode,
    path: Schema.optionalKey(Schema.String),
  }),
]);

// ---------------------------------------------------------------------------
// Activity keepalive and server errors
// ---------------------------------------------------------------------------

export const AetherUserActivityMessage = Schema.Struct({
  channel: Schema.Literal("activity"),
  type: Schema.Literal("user_activity"),
});

/** The workspace service reporting its own failure, e.g. a frame it could not parse. */
export const AetherWorkspaceErrorMessage = Schema.Struct({
  channel: Schema.Literal("error"),
  type: Schema.Literal("error"),
  error: NonEmptyString,
});

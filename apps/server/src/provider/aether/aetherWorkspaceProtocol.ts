// FILE: aetherWorkspaceProtocol.ts
// Purpose: The frames Synara reads from Aether's workspace WebSocket: git diff and files read
// answers, and the service's own error frame.
// Layer: Server provider boundary (Aether). Source of truth: Aether's `workspace-protocol`
// messages and `domain-types/git` (2026-09-24). Terminal, agent and ports are out of scope.
//
// Same rules as the REST schemas: unknown keys are ignored, only fields a caller reads are
// declared, and the enums a caller branches on (`success`, file `kind` and `status`, line
// `kind`, read `encoding`) are strict. `isBinary` no longer exists on diff files.

import { PositiveInt } from "@synara/contracts";
import { Schema } from "effect";

const DiffHunk = Schema.Struct({
  header: Schema.String,
  lines: Schema.Array(
    Schema.Struct({
      kind: Schema.Literals(["add", "del", "context"]),
      text: Schema.String,
      noTrailingNewline: Schema.optionalKey(Schema.Boolean),
    }),
  ),
});

const diffPaths = {
  oldPath: Schema.String,
  newPath: Schema.String,
  status: Schema.Literals(["added", "modified", "deleted", "renamed"]),
} as const;

/**
 * One changed file, by `kind`: `text` has hunks; `binary` has no usable text (read its bytes
 * over the files channel), though a type change can carry text hunks too; `too_large` exceeded
 * `maxBytes` and has no hunks.
 */
export const AetherGitDiffFile = Schema.Union([
  Schema.Struct({
    ...diffPaths,
    kind: Schema.Literals(["text", "binary"]),
    hunks: Schema.Array(DiffHunk),
  }),
  Schema.Struct({ ...diffPaths, kind: Schema.Literal("too_large"), maxBytes: PositiveInt }),
]);
export type AetherGitDiffFile = typeof AetherGitDiffFile.Type;

export const AetherGitDiffResult = Schema.Struct({
  /** The merge-base commit the workspace diffed from, as a full object id (Aether `getDiff`). */
  baseRef: Schema.String,
  files: Schema.Array(AetherGitDiffFile),
});
export type AetherGitDiffResult = typeof AetherGitDiffResult.Type;

const failure = Schema.Struct({ success: Schema.Literal(false), error: Schema.String });

/** The answer to `{channel: "git", type: "diff"}`. */
export const AetherGitDiffResponse = Schema.Union([
  Schema.Struct({ success: Schema.Literal(true), diff: AetherGitDiffResult }),
  failure,
]);

/** The answer to `{channel: "files", type: "read"}`: `type: "read"` or `type: "error"`. */
export const AetherFileReadResponse = Schema.Union([
  Schema.Struct({
    success: Schema.Literal(true),
    /** UTF-8 text, or base64 bytes when `encoding` is `base64`. */
    content: Schema.String,
    encoding: Schema.Literals(["utf8", "base64"]),
  }),
  failure,
]);
export type AetherFileRead = Extract<typeof AetherFileReadResponse.Type, { success: true }>;

/** Answers carry the `requestId` of the request; other frames (other channels) do not. */
export const AetherWorkspaceAnswer = Schema.Struct({ requestId: Schema.String });

/** The workspace service reporting its own failure, e.g. a frame it could not parse. */
export const AetherWorkspaceErrorFrame = Schema.Struct({
  channel: Schema.Literal("error"),
  error: Schema.String,
});

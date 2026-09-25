// FILE: aetherMirrorErrors.ts
// Purpose: The ways an Aether mirror sync fails. The adapter maps each onto a turn failure or a
// warning.
// Layer: Server provider boundary (Aether).
//
// A failure after the reset (`MirrorApplyFailedError`, or a `MirrorPathEscapeError` found through a
// symlink) can leave the worktree partly synced; the sync then records the tree it left, so the
// next sync does not mistake it for local edits. Every other failure leaves the worktree as it was.

import { Schema } from "effect";

const MAX_LISTED_PATHS = 10;

const listPaths = (paths: ReadonlyArray<string>) =>
  paths.length <= MAX_LISTED_PATHS
    ? paths.join(", ")
    : `${paths.slice(0, MAX_LISTED_PATHS).join(", ")} and ${paths.length - MAX_LISTED_PATHS} more`;

/** A diff entry cannot be written as a patch `git apply` accepts: a contract break. */
export class MirrorDiffRebuildError extends Schema.TaggedErrorClass<MirrorDiffRebuildError>()(
  "MirrorDiffRebuildError",
  {
    path: Schema.String,
    detail: Schema.String,
  },
) {
  override get message(): string {
    return `The Aether workspace diff cannot be mirrored locally (${this.path}): ${this.detail}`;
  }
}

/** Files the workspace would not diff because they exceed its size limit. Never skipped. */
export class MirrorTooLargeError extends Schema.TaggedErrorClass<MirrorTooLargeError>()(
  "MirrorTooLargeError",
  {
    files: Schema.Array(Schema.Struct({ path: Schema.String, maxBytes: Schema.Number })),
  },
) {
  override get message(): string {
    const files = this.files.map((file) => `${file.path} (over ${file.maxBytes} bytes)`);
    return `The Aether workspace changed files too large to mirror: ${listPaths(files)}.`;
  }
}

/**
 * A diff path is absolute, has a `..` or `.git` segment, is a name Git refuses (such as one Windows
 * or macOS resolves to `.git`), or reaches through a symlink.
 */
export class MirrorPathEscapeError extends Schema.TaggedErrorClass<MirrorPathEscapeError>()(
  "MirrorPathEscapeError",
  {
    path: Schema.String,
    detail: Schema.String,
  },
) {
  override get message(): string {
    return `Refused to mirror '${this.path}': ${this.detail}`;
  }
}

/**
 * The worktree no longer matches the tree the last sync made (or, before the first sync, a
 * clean checkout of the task base): someone changed it locally. Nothing was overwritten.
 */
export class MirrorLocalEditsError extends Schema.TaggedErrorClass<MirrorLocalEditsError>()(
  "MirrorLocalEditsError",
  {
    worktreePath: Schema.String,
    /** Changed, added and deleted paths. Empty when only HEAD moved, or when they could not be listed. */
    paths: Schema.Array(Schema.String),
    /** Set when HEAD is no longer the commit the mirror left (a local commit, reset or checkout). */
    headMoved: Schema.optional(Schema.Struct({ expected: Schema.String, actual: Schema.String })),
    baseCommit: Schema.String,
  },
) {
  override get message(): string {
    const changes = [
      ...(this.paths.length > 0 ? [`changed ${listPaths(this.paths)}`] : []),
      ...(this.headMoved === undefined
        ? []
        : [`moved HEAD from ${this.headMoved.expected} to ${this.headMoved.actual}`]),
    ];
    return (
      `The Aether mirror worktree ${this.worktreePath} was edited locally since the last sync ` +
      `(${changes.length > 0 ? changes.join("; ") : "the changed paths could not be listed"}). ` +
      "The mirror never overwrites local work. Move those changes elsewhere, then reset the " +
      `worktree to the task base (git reset --hard ${this.baseCommit} && git clean -fd) to resume.`
    );
  }
}

/** The local Git or filesystem step failed. From `reset` on, the worktree may be partly synced. */
export class MirrorApplyFailedError extends Schema.TaggedErrorClass<MirrorApplyFailedError>()(
  "MirrorApplyFailedError",
  {
    worktreePath: Schema.String,
    step: Schema.Literals([
      "check",
      "lock",
      "inspect",
      "reset",
      "apply",
      "remove",
      "write",
      "fingerprint",
      "record",
    ]),
    path: Schema.optional(Schema.String),
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect),
  },
) {
  override get message(): string {
    const path = this.path === undefined ? "" : ` (${this.path})`;
    return `The Aether mirror sync failed at ${this.step}${path}: ${this.detail}`;
  }
}

/** The workspace socket did not answer a diff or file read usefully. Nothing was changed. */
export class MirrorTransportError extends Schema.TaggedErrorClass<MirrorTransportError>()(
  "MirrorTransportError",
  {
    request: Schema.Literals(["git.diff", "files.read"]),
    path: Schema.optional(Schema.String),
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect),
  },
) {
  override get message(): string {
    const path = this.path === undefined ? "" : ` for ${this.path}`;
    return `The Aether workspace ${this.request} request${path} failed: ${this.detail}`;
  }
}

export type AetherMirrorSyncError =
  | MirrorDiffRebuildError
  | MirrorTooLargeError
  | MirrorPathEscapeError
  | MirrorLocalEditsError
  | MirrorApplyFailedError
  | MirrorTransportError;

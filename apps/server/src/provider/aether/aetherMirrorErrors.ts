// FILE: aetherMirrorErrors.ts
// Purpose: The one error an Aether mirror sync fails with. The adapter (P4) maps `reason` onto a
// turn failure or a warning, and owns the wording users see.
// Layer: Server provider boundary (Aether).

import { Schema } from "effect";

export const AetherMirrorFailure = Schema.Literals([
  "diff-rebuild",
  "apply-failed",
  "too-large",
  "path-escape",
  "local-edits",
  "base-mismatch",
  "record",
  "transport",
  /** Aether's name for the task's branch is not a Git branch name. */
  "branch",
  /** Reading the task's branch from origin failed. */
  "fetch",
]);
export type AetherMirrorFailure = typeof AetherMirrorFailure.Type;

/**
 * The worktree is left as it was, except when `git read-tree` fails part way through writing it
 * (a full disk, say). The next sync then reports the half-written paths as local edits.
 */
export class AetherMirrorError extends Schema.TaggedErrorClass<AetherMirrorError>()(
  "AetherMirrorError",
  {
    reason: AetherMirrorFailure,
    /** The paths involved: local edits, refused or too-large paths, the file a read was for. */
    paths: Schema.Array(Schema.String),
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect),
  },
) {}

export const mirrorError = (
  reason: AetherMirrorFailure,
  detail: string,
  extra: { readonly paths?: ReadonlyArray<string>; readonly cause?: unknown } = {},
) =>
  new AetherMirrorError({
    reason,
    detail,
    paths: extra.paths ?? [],
    ...(extra.cause === undefined ? {} : { cause: extra.cause }),
  });

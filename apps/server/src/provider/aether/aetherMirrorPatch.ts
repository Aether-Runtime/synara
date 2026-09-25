// FILE: aetherMirrorPatch.ts
// Purpose: Turns Aether's structured cumulative diff into a mirror plan: one unified patch for
// `git apply`, the paths to remove and the binary paths to write directly, and the mode-only
// changes the wire cannot carry.
// Layer: Server provider boundary (Aether). Pure: no I/O.
//
// The wire has no file modes, so new files are written 100644 and a text entry whose only change
// is its mode is reported rather than applied. `binary` entries are never patched: their bytes
// come over the files channel. A `too_large` entry fails the plan unless it is a deletion, which
// needs no content. Every path is checked lexically here, and the sync has Git check `paths` again
// (Git also refuses the names Windows and macOS resolve to `.git`) before touching the worktree.

import { isWorkspaceRelativePathSafe } from "@synara/shared/path";
import { Result } from "effect";

import {
  MirrorDiffRebuildError,
  MirrorPathEscapeError,
  MirrorTooLargeError,
} from "./aetherMirrorErrors.ts";
import type { AetherGitDiffFile, AetherGitDiffResult } from "./aetherWorkspaceProtocol.ts";

export interface AetherMirrorWrite {
  readonly path: string;
  /** The entry adds the file (an addition or a rename target), so nothing may be there yet. */
  readonly create: boolean;
}

export interface AetherMirrorPlan {
  /** Every worktree path the plan reads, writes or removes. */
  readonly paths: ReadonlyArray<string>;
  /** One unified patch covering every text entry; empty when there is none. */
  readonly patch: string;
  /** Files the patch adds: text additions and rename targets. */
  readonly patchCreates: ReadonlyArray<string>;
  /** Removed directly, after the patch and before any write: binary deletions and rename sources. */
  readonly removals: ReadonlyArray<string>;
  /** Written directly, last, with bytes read over the files channel. */
  readonly writes: ReadonlyArray<AetherMirrorWrite>;
  /** Text entries whose only change is the file mode. Not applied. */
  readonly modeOnly: ReadonlyArray<string>;
  /** Entries the plan applies: every entry except the mode-only ones. */
  readonly fileCount: number;
}

export type AetherMirrorPlanError =
  | MirrorDiffRebuildError
  | MirrorPathEscapeError
  | MirrorTooLargeError;

type TextEntry = Extract<AetherGitDiffFile, { readonly kind: "text" | "binary" }>;

/** The worktree paths an entry reads or writes: `/dev/null` sides are not paths. */
const entryPaths = (file: AetherGitDiffFile): ReadonlyArray<string> => {
  switch (file.status) {
    case "added":
      return [file.newPath];
    case "deleted":
      return [file.oldPath];
    case "modified":
      return [file.newPath];
    case "renamed":
      return [file.oldPath, file.newPath];
  }
};

/** Why `path` could leave the worktree or reach Git's metadata, or undefined when it is safe. */
export const mirrorPathEscape = (path: string): string | undefined => {
  if (path.includes("\0")) return "it contains a NUL byte";
  if (!isWorkspaceRelativePathSafe(path)) {
    return "it is empty or absolute, or has a '.' or '..' segment";
  }
  const segments = path.split(/[\\/]/u);
  if (segments.some((segment) => segment.length === 0)) return "it has an empty segment";
  if (segments.some((segment) => segment.toLowerCase() === ".git")) {
    return "it is inside a '.git' directory";
  }
  return undefined;
};

/** The named C escapes Git writes; every other control byte becomes `\NNN`. */
const C_STYLE_ESCAPES: Readonly<Record<string, string>> = {
  '"': '\\"',
  "\\": "\\\\",
  "\u0007": "\\a",
  "\b": "\\b",
  "\t": "\\t",
  "\n": "\\n",
  "\v": "\\v",
  "\f": "\\f",
  "\r": "\\r",
};

/**
 * Git's C-style quoting for a path in a patch header. A raw quote, backslash or control
 * character makes `git apply` reject the header, so those are escaped and the token quoted, as
 * Git writes them. Non-ASCII stays raw UTF-8: `git apply` reads it either way.
 */
export const quoteGitPath = (token: string): string => {
  let escaped = "";
  let quoted = false;
  for (const character of token) {
    const named = C_STYLE_ESCAPES[character];
    const code = character.codePointAt(0) ?? 0;
    if (named !== undefined) {
      escaped += named;
      quoted = true;
    } else if (code < 0x20 || code === 0x7f) {
      escaped += `\\${code.toString(8).padStart(3, "0")}`;
      quoted = true;
    } else {
      escaped += character;
    }
  }
  return quoted ? `"${escaped}"` : token;
};

const HUNK_RANGE = /^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@/u;
const LINE_PREFIX = { add: "+", del: "-", context: " " } as const;

const renderHunks = (
  path: string,
  hunks: TextEntry["hunks"],
): Result.Result<ReadonlyArray<string>, MirrorDiffRebuildError> => {
  const rendered: Array<string> = [];
  for (const hunk of hunks) {
    const range = HUNK_RANGE.exec(hunk.header);
    if (range === null) {
      return Result.fail(
        new MirrorDiffRebuildError({
          path,
          detail: `hunk header '${hunk.header}' is not a unified diff range`,
        }),
      );
    }
    let oldCount = 0;
    let newCount = 0;
    const body: Array<string> = [];
    for (const line of hunk.lines) {
      if (line.text.includes("\n")) {
        return Result.fail(
          new MirrorDiffRebuildError({ path, detail: "a diff line contains a line break" }),
        );
      }
      if (line.kind !== "add") oldCount += 1;
      if (line.kind !== "del") newCount += 1;
      body.push(`${LINE_PREFIX[line.kind]}${line.text}`);
      if (line.noTrailingNewline === true) body.push("\\ No newline at end of file");
    }
    // The header's counts must match the lines: a mismatch is a truncated hunk, and applying
    // what arrived would mirror part of a change.
    const declaredOld = range[2] === undefined ? 1 : Number(range[2]);
    const declaredNew = range[4] === undefined ? 1 : Number(range[4]);
    if (declaredOld !== oldCount || declaredNew !== newCount) {
      return Result.fail(
        new MirrorDiffRebuildError({
          path,
          detail: `hunk '${hunk.header}' carries ${oldCount} old and ${newCount} new lines`,
        }),
      );
    }
    rendered.push(`@@ -${range[1]},${oldCount} +${range[3]},${newCount} @@`, ...body);
  }
  return Result.succeed(rendered);
};

const renderTextEntry = (file: TextEntry): Result.Result<string, MirrorDiffRebuildError> => {
  const oldPath = file.status === "added" ? file.newPath : file.oldPath;
  const newPath = file.status === "deleted" ? file.oldPath : file.newPath;
  const oldName = quoteGitPath(`a/${oldPath}`);
  const newName = quoteGitPath(`b/${newPath}`);
  const lines = [`diff --git ${oldName} ${newName}`];
  if (file.status === "added") lines.push("new file mode 100644");
  if (file.status === "deleted") lines.push("deleted file mode 100644");
  if (file.status === "renamed") {
    lines.push(`rename from ${quoteGitPath(oldPath)}`, `rename to ${quoteGitPath(newPath)}`);
  }
  if (file.hunks.length > 0) {
    const hunks = renderHunks(newPath, file.hunks);
    if (Result.isFailure(hunks)) return Result.fail(hunks.failure);
    lines.push(
      file.status === "added" ? "--- /dev/null" : `--- ${oldName}`,
      file.status === "deleted" ? "+++ /dev/null" : `+++ ${newName}`,
      ...hunks.success,
    );
  }
  return Result.succeed(`${lines.join("\n")}\n`);
};

/** Plan one mirror sync, or say why the diff cannot be mirrored. */
export const planAetherMirror = (
  diff: AetherGitDiffResult,
): Result.Result<AetherMirrorPlan, AetherMirrorPlanError> => {
  for (const file of diff.files) {
    if (file.status === "modified" && file.oldPath !== file.newPath) {
      return Result.fail(
        new MirrorDiffRebuildError({
          path: file.newPath,
          detail: `a modified entry names two paths ('${file.oldPath}' and '${file.newPath}')`,
        }),
      );
    }
    for (const path of entryPaths(file)) {
      const escape = mirrorPathEscape(path);
      if (escape !== undefined) {
        return Result.fail(new MirrorPathEscapeError({ path, detail: escape }));
      }
    }
  }

  const tooLarge = diff.files.flatMap((file) =>
    file.kind === "too_large" && file.status !== "deleted"
      ? [{ path: file.newPath, maxBytes: file.maxBytes }]
      : [],
  );
  if (tooLarge.length > 0) return Result.fail(new MirrorTooLargeError({ files: tooLarge }));

  const patches: Array<string> = [];
  const patchCreates: Array<string> = [];
  const removals: Array<string> = [];
  const writes: Array<AetherMirrorWrite> = [];
  const modeOnly: Array<string> = [];
  for (const file of diff.files) {
    if (file.kind === "too_large") {
      removals.push(file.oldPath);
    } else if (file.kind === "binary") {
      if (file.status === "deleted" || file.status === "renamed") removals.push(file.oldPath);
      if (file.status !== "deleted") {
        writes.push({
          path: file.newPath,
          create: file.status === "added" || file.status === "renamed",
        });
      }
    } else if (file.status === "modified" && file.hunks.length === 0) {
      // A bare `diff --git` header makes `git apply` reject the whole patch.
      modeOnly.push(file.newPath);
    } else {
      const rendered = renderTextEntry(file);
      if (Result.isFailure(rendered)) return Result.fail(rendered.failure);
      patches.push(rendered.success);
      if (file.status === "added" || file.status === "renamed") patchCreates.push(file.newPath);
    }
  }
  return Result.succeed({
    paths: diff.files.flatMap(entryPaths),
    patch: patches.join(""),
    patchCreates,
    removals,
    writes,
    modeOnly,
    fileCount: diff.files.length - modeOnly.length,
  });
};

// FILE: aetherMirrorPatch.ts
// Purpose: Turns Aether's structured cumulative diff into the steps that build the mirror's target
// tree: a unified patch for `git apply --cached`, the paths to remove from the index, and the
// binary files whose bytes come over the files channel.
// Layer: Server provider boundary (Aether). Pure: no I/O.

import { isWorkspaceRelativePathSafe } from "@synara/shared/path";
import { Result } from "effect";

import { type AetherMirrorError, mirrorError } from "./aetherMirrorErrors.ts";
import type { AetherGitDiffFile, AetherGitDiffResult } from "./aetherWorkspaceProtocol.ts";

export interface AetherMirrorWrite {
  readonly path: string;
  /** The base path whose executable bit the file keeps (a modification or a rename source). */
  readonly modeOf?: string;
}

export interface AetherMirrorPlan {
  /** Text additions, modifications and renames. Git keeps the mode of what it modifies. */
  readonly patch: string;
  /** Removed from the index: every deletion (which needs no content or mode), binary rename sources. */
  readonly removals: ReadonlyArray<string>;
  readonly writes: ReadonlyArray<AetherMirrorWrite>;
  /** Text entries whose only change is the file mode, which the wire does not carry. Not applied. */
  readonly modeOnly: ReadonlyArray<string>;
  /** Entries the plan applies: every entry except the mode-only ones. */
  readonly fileCount: number;
}

type TextEntry = Extract<AetherGitDiffFile, { readonly kind: "text" | "binary" }>;

/** The worktree paths an entry names: `/dev/null` sides are not paths. */
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
): Result.Result<ReadonlyArray<string>, AetherMirrorError> => {
  const rendered: Array<string> = [];
  for (const hunk of hunks) {
    const range = HUNK_RANGE.exec(hunk.header);
    if (range === null) {
      return Result.fail(
        mirrorError("diff-rebuild", `Hunk header '${hunk.header}' is not a unified diff range.`, {
          paths: [path],
        }),
      );
    }
    let oldCount = 0;
    let newCount = 0;
    const body: Array<string> = [];
    for (const line of hunk.lines) {
      if (line.text.includes("\n")) {
        return Result.fail(
          mirrorError("diff-rebuild", "A diff line contains a line break.", { paths: [path] }),
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
        mirrorError(
          "diff-rebuild",
          `Hunk '${hunk.header}' carries ${oldCount} old and ${newCount} new lines.`,
          { paths: [path] },
        ),
      );
    }
    rendered.push(`@@ -${range[1]},${oldCount} +${range[3]},${newCount} @@`, ...body);
  }
  return Result.succeed(rendered);
};

/** Additions, modifications and renames only: deletions are index removals. */
const renderTextEntry = (file: TextEntry): Result.Result<string, AetherMirrorError> => {
  const oldPath = file.status === "added" ? file.newPath : file.oldPath;
  const oldName = quoteGitPath(`a/${oldPath}`);
  const newName = quoteGitPath(`b/${file.newPath}`);
  const lines = [`diff --git ${oldName} ${newName}`];
  if (file.status === "added") lines.push("new file mode 100644");
  if (file.status === "renamed") {
    lines.push(`rename from ${quoteGitPath(oldPath)}`, `rename to ${quoteGitPath(file.newPath)}`);
  }
  if (file.hunks.length > 0) {
    const hunks = renderHunks(file.newPath, file.hunks);
    if (Result.isFailure(hunks)) return Result.fail(hunks.failure);
    lines.push(
      file.status === "added" ? "--- /dev/null" : `--- ${oldName}`,
      `+++ ${newName}`,
      ...hunks.success,
    );
  }
  return Result.succeed(`${lines.join("\n")}\n`);
};

/** Plan the target tree, or say why the diff cannot be mirrored. */
export const planAetherMirror = (
  diff: AetherGitDiffResult,
): Result.Result<AetherMirrorPlan, AetherMirrorError> => {
  for (const file of diff.files) {
    if (file.status === "modified" && file.oldPath !== file.newPath) {
      return Result.fail(
        mirrorError(
          "diff-rebuild",
          `A modified entry names two paths ('${file.oldPath}' and '${file.newPath}').`,
          { paths: [file.oldPath, file.newPath] },
        ),
      );
    }
    for (const path of entryPaths(file)) {
      const escape = mirrorPathEscape(path);
      if (escape !== undefined) {
        return Result.fail(
          mirrorError("path-escape", `Refused '${path}': ${escape}.`, { paths: [path] }),
        );
      }
    }
  }

  // The workspace sends `too_large` only for untracked additions, whose content it withholds.
  const tooLarge = diff.files.flatMap((file) => (file.kind === "too_large" ? [file] : []));
  if (tooLarge.length > 0) {
    return Result.fail(
      mirrorError(
        "too-large",
        tooLarge.map((file) => `${file.newPath} exceeds ${file.maxBytes} bytes`).join("; "),
        { paths: tooLarge.map((file) => file.newPath) },
      ),
    );
  }

  const patches: Array<string> = [];
  const removals: Array<string> = [];
  const writes: Array<AetherMirrorWrite> = [];
  const modeOnly: Array<string> = [];
  for (const file of diff.files) {
    if (file.kind === "too_large") continue;
    if (file.status === "deleted") {
      removals.push(file.oldPath);
    } else if (file.kind === "binary") {
      if (file.status === "renamed") removals.push(file.oldPath);
      writes.push(
        file.status === "added"
          ? { path: file.newPath }
          : { path: file.newPath, modeOf: file.oldPath },
      );
    } else if (file.status === "modified" && file.hunks.length === 0) {
      // A bare `diff --git` header makes `git apply` reject the whole patch.
      modeOnly.push(file.newPath);
    } else {
      const rendered = renderTextEntry(file);
      if (Result.isFailure(rendered)) return Result.fail(rendered.failure);
      patches.push(rendered.success);
    }
  }
  return Result.succeed({
    patch: patches.join(""),
    removals,
    writes,
    modeOnly,
    fileCount: diff.files.length - modeOnly.length,
  });
};

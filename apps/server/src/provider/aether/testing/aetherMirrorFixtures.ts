// FILE: aetherMirrorFixtures.ts
// Purpose: Builds Aether workspace diffs in their wire shape for the mirror sync tests, and a fake
// workspace socket that serves them with the binary files' bytes.
// Layer: Test support (Aether provider).

import { Effect } from "effect";

import type {
  AetherFileRead,
  AetherGitDiffFile,
  AetherGitDiffResult,
} from "../aetherWorkspaceProtocol.ts";
import { AetherWorkspaceRequestError } from "../aetherWorkspaceSocket.ts";
import type { AetherMirrorWorkspace } from "../aetherMirrorSync.ts";

type Hunk = Extract<AetherGitDiffFile, { readonly kind: "text" | "binary" }>["hunks"][number];
type Line = Hunk["lines"][number];

const NO_NEWLINE = "\\ No newline at end of file";

/**
 * One hunk from unified-diff lines: `" ctx"`, `"-old"`, `"+new"`, and `NO_NEWLINE` after a line
 * that has no trailing newline. The header's counts are computed from the lines.
 */
export const hunk = (oldStart: number, newStart: number, ...lines: ReadonlyArray<string>): Hunk => {
  const parsed: Array<Line> = [];
  for (const line of lines) {
    if (line === NO_NEWLINE) {
      parsed[parsed.length - 1] = { ...parsed[parsed.length - 1]!, noTrailingNewline: true };
      continue;
    }
    const kind = line[0] === "+" ? "add" : line[0] === "-" ? "del" : "context";
    parsed.push({ kind, text: line.slice(1) });
  }
  const oldCount = parsed.filter((line) => line.kind !== "add").length;
  const newCount = parsed.filter((line) => line.kind !== "del").length;
  return { header: `@@ -${oldStart},${oldCount} +${newStart},${newCount} @@`, lines: parsed };
};

const contentLines = (content: string, prefix: "+" | "-"): Array<string> => {
  if (content.length === 0) return [];
  const trailing = content.endsWith("\n");
  const lines = (trailing ? content.slice(0, -1) : content).split("\n").map((l) => prefix + l);
  return trailing ? lines : [...lines, NO_NEWLINE];
};

/** A new text file. The wire names `/dev/null` as the old side, and sends no hunk when empty. */
export const added = (path: string, content: string): AetherGitDiffFile => ({
  oldPath: "/dev/null",
  newPath: path,
  status: "added",
  kind: "text",
  hunks: content.length === 0 ? [] : [hunk(0, 1, ...contentLines(content, "+"))],
});

export const deleted = (path: string, content: string): AetherGitDiffFile => ({
  oldPath: path,
  newPath: "/dev/null",
  status: "deleted",
  kind: "text",
  hunks: content.length === 0 ? [] : [hunk(1, 0, ...contentLines(content, "-"))],
});

export const modified = (path: string, ...hunks: ReadonlyArray<Hunk>): AetherGitDiffFile => ({
  oldPath: path,
  newPath: path,
  status: "modified",
  kind: "text",
  hunks,
});

/** A rename; with no hunks it is a pure rename. */
export const renamed = (
  oldPath: string,
  newPath: string,
  ...hunks: ReadonlyArray<Hunk>
): AetherGitDiffFile => ({ oldPath, newPath, status: "renamed", kind: "text", hunks });

/** A chmod: a modified text entry with no hunks. */
export const modeOnly = (path: string): AetherGitDiffFile => modified(path);

export const binary = (
  status: AetherGitDiffFile["status"],
  oldPath: string,
  newPath: string,
): AetherGitDiffFile => ({ oldPath, newPath, status, kind: "binary", hunks: [] });

export const tooLarge = (
  status: AetherGitDiffFile["status"],
  path: string,
  maxBytes: number,
): AetherGitDiffFile => ({
  oldPath: status === "added" ? "/dev/null" : path,
  newPath: status === "deleted" ? "/dev/null" : path,
  status,
  kind: "too_large",
  maxBytes,
});

export const diffOf = (...files: ReadonlyArray<AetherGitDiffFile>): AetherGitDiffResult => ({
  baseRef: "origin/main",
  files,
});

const WORKSPACE_ID = "0b0e4f7e-5c1a-4d2b-9e3f-6a7b8c9d0e1f";

/**
 * A workspace socket that answers with whatever the test last served: a diff, and the bytes of
 * the files it names. `failDiff` and `failRead` make the next requests fail as a dropped socket
 * or a timeout would.
 */
export const makeFakeWorkspace = () => {
  let diff: AetherGitDiffResult = diffOf();
  let files = new Map<string, Uint8Array>();
  let diffFailure: string | undefined;
  let readFailure: string | undefined;
  const reads: Array<string> = [];

  const workspace: AetherMirrorWorkspace = {
    requestGitDiff: () =>
      diffFailure === undefined
        ? Effect.succeed(diff)
        : Effect.fail(
            new AetherWorkspaceRequestError({
              workspaceId: WORKSPACE_ID,
              request: "git.diff",
              reason: "timeout",
              detail: diffFailure,
            }),
          ),
    readFile: (path) =>
      Effect.suspend(() => {
        reads.push(path);
        const content = files.get(path);
        if (readFailure !== undefined || content === undefined) {
          return Effect.fail(
            new AetherWorkspaceRequestError({
              workspaceId: WORKSPACE_ID,
              request: "files.read",
              reason: readFailure === undefined ? "failed" : "timeout",
              detail: readFailure ?? `ENOENT: ${path}`,
            }),
          );
        }
        const read: AetherFileRead = {
          success: true,
          content: Buffer.from(content).toString("base64"),
          encoding: "base64",
        };
        return Effect.succeed(read);
      }),
  };

  return {
    workspace,
    reads,
    serve: (next: AetherGitDiffResult, nextFiles: Record<string, Uint8Array | string> = {}) => {
      diff = next;
      files = new Map(
        Object.entries(nextFiles).map(([path, content]) => [
          path,
          typeof content === "string" ? Buffer.from(content, "utf8") : content,
        ]),
      );
      diffFailure = undefined;
      readFailure = undefined;
    },
    failDiff: (detail: string) => {
      diffFailure = detail;
    },
    failRead: (detail: string) => {
      readFailure = detail;
    },
  };
};

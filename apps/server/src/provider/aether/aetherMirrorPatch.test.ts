// FILE: aetherMirrorPatch.test.ts
// Purpose: The mirror planner on its own: the patch it writes for each entry shape, path quoting,
// and every diff it refuses before anything touches a worktree.
// Layer: Server provider tests (Aether). Applying the patches is covered with real Git in
// aetherMirrorSync.test.ts.
import { Result } from "effect";
import { describe, expect, it } from "vitest";

import type { AetherMirrorPlanError } from "./aetherMirrorPatch.ts";
import { mirrorPathEscape, planAetherMirror, quoteGitPath } from "./aetherMirrorPatch.ts";
import type { AetherGitDiffFile } from "./aetherWorkspaceProtocol.ts";
import {
  added,
  binary,
  deleted,
  diffOf,
  hunk,
  modeOnly,
  modified,
  renamed,
  tooLarge,
} from "./testing/aetherMirrorFixtures.ts";

const plan = (...files: ReadonlyArray<AetherGitDiffFile>) => {
  const result = planAetherMirror(diffOf(...files));
  if (Result.isFailure(result)) throw new Error(`unexpected refusal: ${result.failure.message}`);
  return result.success;
};

const refusal = (...files: ReadonlyArray<AetherGitDiffFile>): AetherMirrorPlanError => {
  const result = planAetherMirror(diffOf(...files));
  if (Result.isSuccess(result)) throw new Error("expected the plan to be refused");
  return result.failure;
};

describe("planAetherMirror", () => {
  it("writes git headers for added, modified, deleted and renamed text files", () => {
    const { patch, fileCount } = plan(
      added("notes.md", "hello"),
      modified("app.txt", hunk(1, 1, " one", "-two", "+2")),
      deleted("gone.txt", "bye\n"),
      renamed("lib.txt", "src/lib.txt", hunk(1, 1, " lib", "+more")),
    );
    expect(fileCount).toBe(4);
    expect(patch).toBe(
      [
        "diff --git a/notes.md b/notes.md",
        "new file mode 100644",
        "--- /dev/null",
        "+++ b/notes.md",
        "@@ -0,0 +1,1 @@",
        "+hello",
        "\\ No newline at end of file",
        "diff --git a/app.txt b/app.txt",
        "--- a/app.txt",
        "+++ b/app.txt",
        "@@ -1,2 +1,2 @@",
        " one",
        "-two",
        "+2",
        "diff --git a/gone.txt b/gone.txt",
        "deleted file mode 100644",
        "--- a/gone.txt",
        "+++ /dev/null",
        "@@ -1,1 +0,0 @@",
        "-bye",
        "diff --git a/lib.txt b/src/lib.txt",
        "rename from lib.txt",
        "rename to src/lib.txt",
        "--- a/lib.txt",
        "+++ b/src/lib.txt",
        "@@ -1,1 +1,2 @@",
        " lib",
        "+more",
        "",
      ].join("\n"),
    );
  });

  it("writes an empty new file and a pure rename without hunks", () => {
    expect(plan(added("empty.txt", ""), renamed("a.txt", "b.txt")).patch).toBe(
      [
        "diff --git a/empty.txt b/empty.txt",
        "new file mode 100644",
        "diff --git a/a.txt b/b.txt",
        "rename from a.txt",
        "rename to b.txt",
        "",
      ].join("\n"),
    );
  });

  it("quotes paths the way Git does, leaving non-ASCII raw", () => {
    expect(quoteGitPath("a/plain name.txt")).toBe("a/plain name.txt");
    expect(quoteGitPath('a/we"ird\\name.txt')).toBe('"a/we\\"ird\\\\name.txt"');
    expect(quoteGitPath("a/tab\there\n")).toBe('"a/tab\\there\\n"');
    expect(quoteGitPath("a/bell\u0007\u007f")).toBe('"a/bell\\a\\177"');
    expect(quoteGitPath("a/ünï/日本.txt")).toBe("a/ünï/日本.txt");
    expect(plan(renamed('q"1.txt', "日本.txt")).patch).toContain('rename from "q\\"1.txt"');
  });

  it("reports a mode-only change instead of writing a header git apply rejects", () => {
    const result = plan(modeOnly("tools/run.sh"), added("notes.md", "hi\n"));
    expect(result.modeOnly).toEqual(["tools/run.sh"]);
    expect(result.patch).not.toContain("tools/run.sh");
    expect(result.fileCount).toBe(1);
  });

  it("removes and writes binary files directly, and removes a too-large deletion", () => {
    const result = plan(
      binary("added", "/dev/null", "img/new.png"),
      binary("modified", "img/logo.png", "img/logo.png"),
      binary("deleted", "img/old.png", "/dev/null"),
      binary("renamed", "img/a.png", "img/b.png"),
      tooLarge("deleted", "data/huge.json", 1_048_576),
    );
    expect(result.patch).toBe("");
    expect(result.removals).toEqual(["img/old.png", "img/a.png", "data/huge.json"]);
    expect(result.writes).toEqual(["img/new.png", "img/logo.png", "img/b.png"]);
  });

  it("refuses every too-large file it would have to write, with its limit", () => {
    const error = refusal(
      added("notes.md", "hi\n"),
      tooLarge("modified", "data/big.json", 1_048_576),
      tooLarge("added", "data/new.bin", 2_000),
    );
    expect(error).toMatchObject({
      _tag: "MirrorTooLargeError",
      files: [
        { path: "data/big.json", maxBytes: 1_048_576 },
        { path: "data/new.bin", maxBytes: 2_000 },
      ],
    });
    expect(error.message).toContain("data/big.json (over 1048576 bytes)");
  });

  it.each([
    ["an absolute path", added("/etc/passwd", "x\n")],
    ["a Windows drive path", binary("added", "/dev/null", "C:\\Windows\\x.dll")],
    ["a '..' segment", added("../escape.txt", "x\n")],
    ["a backslash '..' segment", binary("added", "/dev/null", "a\\..\\..\\x.bin")],
    ["a '.git' segment", binary("added", "/dev/null", ".git/hooks/post-checkout")],
    ["a nested '.GIT' segment", deleted("sub/.GIT/config", "x\n")],
    ["an empty segment", added("a//b.txt", "x\n")],
    ["a NUL byte", added("a\0b.txt", "x\n")],
    ["an escaping rename target", renamed("lib.txt", "../lib.txt")],
    ["an escaping binary rename source", binary("renamed", "../x.bin", "x.bin")],
  ])("refuses %s", (_name, file) => {
    expect(refusal(added("fine.txt", "x\n"), file)._tag).toBe("MirrorPathEscapeError");
  });

  it("ignores the /dev/null side of added and deleted entries", () => {
    expect(mirrorPathEscape("/dev/null")).toBeDefined();
    expect(() => plan(added("a.txt", "x\n"), deleted("b.txt", "y\n"))).not.toThrow();
  });

  it("refuses a hunk it cannot trust", () => {
    const truncated = modified("app.txt", {
      header: "@@ -1,3 +1,3 @@",
      lines: [{ kind: "context", text: "one" }],
    });
    expect(refusal(truncated)).toMatchObject({
      _tag: "MirrorDiffRebuildError",
      path: "app.txt",
    });
    const unranged = modified("app.txt", { header: "@@", lines: [] });
    expect(refusal(unranged).message).toContain("not a unified diff range");
    const split = modified("app.txt", {
      header: "@@ -1 +1 @@",
      lines: [{ kind: "context", text: "a\nb" }],
    });
    expect(refusal(split).message).toContain("line break");
  });

  it("accepts a range without counts, which means one line", () => {
    const single = modified("app.txt", {
      header: "@@ -3 +3 @@ function heading()",
      lines: [
        { kind: "del", text: "old" },
        { kind: "add", text: "new" },
      ],
    });
    expect(plan(single).patch).toContain("@@ -3,1 +3,1 @@\n-old\n+new\n");
  });

  it("refuses a modified entry that names two paths", () => {
    expect(refusal({ ...modified("a.txt"), newPath: "b.txt" })._tag).toBe("MirrorDiffRebuildError");
  });
});

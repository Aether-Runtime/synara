// FILE: GitCore.ownership.test.ts
// Purpose: Exercises provider-owned worktrees in GitCore: claims, withMutation and primitive
//          refusals, the guarded editor write, and stdin for execute.
// Layer: Server Git service tests
// Depends on: GitCoreLive plus real temporary Git repositories.
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import * as fs from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import { ThreadId } from "@synara/contracts";
import { Deferred, Effect, Exit, Fiber, FileSystem, Layer, Path, Scope } from "effect";
import { describe, expect } from "vitest";

import { GitCoreLive } from "./GitCore.ts";
import { WorktreeOwnedError } from "../Errors.ts";
import { GitCore, type WorktreeOwner } from "../Services/GitCore.ts";
import { ServerConfig } from "../../config.ts";
import { WorkspaceLayerLive } from "../../workspace/runtimeLayer.ts";
import { WorkspaceFileSystem } from "../../workspace/Services/WorkspaceFileSystem.ts";
import {
  removeWorktreeReleasingDeletedOwner,
  toWsRpcError,
  writeWorkspaceFileOutsideOwnedWorktree,
} from "../../wsRpc.ts";

const ServerConfigLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "synara-git-ownership-test-",
});
const TestLayer = Layer.mergeAll(
  NodeServices.layer,
  GitCoreLive.pipe(Layer.provide(ServerConfigLayer), Layer.provide(NodeServices.layer)),
  WorkspaceLayerLive.pipe(Layer.provide(NodeServices.layer)),
);

// Differently cased paths name the same directory only on a case-insensitive
// filesystem, and only macOS and Windows native realpath return the on-disk
// case. Elsewhere (including Linux casefold mounts) the mixed-case test skips.
const nativeRealpathFoldsCase =
  (process.platform === "darwin" || process.platform === "win32") &&
  (() => {
    const probe = mkdtempSync(path.join(tmpdir(), "case-probe-"));
    try {
      return existsSync(path.join(path.dirname(probe), path.basename(probe).toUpperCase()));
    } finally {
      rmSync(probe, { recursive: true, force: true });
    }
  })();

const aetherOwner: WorktreeOwner = {
  threadId: ThreadId.makeUnsafe("thread-aether"),
  provider: "aether",
};
const otherOwner: WorktreeOwner = {
  threadId: ThreadId.makeUnsafe("thread-other"),
  provider: "codex",
};

const git = (cwd: string, args: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const gitCore = yield* GitCore;
    const result = yield* gitCore.execute({
      operation: "GitCore.ownership.test.git",
      cwd,
      args,
      timeoutMs: 10_000,
    });
    return result.stdout.trim();
  });

/** A repository on `main` with two linked worktrees, each on its own branch. */
const makeRepository = (ownedName = "owned") =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const root = yield* fileSystem
      .makeTempDirectoryScoped({ prefix: "git-ownership-" })
      .pipe(Effect.flatMap((dir) => Effect.promise(() => fs.realpath(dir))));
    const repo = path.join(root, "repo");
    yield* fileSystem.makeDirectory(repo);
    yield* git(repo, ["init", "--initial-branch=main"]);
    yield* git(repo, ["config", "user.email", "test@test.com"]);
    yield* git(repo, ["config", "user.name", "Test"]);
    yield* fileSystem.writeFileString(path.join(repo, "README.md"), "# test\n");
    yield* git(repo, ["add", "."]);
    yield* git(repo, ["commit", "-m", "initial commit"]);
    const owned = path.join(root, ownedName);
    const other = path.join(root, "other");
    yield* git(repo, ["worktree", "add", "-b", "feature/owned", owned]);
    yield* git(repo, ["worktree", "add", "-b", "feature/other", other]);
    return { root, repo, owned, other };
  });

const claimInNewScope = (worktreePath: string, owner: WorktreeOwner) =>
  Effect.gen(function* () {
    const gitCore = yield* GitCore;
    const scope = yield* Scope.make();
    const claim = yield* Scope.provide(gitCore.claimWorktree(worktreePath, owner), scope);
    return { claim, release: Scope.close(scope, Exit.void) };
  });

const isOwned = (candidate: string) =>
  Effect.gen(function* () {
    const gitCore = yield* GitCore;
    return yield* gitCore.assertWorktreeNotOwned(candidate).pipe(
      Effect.as(false),
      Effect.catchTag("WorktreeOwnedError", () => Effect.succeed(true)),
    );
  });

const stagedFiles = (cwd: string) =>
  git(cwd, ["diff", "--cached", "--name-only"]).pipe(
    Effect.map((stdout) => stdout.split("\n").filter((line) => line.length > 0)),
  );

it.layer(TestLayer)("GitCore worktree ownership", (it) => {
  describe("claims", () => {
    it.effect("owns a worktree and every path inside it until the claim's scope closes", () =>
      Effect.gen(function* () {
        const { repo, owned } = yield* makeRepository();

        const { claim, release } = yield* claimInNewScope(owned, aetherOwner);
        expect(claim).toMatchObject({ worktreePath: owned, owner: aetherOwner });
        expect(yield* isOwned(owned)).toBe(true);
        expect(yield* isOwned(path.join(owned, "src", "new-file.ts"))).toBe(true);
        expect(yield* isOwned(repo)).toBe(false);

        yield* release;
        expect(yield* isOwned(owned)).toBe(false);
      }),
    );

    it.effect("refuses a second claim from any owner until the claim is released", () =>
      Effect.gen(function* () {
        const { owned } = yield* makeRepository();
        const gitCore = yield* GitCore;
        const secondClaim = (owner: WorktreeOwner) =>
          gitCore.claimWorktree(owned, owner).pipe(Effect.scoped, Effect.flip);

        const first = yield* claimInNewScope(owned, aetherOwner);
        expect(yield* secondClaim(otherOwner)).toMatchObject({
          _tag: "WorktreeOwnedError",
          worktreePath: owned,
          ownerThreadId: aetherOwner.threadId,
          provider: "aether",
        });
        expect(yield* secondClaim(aetherOwner)).toBeInstanceOf(WorktreeOwnedError);
        yield* first.release;

        const afterRelease = yield* claimInNewScope(owned, otherOwner);
        expect(afterRelease.claim.owner).toEqual(otherOwner);
        yield* afterRelease.release;
      }),
    );

    it.effect("refuses overlapping claims on a worktree nested in another", () =>
      Effect.gen(function* () {
        const { repo } = yield* makeRepository();
        const gitCore = yield* GitCore;
        const nested = path.join(repo, ".worktrees", "nested");
        yield* git(repo, ["worktree", "add", "-b", "feature/nested", nested]);
        const overlapping = (worktreePath: string) =>
          gitCore.claimWorktree(worktreePath, otherOwner).pipe(Effect.scoped, Effect.flip);

        // The main checkout is owned: its nested worktree cannot be claimed.
        const outer = yield* claimInNewScope(repo, aetherOwner);
        expect(yield* overlapping(nested)).toMatchObject({ worktreePath: repo });
        yield* outer.release;

        // The nested worktree is owned: the main checkout containing it cannot be claimed.
        const inner = yield* claimInNewScope(nested, aetherOwner);
        expect(inner.claim.worktreePath).toBe(nested);
        expect(yield* overlapping(repo)).toMatchObject({ worktreePath: nested });
        yield* inner.release;
      }),
    );

    it.effect("canonicalizes claimed and checked paths through symlinks", () =>
      Effect.gen(function* () {
        const { root, owned } = yield* makeRepository();
        const gitCore = yield* GitCore;
        const link = path.join(root, "owned-link");
        yield* Effect.promise(() => fs.symlink(owned, link, "dir"));

        const claim = yield* gitCore.claimWorktree(link, aetherOwner);
        expect(claim.worktreePath).toBe(owned);
        expect(yield* isOwned(path.join(link, "README.md"))).toBe(true);
        const refused = yield* gitCore.withMutation(link, Effect.void).pipe(Effect.flip);
        expect(refused).toBeInstanceOf(WorktreeOwnedError);
      }),
    );

    it.effect.skipIf(!nativeRealpathFoldsCase)(
      "refuses a differently cased path to the owned worktree on a case-insensitive filesystem",
      () =>
        Effect.gen(function* () {
          const { owned } = yield* makeRepository("Owned");
          const gitCore = yield* GitCore;
          const lowerCased = path.join(path.dirname(owned), "owned");
          yield* gitCore.claimWorktree(owned, aetherOwner);
          yield* Effect.promise(() => fs.writeFile(path.join(owned, "local.txt"), "local\n"));

          const staged = yield* gitCore
            .withMutation(lowerCased, gitCore.stageFiles(lowerCased, ["local.txt"]))
            .pipe(Effect.flip);
          expect(staged).toBeInstanceOf(WorktreeOwnedError);
          expect(yield* stagedFiles(owned)).toEqual([]);

          const written = yield* writeWorkspaceFileOutsideOwnedWorktree(
            {
              git: gitCore,
              workspaceFileSystem: yield* WorkspaceFileSystem,
              path: yield* Path.Path,
            },
            { cwd: lowerCased, relativePath: "README.md", contents: "overwritten\n" },
          ).pipe(Effect.flip);
          expect(written).toBeInstanceOf(WorktreeOwnedError);
          expect(
            yield* Effect.promise(() => fs.readFile(path.join(owned, "README.md"), "utf8")),
          ).toBe("# test\n");
        }),
    );

    it.effect("waits for an in-flight mutation of the repository before claiming", () =>
      Effect.gen(function* () {
        const { repo, owned } = yield* makeRepository();
        const gitCore = yield* GitCore;
        const events: Array<string> = [];
        const mutationStarted = yield* Deferred.make<void>();
        const finishMutation = yield* Deferred.make<void>();

        // The mutation runs in the main checkout; the claim targets a linked
        // worktree. Both share the repository's common directory, so one lock.
        const mutation = yield* gitCore
          .withMutation(
            repo,
            Effect.gen(function* () {
              yield* Deferred.succeed(mutationStarted, undefined);
              yield* Deferred.await(finishMutation);
              events.push("mutation finished");
            }),
          )
          .pipe(Effect.forkChild);
        yield* Deferred.await(mutationStarted);

        const claiming = yield* gitCore.claimWorktree(owned, aetherOwner).pipe(
          Effect.tap(() => Effect.sync(() => events.push("claimed"))),
          Effect.forkChild,
        );
        // The claim's own git lookup finishes well within this; only the
        // lock can keep it from registering.
        yield* Effect.promise(() => new Promise<void>((resolve) => setTimeout(resolve, 300)));
        expect(events).toEqual([]);
        expect(yield* isOwned(owned)).toBe(false);

        yield* Deferred.succeed(finishMutation, undefined);
        yield* Fiber.join(mutation);
        yield* Fiber.join(claiming);
        expect(events).toEqual(["mutation finished", "claimed"]);
        expect(yield* isOwned(owned)).toBe(true);
      }),
    );
  });

  describe("mutations", () => {
    it.effect("refuses a non-owner mutation inside the owned worktree before it runs", () =>
      Effect.gen(function* () {
        const { owned } = yield* makeRepository();
        const gitCore = yield* GitCore;
        yield* gitCore.claimWorktree(owned, aetherOwner);
        yield* Effect.promise(() => fs.writeFile(path.join(owned, "local.txt"), "local\n"));

        let ran = false;
        const refused = yield* gitCore
          .withMutation(
            owned,
            Effect.sync(() => {
              ran = true;
            }),
          )
          .pipe(Effect.flip);
        expect(refused).toMatchObject({
          _tag: "WorktreeOwnedError",
          worktreePath: owned,
          ownerThreadId: aetherOwner.threadId,
          provider: "aether",
        });
        expect(ran).toBe(false);

        yield* Effect.promise(() => fs.mkdir(path.join(owned, "src")));
        const fromSubdirectory = yield* gitCore
          .withMutation(path.join(owned, "src"), gitCore.stageFiles(owned, ["local.txt"]))
          .pipe(Effect.flip);
        expect(fromSubdirectory).toBeInstanceOf(WorktreeOwnedError);
        expect(yield* stagedFiles(owned)).toEqual([]);
      }),
    );

    it.effect("refuses removing the owned worktree, and git refuses deleting its branch", () =>
      Effect.gen(function* () {
        const { repo, owned } = yield* makeRepository();
        const gitCore = yield* GitCore;
        yield* gitCore.claimWorktree(owned, aetherOwner);

        const removal = yield* gitCore
          .withMutation(repo, gitCore.removeWorktree({ cwd: repo, path: owned, force: true }))
          .pipe(Effect.flip);
        expect(removal).toBeInstanceOf(WorktreeOwnedError);
        // A relative path is resolved against `cwd`, as git does.
        const relativeRemoval = yield* gitCore
          .withMutation(
            repo,
            gitCore.removeWorktree({ cwd: repo, path: path.relative(repo, owned), force: true }),
          )
          .pipe(Effect.flip);
        expect(relativeRemoval).toBeInstanceOf(WorktreeOwnedError);
        expect(existsSync(owned)).toBe(true);

        // Branches need no ownership check: git will not delete or force-move a
        // branch that another worktree has checked out.
        const deletion = yield* gitCore
          .withMutation(
            repo,
            gitCore.deleteBranch({ cwd: repo, branch: "feature/owned", force: true }),
          )
          .pipe(Effect.flip);
        expect(deletion._tag).toBe("GitCommandError");
        expect(yield* git(repo, ["branch", "--list", "feature/owned"])).toContain("feature/owned");
      }),
    );

    describe("the git.removeWorktree RPC", () => {
      /** A read model holding only the claim's owner, deleted or not. */
      const ownerReadModel = (deletedAt: string | null) =>
        Effect.succeed({ threads: [{ id: aetherOwner.threadId, deletedAt }] });

      it.effect(
        "stops a deleted owner's session, which releases its claim, before removing its worktree",
        () =>
          Effect.gen(function* () {
            const { repo, owned } = yield* makeRepository();
            const gitCore = yield* GitCore;
            const { release } = yield* claimInNewScope(owned, aetherOwner);
            // The mirror leaves the worktree dirty.
            yield* Effect.promise(() => fs.writeFile(path.join(owned, "mirrored.txt"), "cloud\n"));
            const events: Array<string> = [];

            yield* removeWorktreeReleasingDeletedOwner(
              {
                git: {
                  withMutation: gitCore.withMutation,
                  removeWorktree: (input) =>
                    gitCore.removeWorktree(input).pipe(
                      Effect.tapError((error) => Effect.sync(() => events.push(error._tag))),
                      Effect.tap(() => Effect.sync(() => events.push("removed"))),
                    ),
                },
                providerService: {
                  stopSession: (input) =>
                    Effect.sync(() => events.push(`stopSession ${input.threadId}`)).pipe(
                      Effect.andThen(release),
                    ),
                },
                readModel: ownerReadModel("2026-09-25T00:00:00.000Z"),
              },
              { cwd: repo, path: owned, force: true },
            );

            expect(events).toEqual([
              "WorktreeOwnedError",
              `stopSession ${aetherOwner.threadId}`,
              "removed",
            ]);
            expect(existsSync(owned)).toBe(false);
          }),
      );

      it.effect("still refuses a worktree whose owner thread is not deleted", () =>
        Effect.gen(function* () {
          const { repo, owned } = yield* makeRepository();
          const gitCore = yield* GitCore;
          yield* gitCore.claimWorktree(owned, aetherOwner);
          let stopped = false;

          const refused = yield* removeWorktreeReleasingDeletedOwner(
            {
              git: gitCore,
              providerService: {
                stopSession: () =>
                  Effect.sync(() => {
                    stopped = true;
                  }),
              },
              readModel: ownerReadModel(null),
            },
            { cwd: repo, path: owned, force: true },
          ).pipe(Effect.flip);

          expect(refused).toBeInstanceOf(WorktreeOwnedError);
          expect(stopped).toBe(false);
          expect(existsSync(owned)).toBe(true);
        }),
      );
    });

    it.effect("refuses creating a worktree inside the owned worktree", () =>
      Effect.gen(function* () {
        const { repo, owned } = yield* makeRepository();
        const gitCore = yield* GitCore;
        yield* gitCore.claimWorktree(owned, aetherOwner);
        const inside = path.join(owned, "nested");

        const created = yield* gitCore
          .withMutation(
            repo,
            gitCore.createWorktree({
              cwd: repo,
              branch: "main",
              newBranch: "feature/inside",
              path: inside,
            }),
          )
          .pipe(Effect.flip);
        expect(created).toBeInstanceOf(WorktreeOwnedError);

        const detached = yield* gitCore
          .withMutation(
            repo,
            gitCore.createDetachedWorktree({
              cwd: repo,
              ref: "main",
              newBranch: "feature/inside-detached",
              path: path.relative(repo, inside),
            }),
          )
          .pipe(Effect.flip);
        expect(detached).toBeInstanceOf(WorktreeOwnedError);

        expect(existsSync(inside)).toBe(false);
        expect(
          yield* git(repo, ["branch", "--list", "feature/inside", "feature/inside-detached"]),
        ).toBe("");
      }),
    );

    it.effect("lets the owner mutate its worktree with its own claim only", () =>
      Effect.gen(function* () {
        const { owned, other } = yield* makeRepository();
        const gitCore = yield* GitCore;
        const claim = yield* gitCore.claimWorktree(owned, aetherOwner);
        const otherClaim = yield* gitCore.claimWorktree(other, otherOwner);
        yield* Effect.promise(() => fs.writeFile(path.join(owned, "mirrored.txt"), "mirror\n"));

        const withForeignClaim = yield* gitCore
          .withMutation(owned, gitCore.stageFiles(owned, ["mirrored.txt"]), { claim: otherClaim })
          .pipe(Effect.flip);
        expect(withForeignClaim).toBeInstanceOf(WorktreeOwnedError);

        yield* gitCore.withMutation(owned, gitCore.stageFiles(owned, ["mirrored.txt"]), {
          claim,
        });
        expect(yield* stagedFiles(owned)).toEqual(["mirrored.txt"]);
      }),
    );

    it.effect("keeps mutations in the repository's other worktrees allowed", () =>
      Effect.gen(function* () {
        const { repo, owned, other } = yield* makeRepository();
        const gitCore = yield* GitCore;
        yield* gitCore.claimWorktree(owned, aetherOwner);
        yield* Effect.promise(() => fs.writeFile(path.join(other, "other.txt"), "other\n"));
        yield* Effect.promise(() => fs.writeFile(path.join(repo, "main.txt"), "main\n"));

        yield* gitCore.withMutation(other, gitCore.stageFiles(other, ["other.txt"]));
        yield* gitCore.withMutation(repo, gitCore.stageFiles(repo, ["main.txt"]));
        expect(yield* stagedFiles(other)).toEqual(["other.txt"]);
        expect(yield* stagedFiles(repo)).toEqual(["main.txt"]);

        yield* gitCore.withMutation(
          repo,
          gitCore.removeWorktree({ cwd: repo, path: other, force: true }),
        );
        expect(existsSync(other)).toBe(false);
      }),
    );
  });

  describe("guarded editor writes", () => {
    it.effect("refuses a file write into the owned worktree before touching the file", () =>
      Effect.gen(function* () {
        const { repo, owned } = yield* makeRepository();
        const gitCore = yield* GitCore;
        const services = {
          git: gitCore,
          workspaceFileSystem: yield* WorkspaceFileSystem,
          path: yield* Path.Path,
        };
        yield* gitCore.claimWorktree(owned, aetherOwner);

        const refused = yield* writeWorkspaceFileOutsideOwnedWorktree(services, {
          cwd: owned,
          relativePath: "README.md",
          contents: "overwritten\n",
        }).pipe(Effect.flip);
        expect(refused).toBeInstanceOf(WorktreeOwnedError);
        expect(
          yield* Effect.promise(() => fs.readFile(path.join(owned, "README.md"), "utf8")),
        ).toBe("# test\n");
        expect(toWsRpcError(refused, "Failed to write workspace file")).toMatchObject({
          code: "WORKTREE_OWNED",
          retryable: false,
          message: refused.message,
        });

        // A workspace rooted above the owned worktree cannot reach into it either.
        const nested = yield* writeWorkspaceFileOutsideOwnedWorktree(services, {
          cwd: path.dirname(owned),
          relativePath: path.join(path.basename(owned), "new.txt"),
          contents: "new\n",
        }).pipe(Effect.flip);
        expect(nested).toBeInstanceOf(WorktreeOwnedError);
        expect(existsSync(path.join(owned, "new.txt"))).toBe(false);

        yield* writeWorkspaceFileOutsideOwnedWorktree(services, {
          cwd: repo,
          relativePath: "README.md",
          contents: "edited\n",
        });
        expect(yield* Effect.promise(() => fs.readFile(path.join(repo, "README.md"), "utf8"))).toBe(
          "edited\n",
        );
      }),
    );
  });

  describe("execute stdin", () => {
    it.effect("pipes a patch to git apply", () =>
      Effect.gen(function* () {
        const { repo } = yield* makeRepository();
        const gitCore = yield* GitCore;
        const readme = path.join(repo, "README.md");
        yield* Effect.promise(() => fs.writeFile(readme, "# test\n\nmirrored line\n"));
        const patch = (yield* gitCore.execute({
          operation: "GitCore.ownership.test.diff",
          cwd: repo,
          args: ["diff", "--binary"],
        })).stdout;
        yield* git(repo, ["checkout", "--", "README.md"]);
        expect(yield* Effect.promise(() => fs.readFile(readme, "utf8"))).toBe("# test\n");

        yield* gitCore.execute({
          operation: "GitCore.ownership.test.apply",
          cwd: repo,
          args: ["apply", "-"],
          stdin: patch,
        });
        expect(yield* Effect.promise(() => fs.readFile(readme, "utf8"))).toBe(
          "# test\n\nmirrored line\n",
        );
      }),
    );
  });
});

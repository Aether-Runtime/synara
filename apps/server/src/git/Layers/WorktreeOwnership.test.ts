// FILE: WorktreeOwnership.test.ts
// Purpose: Exercises provider worktree ownership: claims, GitCore.withMutation enforcement,
//          the guarded editor write, and stdin for GitCore.execute.
// Layer: Server Git service tests
// Depends on: GitCoreLive (which provides WorktreeOwnership) plus real temporary Git repositories.
import { existsSync } from "node:fs";
import * as fs from "node:fs/promises";
import path from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import { ThreadId } from "@synara/contracts";
import { Deferred, Effect, Exit, Fiber, FileSystem, Layer, Path, Scope } from "effect";
import { describe, expect } from "vitest";

import { GitCoreLive } from "./GitCore.ts";
import { WorktreeOwnedError } from "../Errors.ts";
import { GitCore } from "../Services/GitCore.ts";
import { type WorktreeOwner, WorktreeOwnership } from "../Services/WorktreeOwnership.ts";
import { ServerConfig } from "../../config.ts";
import { WorkspaceLayerLive } from "../../workspace/runtimeLayer.ts";
import { WorkspaceFileSystem } from "../../workspace/Services/WorkspaceFileSystem.ts";
import { toWsRpcError, writeWorkspaceFileOutsideOwnedWorktree } from "../../wsRpc.ts";

const ServerConfigLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "synara-worktree-ownership-test-",
});
const TestLayer = Layer.mergeAll(
  NodeServices.layer,
  GitCoreLive.pipe(Layer.provide(ServerConfigLayer), Layer.provide(NodeServices.layer)),
  WorkspaceLayerLive.pipe(Layer.provide(NodeServices.layer)),
);

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
      operation: "WorktreeOwnership.test.git",
      cwd,
      args,
      timeoutMs: 10_000,
    });
    return result.stdout.trim();
  });

const pause = (ms: number) =>
  Effect.promise(() => new Promise<void>((resolve) => setTimeout(resolve, ms)));

/** A repository on `main` with two linked worktrees, each on its own branch. */
const makeRepository = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const root = yield* fileSystem
    .makeTempDirectoryScoped({ prefix: "worktree-ownership-" })
    .pipe(Effect.flatMap((dir) => Effect.promise(() => fs.realpath(dir))));
  const repo = path.join(root, "repo");
  yield* fileSystem.makeDirectory(repo);
  yield* git(repo, ["init", "--initial-branch=main"]);
  yield* git(repo, ["config", "user.email", "test@test.com"]);
  yield* git(repo, ["config", "user.name", "Test"]);
  yield* fileSystem.writeFileString(path.join(repo, "README.md"), "# test\n");
  yield* git(repo, ["add", "."]);
  yield* git(repo, ["commit", "-m", "initial commit"]);
  const owned = path.join(root, "owned");
  const other = path.join(root, "other");
  yield* git(repo, ["worktree", "add", "-b", "feature/owned", owned]);
  yield* git(repo, ["worktree", "add", "-b", "feature/other", other]);
  return { root, repo, owned, other };
});

const claimInNewScope = (worktreePath: string, owner: WorktreeOwner) =>
  Effect.gen(function* () {
    const ownership = yield* WorktreeOwnership;
    const scope = yield* Scope.make();
    const claim = yield* Scope.provide(ownership.claim(worktreePath, owner), scope);
    return { claim, release: Scope.close(scope, Exit.void) };
  });

const stagedFiles = (cwd: string) =>
  git(cwd, ["diff", "--cached", "--name-only"]).pipe(
    Effect.map((stdout) => stdout.split("\n").filter((line) => line.length > 0)),
  );

it.layer(TestLayer)("WorktreeOwnership", (it) => {
  describe("claims", () => {
    it.effect("owns a worktree and every path inside it until the claim's scope closes", () =>
      Effect.gen(function* () {
        const { repo, owned } = yield* makeRepository;
        const ownership = yield* WorktreeOwnership;

        const { claim, release } = yield* claimInNewScope(owned, aetherOwner);
        expect(claim.worktreePath).toBe(owned);
        expect(yield* ownership.ownerOf(owned)).toEqual({
          worktreePath: owned,
          owner: aetherOwner,
        });
        expect(yield* ownership.ownerOf(path.join(owned, "src", "new-file.ts"))).toEqual({
          worktreePath: owned,
          owner: aetherOwner,
        });
        expect(yield* ownership.ownerOf(repo)).toBeNull();

        yield* release;
        expect(yield* ownership.ownerOf(owned)).toBeNull();
      }),
    );

    it.effect("refuses another owner and keeps the owner's claims until the last is released", () =>
      Effect.gen(function* () {
        const { owned } = yield* makeRepository;
        const ownership = yield* WorktreeOwnership;

        const first = yield* claimInNewScope(owned, aetherOwner);
        const conflict = yield* ownership.claim(owned, otherOwner).pipe(Effect.scoped, Effect.flip);
        expect(conflict).toBeInstanceOf(WorktreeOwnedError);
        expect(conflict).toMatchObject({
          worktreePath: owned,
          ownerThreadId: aetherOwner.threadId,
          provider: "aether",
        });

        const second = yield* claimInNewScope(owned, aetherOwner);
        yield* first.release;
        expect(yield* ownership.ownerOf(owned)).toMatchObject({ owner: aetherOwner });
        yield* second.release;
        expect(yield* ownership.ownerOf(owned)).toBeNull();

        const afterRelease = yield* claimInNewScope(owned, otherOwner);
        expect(afterRelease.claim.owner).toEqual(otherOwner);
        yield* afterRelease.release;
      }),
    );

    it.effect("canonicalizes claimed and queried paths through symlinks", () =>
      Effect.gen(function* () {
        const { root, owned } = yield* makeRepository;
        const ownership = yield* WorktreeOwnership;
        const gitCore = yield* GitCore;
        const link = path.join(root, "owned-link");
        yield* Effect.promise(() => fs.symlink(owned, link, "dir"));

        const claim = yield* ownership.claim(link, aetherOwner);
        expect(claim.worktreePath).toBe(owned);
        expect(yield* ownership.ownerOf(owned)).toMatchObject({ worktreePath: owned });
        expect(yield* ownership.ownerOf(path.join(link, "README.md"))).toMatchObject({
          worktreePath: owned,
        });

        const refused = yield* gitCore.withMutation(link, Effect.void).pipe(Effect.flip);
        expect(refused).toBeInstanceOf(WorktreeOwnedError);
      }),
    );

    it.effect("waits for an in-flight mutation of the repository before claiming", () =>
      Effect.gen(function* () {
        const { repo, owned } = yield* makeRepository;
        const ownership = yield* WorktreeOwnership;
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

        const claiming = yield* ownership.claim(owned, aetherOwner).pipe(
          Effect.tap(() => Effect.sync(() => events.push("claimed"))),
          Effect.forkChild,
        );
        yield* pause(300);
        expect(events).toEqual([]);
        expect(yield* ownership.ownerOf(owned)).toBeNull();

        yield* Deferred.succeed(finishMutation, undefined);
        yield* Fiber.join(mutation);
        yield* Fiber.join(claiming);
        expect(events).toEqual(["mutation finished", "claimed"]);
        expect(yield* ownership.ownerOf(owned)).toMatchObject({ owner: aetherOwner });
      }),
    );
  });

  describe("GitCore.withMutation", () => {
    it.effect("refuses a non-owner mutation inside the owned worktree before it runs", () =>
      Effect.gen(function* () {
        const { owned } = yield* makeRepository;
        const ownership = yield* WorktreeOwnership;
        const gitCore = yield* GitCore;
        yield* ownership.claim(owned, aetherOwner);
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
        expect(refused).toBeInstanceOf(WorktreeOwnedError);
        expect(refused).toMatchObject({
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

    it.effect(
      "refuses removing the owned worktree or deleting its branch from the main checkout",
      () =>
        Effect.gen(function* () {
          const { repo, owned } = yield* makeRepository;
          const ownership = yield* WorktreeOwnership;
          const gitCore = yield* GitCore;
          yield* ownership.claim(owned, aetherOwner);
          const ownedHead = yield* git(repo, ["rev-parse", "refs/heads/feature/owned"]);

          const removal = yield* gitCore
            .withMutation(repo, gitCore.removeWorktree({ cwd: repo, path: owned, force: true }), {
              targets: { worktreePaths: [owned] },
            })
            .pipe(Effect.flip);
          expect(removal).toBeInstanceOf(WorktreeOwnedError);
          expect(existsSync(owned)).toBe(true);

          // `git update-ref -d` would delete a checked-out branch, so this is
          // refused by the declared target rather than by git itself.
          const deletion = yield* gitCore
            .withMutation(
              repo,
              gitCore.deleteBranchIfUnchanged({
                cwd: repo,
                branch: "feature/owned",
                expectedHead: ownedHead,
              }),
              { targets: { branches: ["feature/owned"] } },
            )
            .pipe(Effect.flip);
          expect(deletion).toBeInstanceOf(WorktreeOwnedError);
          expect(yield* git(repo, ["rev-parse", "refs/heads/feature/owned"])).toBe(ownedHead);
        }),
    );

    it.effect("lets the owner mutate its worktree with its own claim only", () =>
      Effect.gen(function* () {
        const { owned, other } = yield* makeRepository;
        const ownership = yield* WorktreeOwnership;
        const gitCore = yield* GitCore;
        const claim = yield* ownership.claim(owned, aetherOwner);
        const otherClaim = yield* ownership.claim(other, otherOwner);
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
        const { repo, owned, other } = yield* makeRepository;
        const ownership = yield* WorktreeOwnership;
        const gitCore = yield* GitCore;
        yield* ownership.claim(owned, aetherOwner);
        yield* Effect.promise(() => fs.writeFile(path.join(other, "other.txt"), "other\n"));
        yield* Effect.promise(() => fs.writeFile(path.join(repo, "main.txt"), "main\n"));

        yield* gitCore.withMutation(other, gitCore.stageFiles(other, ["other.txt"]));
        yield* gitCore.withMutation(repo, gitCore.stageFiles(repo, ["main.txt"]));
        expect(yield* stagedFiles(other)).toEqual(["other.txt"]);
        expect(yield* stagedFiles(repo)).toEqual(["main.txt"]);

        yield* gitCore.withMutation(
          repo,
          gitCore.removeWorktree({ cwd: repo, path: other, force: true }),
          { targets: { worktreePaths: [other], branches: ["feature/other"] } },
        );
        expect(existsSync(other)).toBe(false);
      }),
    );
  });

  describe("guarded editor writes", () => {
    it.effect("refuses a file write into the owned worktree before touching the file", () =>
      Effect.gen(function* () {
        const { repo, owned } = yield* makeRepository;
        const ownership = yield* WorktreeOwnership;
        const services = {
          worktreeOwnership: ownership,
          workspaceFileSystem: yield* WorkspaceFileSystem,
          path: yield* Path.Path,
        };
        yield* ownership.claim(owned, aetherOwner);

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

  describe("GitCore.execute stdin", () => {
    it.effect("pipes a patch to git apply as a string or as bytes", () =>
      Effect.gen(function* () {
        const { repo } = yield* makeRepository;
        const gitCore = yield* GitCore;
        const readme = path.join(repo, "README.md");
        yield* Effect.promise(() => fs.writeFile(readme, "# test\n\nmirrored line\n"));
        const patch = (yield* gitCore.execute({
          operation: "WorktreeOwnership.test.diff",
          cwd: repo,
          args: ["diff", "--binary"],
        })).stdout;
        yield* git(repo, ["checkout", "--", "README.md"]);
        expect(yield* Effect.promise(() => fs.readFile(readme, "utf8"))).toBe("# test\n");

        yield* gitCore.execute({
          operation: "WorktreeOwnership.test.apply",
          cwd: repo,
          args: ["apply", "-"],
          stdin: patch,
        });
        expect(yield* Effect.promise(() => fs.readFile(readme, "utf8"))).toBe(
          "# test\n\nmirrored line\n",
        );

        yield* gitCore.execute({
          operation: "WorktreeOwnership.test.applyReverse",
          cwd: repo,
          args: ["apply", "--reverse", "-"],
          stdin: new TextEncoder().encode(patch),
        });
        expect(yield* Effect.promise(() => fs.readFile(readme, "utf8"))).toBe("# test\n");
      }),
    );
  });
});

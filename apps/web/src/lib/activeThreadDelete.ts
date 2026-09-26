// FILE: activeThreadDelete.ts
// Purpose: Owns the shared server-delete sequence for active threads, including the worktree
// the user chooses to delete with them.
// Layer: Web orchestration helper
// Exports: deleteActiveThreadFromClient

import type { ThreadId } from "@synara/contracts";
import { terminalScopeIdsForThread } from "@synara/shared/terminalThreads";
import { collectSubagentDescendants } from "@synara/shared/threadHierarchy";

import { readNativeApi } from "../nativeApi";
import { useStore } from "../store";
import { getThreadFromState, getThreadsFromState } from "../threadDerivation";
import type { Thread } from "../types";
import { getOrphanedWorktreePathForThread } from "../worktreeCleanup";
import { reconcileDeletedThreadFromClient } from "./deletedThreadClientReconciliation";
import { newCommandId } from "./utils";

// The terminal runtime pulls in xterm and its addons (~223 KB gzip). Importing it
// statically here anchored the whole terminal stack into the eager sidebar/router
// graph, so every page load paid for it. Deleting a thread is a rare, already
// async user action, so the chunk is fetched on demand instead. The import is
// awaited (never fire-and-forget) so disposal cannot race the rest of the delete
// sequence, and the resolved module is cached by the module system afterwards.
async function disposeThreadTerminalRuntimes(threadId: ThreadId): Promise<void> {
  try {
    const { terminalRuntimeRegistry } =
      await import("../components/terminal/terminalRuntimeRegistry");
    for (const scopeId of terminalScopeIdsForThread(threadId)) {
      terminalRuntimeRegistry.disposeThread(scopeId);
    }
  } catch (error) {
    // A failed chunk fetch must not abort the delete sequence: the durable delete
    // already landed server-side and the server owns provider/terminal teardown.
    console.error("Failed to dispose terminal runtimes for deleted thread", { threadId, error });
  }
}

export async function deleteActiveThreadFromClient<TPrepared = undefined>(input: {
  readonly threadId: ThreadId;
  /** Delete native descendants before their parent, invoking callbacks for each accepted delete. */
  readonly includeSubagentDescendants?: boolean;
  readonly deletedThreadIds?: ReadonlySet<ThreadId>;
  readonly reconcileDeletedThread?: boolean;
  readonly worktreeCleanupMode?: "prompt" | "skip";
  readonly prepareForDelete?: (thread: Thread) => TPrepared;
  readonly onDeleted: (input: {
    thread: Thread;
    prepared: TPrepared | undefined;
  }) => void | Promise<void>;
}): Promise<void> {
  const api = readNativeApi();
  if (!api) return;
  const state = useStore.getState();
  const thread = getThreadFromState(state, input.threadId);
  if (!thread) return;
  const allThreads = getThreadsFromState(state);
  const threadsToDelete = input.includeSubagentDescendants
    ? [...collectSubagentDescendants(allThreads, thread.id).toReversed(), thread]
    : [thread];
  const deletedThreadIds = new Set([
    ...(input.deletedThreadIds ?? []),
    ...threadsToDelete.map((candidate) => candidate.id),
  ]);
  const survivingThreads = allThreads.filter(
    (candidate) => candidate.id === input.threadId || !deletedThreadIds.has(candidate.id),
  );
  const orphanedWorktreePath = getOrphanedWorktreePathForThread(survivingThreads, input.threadId);
  const removeWorktreePath =
    (input.worktreeCleanupMode ?? "prompt") === "prompt" &&
    orphanedWorktreePath !== null &&
    (await api.dialogs.confirm(
      [
        "This thread is the only one linked to this worktree:",
        orphanedWorktreePath,
        "",
        "Delete the worktree too?",
      ].join("\n"),
    ))
      ? orphanedWorktreePath
      : null;

  // Children go first: if a delete fails, their surviving parent remains reachable.
  // The requested thread's delete comes last and carries the worktree, so it is removed only
  // after the entire subtree was accepted. The server removes it once the thread's session has
  // stopped, whether or not this client is still connected.
  for (const deletedThread of threadsToDelete) {
    const prepared = input.prepareForDelete?.(deletedThread);
    await api.orchestration.dispatchCommand({
      type: "thread.delete",
      commandId: newCommandId(),
      threadId: deletedThread.id,
      ...(removeWorktreePath !== null && deletedThread.id === input.threadId
        ? { removeWorktreePath }
        : {}),
    });
    // Provider and terminal cleanup are owned by the server-side lifecycle
    // reactor. Dispose only the local renderer after the durable delete intent
    // was accepted, so a rejected delete never tears down a live client session.
    await disposeThreadTerminalRuntimes(deletedThread.id);
    if (input.reconcileDeletedThread ?? true) {
      void reconcileDeletedThreadFromClient({
        threadId: deletedThread.id,
        removeDeletedThreadFromClientState: useStore.getState().removeDeletedThreadFromClientState,
      });
    }
    await input.onDeleted({ thread: deletedThread, prepared });
  }
}

// FILE: providerCapabilityReadModel.ts
// Purpose: Shared test read model for provider capability refusals: one project and one thread
//          on a chosen provider, optionally in a worktree, with a goal, or past its first turn.
// Layer: Server test utility (imported by *.test.ts only; never by production code)

import {
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  EventId,
  MessageId,
  ProjectId,
  ThreadId,
  TurnId,
  type ModelSelection,
  type OrchestrationReadModel,
} from "@synara/contracts";
import { Effect } from "effect";

import { createEmptyReadModel, projectEvent } from "../projector.ts";

export const NOW = "2026-09-25T12:00:00.000Z";
export const PROJECT_ID = ProjectId.makeUnsafe("project-capabilities");
export const THREAD_ID = ThreadId.makeUnsafe("thread-capabilities");
export const NEW_THREAD_ID = ThreadId.makeUnsafe("thread-capabilities-new");
export const MESSAGE_ID = MessageId.makeUnsafe("message-capabilities");
const TURN_ID = TurnId.makeUnsafe("turn-capabilities");
export const WORKTREE_PATH = "/tmp/project/.worktrees/capabilities";

export const AETHER: ModelSelection = { provider: "aether", model: "claude-opus-5-5" };
export const CODEX: ModelSelection = { provider: "codex", model: "gpt-5-codex" };
export const CLAUDE: ModelSelection = { provider: "claudeAgent", model: "claude-opus-4-6" };

export async function makeReadModel(input: {
  readonly modelSelection: ModelSelection;
  readonly worktreePath?: string | null;
  readonly goal?: string;
  readonly goalPaused?: boolean;
}): Promise<OrchestrationReadModel> {
  const withProject = await Effect.runPromise(
    projectEvent(createEmptyReadModel(NOW), {
      sequence: 1,
      eventId: EventId.makeUnsafe("evt-project-create"),
      aggregateKind: "project",
      aggregateId: PROJECT_ID,
      type: "project.created",
      occurredAt: NOW,
      commandId: CommandId.makeUnsafe("cmd-project-create"),
      causationEventId: null,
      correlationId: CommandId.makeUnsafe("cmd-project-create"),
      metadata: {},
      payload: {
        projectId: PROJECT_ID,
        kind: "project",
        title: "Project",
        workspaceRoot: "/tmp/project",
        defaultModelSelection: null,
        scripts: [],
        createdAt: NOW,
        updatedAt: NOW,
      },
    }),
  );
  const worktreePath = input.worktreePath === undefined ? WORKTREE_PATH : input.worktreePath;
  const readModel = await Effect.runPromise(
    projectEvent(withProject, {
      sequence: 2,
      eventId: EventId.makeUnsafe("evt-thread-create"),
      aggregateKind: "thread",
      aggregateId: THREAD_ID,
      type: "thread.created",
      occurredAt: NOW,
      commandId: CommandId.makeUnsafe("cmd-thread-create"),
      causationEventId: null,
      correlationId: CommandId.makeUnsafe("cmd-thread-create"),
      metadata: {},
      payload: {
        threadId: THREAD_ID,
        projectId: PROJECT_ID,
        title: "Thread",
        modelSelection: input.modelSelection,
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access",
        envMode: worktreePath === null ? "local" : "worktree",
        branch: worktreePath === null ? null : "synara/capabilities",
        worktreePath,
        parentThreadId: null,
        subagentAgentId: null,
        subagentNickname: null,
        subagentRole: null,
        forkSourceThreadId: null,
        sidechatSourceThreadId: null,
        handoff: null,
        createdAt: NOW,
        updatedAt: NOW,
      },
    }),
  );
  const [thread] = readModel.threads;
  return {
    ...readModel,
    threads: [
      {
        ...thread!,
        messages: [
          {
            id: MESSAGE_ID,
            role: "user",
            text: "first request",
            turnId: TURN_ID,
            streaming: false,
            source: "native",
            createdAt: NOW,
            updatedAt: NOW,
          },
        ],
        ...(input.goal !== undefined
          ? {
              goal: input.goal,
              goalStartedAt: NOW,
              goalPausedAt: input.goalPaused ? NOW : null,
            }
          : {}),
      },
    ],
  };
}

/** The thread after its first turn started: bound to a session of `providerName`. */
export function withSession(readModel: OrchestrationReadModel, providerName: string) {
  const [thread] = readModel.threads;
  return {
    ...readModel,
    threads: [
      {
        ...thread!,
        session: {
          threadId: THREAD_ID,
          status: "ready" as const,
          providerName,
          runtimeMode: "full-access" as const,
          activeTurnId: null,
          lastError: null,
          updatedAt: NOW,
        },
      },
    ],
  };
}

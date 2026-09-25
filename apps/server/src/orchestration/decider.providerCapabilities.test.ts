// FILE: decider.providerCapabilities.test.ts
// Purpose: Covers the decider refusing features a provider's capabilities rule out
//          (Aether: no agent gateway, a provider-owned worktree, no conversation
//          rollback) before any event is emitted, and leaving Codex and Claude as-is.

import {
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  EventId,
  MessageId,
  ProjectId,
  ThreadId,
  TurnId,
  type ModelSelection,
  type OrchestrationCommand,
  type OrchestrationReadModel,
} from "@synara/contracts";
import { Effect } from "effect";
import { describe, expect, it } from "vitest";

import { decideOrchestrationCommand } from "./decider.ts";
import { createEmptyReadModel, projectEvent } from "./projector.ts";

const NOW = "2026-09-25T12:00:00.000Z";
const PROJECT_ID = ProjectId.makeUnsafe("project-capabilities");
const THREAD_ID = ThreadId.makeUnsafe("thread-capabilities");
const NEW_THREAD_ID = ThreadId.makeUnsafe("thread-capabilities-new");
const MESSAGE_ID = MessageId.makeUnsafe("message-capabilities");
const TURN_ID = TurnId.makeUnsafe("turn-capabilities");
const WORKTREE_PATH = "/tmp/project/.worktrees/capabilities";

const AETHER: ModelSelection = { provider: "aether", model: "claude-opus-5-5" };
const CODEX: ModelSelection = { provider: "codex", model: "gpt-5-codex" };
const CLAUDE: ModelSelection = { provider: "claudeAgent", model: "claude-opus-4-6" };

async function makeReadModel(input: {
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

async function decide(command: OrchestrationCommand, readModel: OrchestrationReadModel) {
  const decided = await Effect.runPromise(decideOrchestrationCommand({ command, readModel }));
  return Array.isArray(decided) ? decided : [decided];
}

function refusal(command: OrchestrationCommand, readModel: OrchestrationReadModel) {
  return Effect.runPromise(
    decideOrchestrationCommand({ command, readModel }).pipe(Effect.flip),
  ).then((error) => error.message);
}

const commandId = (id: string) => CommandId.makeUnsafe(`cmd-${id}`);

const goalSet = (goal = "Ship the feature"): OrchestrationCommand => ({
  type: "thread.meta.update",
  commandId: commandId("goal-set"),
  threadId: THREAD_ID,
  goal,
});

const turnStart = (
  text: string,
  options: {
    readonly enableComputerControl?: boolean;
    readonly modelSelection?: ModelSelection;
  } = {},
): OrchestrationCommand => ({
  type: "thread.turn.start",
  commandId: commandId("turn-start"),
  threadId: THREAD_ID,
  message: {
    messageId: MessageId.makeUnsafe("message-next"),
    role: "user",
    text,
    attachments: [],
  },
  interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
  runtimeMode: "full-access",
  createdAt: NOW,
  ...options,
});

const createFromSource = (
  type: "thread.fork.create" | "thread.handoff.create",
  modelSelection: ModelSelection,
): OrchestrationCommand => ({
  type,
  commandId: commandId(type),
  threadId: NEW_THREAD_ID,
  sourceThreadId: THREAD_ID,
  projectId: PROJECT_ID,
  title: "New thread",
  modelSelection,
  runtimeMode: "full-access",
  interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
  envMode: "worktree",
  branch: "synara/capabilities",
  worktreePath: WORKTREE_PATH,
  createBranchFlowCompleted: false,
  ...(type === "thread.fork.create" ? { sidechatSourceThreadId: THREAD_ID } : {}),
  importedMessages: [],
  createdAt: NOW,
});

const checkpointRevert = (scope: "thread" | "files"): OrchestrationCommand => ({
  type: "thread.checkpoint.revert",
  commandId: commandId(`revert-${scope}`),
  threadId: THREAD_ID,
  turnCount: 0,
  scope,
  createdAt: NOW,
});

const editAndResend = (): OrchestrationCommand => ({
  type: "thread.message.edit-and-resend",
  commandId: commandId("edit-resend"),
  threadId: THREAD_ID,
  messageId: MESSAGE_ID,
  text: "edited request",
  interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
  runtimeMode: "full-access",
  createdAt: NOW,
});

describe("decider provider capability gating", () => {
  describe("an Aether thread", () => {
    it("refuses to set, resume or continue a thread goal", async () => {
      expect(await refusal(goalSet(), await makeReadModel({ modelSelection: AETHER }))).toContain(
        "Thread goals are unavailable with Aether",
      );
      const paused = await makeReadModel({
        modelSelection: AETHER,
        goal: "Ship it",
        goalPaused: true,
      });
      expect(
        await refusal(
          {
            type: "thread.meta.update",
            commandId: commandId("goal-resume"),
            threadId: THREAD_ID,
            goalPaused: false,
          },
          paused,
        ),
      ).toContain("agent gateway");
      expect(
        await refusal(
          {
            type: "thread.goal.continue",
            commandId: commandId("goal-continue"),
            threadId: THREAD_ID,
            goalStartedAt: NOW,
            trigger: "turn-completed",
            createdAt: NOW,
          },
          paused,
        ),
      ).toContain("Thread goals are unavailable with Aether");
    });

    it("still lets a goal be paused or cleared", async () => {
      const readModel = await makeReadModel({ modelSelection: AETHER, goal: "Ship it" });
      for (const command of [
        {
          type: "thread.meta.update" as const,
          commandId: commandId("goal-pause"),
          threadId: THREAD_ID,
          goalPaused: true,
        },
        goalSet(""),
      ]) {
        expect((await decide(command, readModel))[0]?.type).toBe("thread.meta-updated");
      }
    });

    it("refuses to switch a thread with an active goal to Aether", async () => {
      const readModel = await makeReadModel({ modelSelection: CODEX, goal: "Ship it" });
      expect(
        await refusal(
          {
            type: "thread.meta.update",
            commandId: commandId("switch-to-aether"),
            threadId: THREAD_ID,
            modelSelection: AETHER,
          },
          readModel,
        ),
      ).toContain("Thread goals are unavailable with Aether");
    });

    it("refuses a first turn that would carry an active goal onto Aether", async () => {
      const readModel = await makeReadModel({ modelSelection: CODEX, goal: "Ship it" });
      expect(await refusal(turnStart("hello", { modelSelection: AETHER }), readModel)).toBe(
        "Orchestration command invariant failed (thread.turn.start): Thread goals are unavailable with Aether: its agent cannot reach Synara's agent gateway, so it could never mark a goal as achieved.",
      );
      const events = await decide(turnStart("hello"), readModel);
      expect(events.map((event) => event.type)).toContain("thread.turn-start-requested");
    });

    it("accepts a first turn onto Aether when the thread's goal is paused", async () => {
      // A paused goal is never sent to the provider, so nothing waits on the gateway.
      const paused = await makeReadModel({
        modelSelection: CODEX,
        goal: "Ship it",
        goalPaused: true,
      });
      const events = await decide(turnStart("hello", { modelSelection: AETHER }), paused);
      expect(events.map((event) => event.type)).toContain("thread.turn-start-requested");
    });

    it("refuses moving an idle thread to the local checkout", async () => {
      // No session and no live worktree claim: the descriptor alone refuses it.
      expect(
        await refusal(
          {
            type: "thread.meta.update",
            commandId: commandId("move-to-local"),
            threadId: THREAD_ID,
            envMode: "local",
            worktreePath: null,
          },
          await makeReadModel({ modelSelection: AETHER }),
        ),
      ).toBe(
        "Orchestration command invariant failed (thread.meta.update): Aether threads run only in a worktree that Aether owns, never in the local checkout.",
      );
    });

    it("refuses Computer Use on every dispatch path", async () => {
      const readModel = await makeReadModel({ modelSelection: AETHER });
      expect(await refusal(turnStart("hello", { enableComputerControl: true }), readModel)).toBe(
        "Orchestration command invariant failed (thread.turn.start): Computer Use, browser automation and device tools are unavailable with Aether: its agent cannot reach Synara's agent gateway.",
      );
      expect(await refusal(turnStart("/computer-use open Calculator"), readModel)).toContain(
        "Computer Use",
      );
      expect(
        await refusal(
          {
            type: "thread.turn.dispatch-queued",
            commandId: commandId("dispatch-queued"),
            threadId: THREAD_ID,
            messageId: MESSAGE_ID,
            interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
            runtimeMode: "full-access",
            computerControlMode: "chat",
            createdAt: NOW,
          },
          readModel,
        ),
      ).toContain("Computer Use");
    });

    it("refuses a turn outside the worktree Aether owns", async () => {
      expect(
        await refusal(
          turnStart("hello"),
          await makeReadModel({ modelSelection: AETHER, worktreePath: null }),
        ),
      ).toContain(
        "Aether threads run only in a worktree that Aether owns, never in the local checkout.",
      );
      const events = await decide(
        turnStart("hello"),
        await makeReadModel({ modelSelection: AETHER }),
      );
      expect(events.map((event) => event.type)).toContain("thread.turn-start-requested");
    });

    it("refuses a side chat, whichever provider it targets", async () => {
      const readModel = await makeReadModel({ modelSelection: AETHER });
      for (const target of [AETHER, CLAUDE]) {
        expect(await refusal(createFromSource("thread.fork.create", target), readModel)).toContain(
          "Side chats are unavailable in Aether threads",
        );
      }
      expect(
        await refusal(
          createFromSource("thread.fork.create", AETHER),
          await makeReadModel({ modelSelection: CODEX }),
        ),
      ).toContain("Side chats are unavailable in Aether threads");
    });

    it("refuses a provider handoff to or from Aether", async () => {
      expect(
        await refusal(
          createFromSource("thread.handoff.create", CLAUDE),
          await makeReadModel({ modelSelection: AETHER }),
        ),
      ).toContain("Handing a thread off to or from Aether is not available yet");
      expect(
        await refusal(
          createFromSource("thread.handoff.create", AETHER),
          await makeReadModel({ modelSelection: CODEX }),
        ),
      ).toContain("Handing a thread off to or from Aether is not available yet");
    });

    it("refuses a checkpoint revert of either scope before any event", async () => {
      const readModel = await makeReadModel({ modelSelection: AETHER });
      for (const scope of ["thread", "files"] as const) {
        expect(await refusal(checkpointRevert(scope), readModel)).toContain(
          "Aether cannot roll back its conversation",
        );
      }
      expect(
        await refusal(
          {
            type: "thread.conversation.rollback",
            commandId: commandId("conversation-rollback"),
            threadId: THREAD_ID,
            messageId: MESSAGE_ID,
            numTurns: 1,
            createdAt: NOW,
          },
          readModel,
        ),
      ).toContain("Aether cannot roll back its conversation");
    });

    it("refuses edit-and-resend up front, before the session is marked starting", async () => {
      expect(await refusal(editAndResend(), await makeReadModel({ modelSelection: AETHER }))).toBe(
        "Orchestration command invariant failed (thread.message.edit-and-resend): Aether cannot roll back its conversation, so reverting changes or editing earlier messages is unavailable in this thread.",
      );
    });
  });

  it("refuses a gated command on a thread bound to an unknown provider", async () => {
    const readModel = await makeReadModel({ modelSelection: CODEX });
    const [thread] = readModel.threads;
    const unknownSession = {
      ...readModel,
      threads: [
        {
          ...thread!,
          session: {
            threadId: THREAD_ID,
            status: "ready" as const,
            providerName: "retired-provider",
            runtimeMode: "full-access" as const,
            activeTurnId: null,
            lastError: null,
            updatedAt: NOW,
          },
        },
      ],
    };
    expect(await refusal(checkpointRevert("thread"), unknownSession)).toContain(
      "bound to unknown provider 'retired-provider'",
    );
  });

  describe.each([
    ["Codex", CODEX],
    ["Claude", CLAUDE],
  ] as const)("a %s thread", (_name, modelSelection) => {
    it("accepts every gated command", async () => {
      const readModel = await makeReadModel({ modelSelection, worktreePath: null });
      const accepted: ReadonlyArray<[OrchestrationCommand, string]> = [
        [goalSet(), "thread.meta-updated"],
        [
          {
            type: "thread.meta.update",
            commandId: commandId("move-to-local"),
            threadId: THREAD_ID,
            envMode: "local",
            worktreePath: null,
          },
          "thread.meta-updated",
        ],
        [turnStart("hello", { enableComputerControl: true }), "thread.turn-start-requested"],
        [createFromSource("thread.fork.create", modelSelection), "thread.created"],
        [createFromSource("thread.handoff.create", CLAUDE), "thread.created"],
        [checkpointRevert("thread"), "thread.checkpoint-revert-requested"],
        [editAndResend(), "thread.message-edit-resend-requested"],
      ];
      for (const [command, eventType] of accepted) {
        const events = await decide(command, readModel);
        expect(events.map((event) => event.type)).toContain(eventType);
      }
    });
  });
});

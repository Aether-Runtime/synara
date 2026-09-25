// FILE: aetherFakeTask.ts
// Purpose: A fake Aether API holding one task in its durable-conversation shape, for adapter
// tests: it answers the adapter's REST calls, and the test moves the task along (text, tools,
// questions, plans, settles) the way the VM would.
// Layer: Test support (Aether provider).

import { randomUUID } from "node:crypto";

import { Effect, Schema } from "effect";

import {
  AetherApiConflictError,
  AetherApiPaymentRequiredError,
  AetherApiTransportError,
  type AetherApiError,
} from "../aetherApiErrors.ts";
import {
  AetherConversationTurnsPage,
  AetherCreateTaskResponse,
  AetherLiveConversationSnapshot,
  AetherProjectList,
  AetherRespondToTaskResponse,
  AetherTurnActivityPage,
  type AetherCreateTaskRequest,
  type AetherCreateTaskResponse as CreatedTask,
  type AetherRespondToTaskRequest,
  type AetherRespondToTaskResponse as Responded,
} from "../aetherApiSchemas.ts";
import type { AetherApiClientShape } from "../../Services/AetherApiClient.ts";
import { PROJECT_ID, TASK_ID, WORKSPACE_ID } from "./aetherApiFixtures.ts";

type Json = Record<string, unknown>;
type TurnState = "queued" | "running" | "awaiting_input" | "settled" | "errored";

export const FAKE_REPO_URL = "https://github.com/acme/widgets.git";

export interface FakeTurn {
  readonly id: string;
  readonly root: Json;
  state: TurnState;
  revision: number;
  readonly items: Json[];
}

const decode = <S extends Schema.Top & { readonly DecodingServices: never }>(
  schema: S,
  body: unknown,
): S["Type"] => Schema.decodeUnknownSync(schema)(body);

/** The fake task and the client that reads and writes it. */
export function makeFakeAetherTask() {
  let task: { status: string; awaiting: Json; error?: string } | undefined;
  const turns: FakeTurn[] = [];
  let head = 1;
  const open = new Map<string, Json>();
  const closed = new Map<string, Json>();
  const keys = new Set<string>();
  const messages = new Map<string, string>();
  const calls = {
    create: [] as AetherCreateTaskRequest[],
    respond: [] as AetherRespondToTaskRequest[],
    stop: [] as Json[],
  };
  const faults: {
    loseCreate: boolean;
    loseRespond: boolean;
    staleAnswer: boolean;
    payment: boolean;
    /** The next create, or respond, fails with this and changes nothing. */
    refuseCreate: AetherApiError | undefined;
    refuseRespond: AetherApiError | undefined;
  } = {
    loseCreate: false,
    loseRespond: false,
    staleAnswer: false,
    payment: false,
    refuseCreate: undefined,
    refuseRespond: undefined,
  };
  /** Called when a turn starts running: the test scripts what the VM does. */
  const hooks: { onTurn: (turn: FakeTurn) => void } = { onTurn: () => {} };

  const bump = (turn?: FakeTurn) => {
    head += 1;
    if (turn !== undefined) turn.revision = head;
  };
  const idle = () => {
    task = { status: "awaiting_input", awaiting: { kind: "message" } };
  };
  const running = () =>
    turns.find((turn) => turn.state === "running" || turn.state === "awaiting_input");
  const startTurn = (root: Json, queued: boolean) => {
    const turn: FakeTurn = {
      id: randomUUID(),
      root,
      state: queued ? "queued" : "running",
      revision: 0,
      items: [],
    };
    turns.push(turn);
    if (!queued) task = { status: "processing", awaiting: {} };
    bump(turn);
    if (!queued) hooks.onTurn(turn);
    return turn;
  };
  let messageCount = 0;
  const messageId = () => `msg-user-${(messageCount += 1)}`;
  const transportError = (endpoint: string) =>
    new AetherApiTransportError({ endpoint, cause: new Error("socket hang up") });

  const taskBody = () => {
    const status = task?.status ?? "queued";
    return {
      status,
      run_context: { workspace_id: WORKSPACE_ID },
      ...(status === "awaiting_input" ? { awaiting_input: task!.awaiting } : {}),
      ...(status === "errored" ? { error: task!.error } : {}),
    };
  };

  const client: AetherApiClientShape = {
    connection: Effect.die("the fake has no connection"),
    getProfile: () => Effect.die("unused"),
    listProjects: () =>
      Effect.succeed(
        decode(AetherProjectList, { projects: [{ id: PROJECT_ID, repo_url: FAKE_REPO_URL }] })
          .projects,
      ),
    createTask: (request) =>
      Effect.suspend((): Effect.Effect<CreatedTask, AetherApiError> => {
        calls.create.push(request);
        if (faults.payment) {
          return Effect.fail(
            new AetherApiPaymentRequiredError({
              endpoint: "POST /tasks",
              code: "workspace_runtime_not_admitted",
              tier: "free",
              vmSize: "small",
              serverMessage: "Your plan does not include cloud workspaces.",
            }),
          );
        }
        const refused = faults.refuseCreate;
        if (refused !== undefined) {
          faults.refuseCreate = undefined;
          return Effect.fail(refused);
        }
        if (!keys.has(request.idempotency_key)) {
          keys.add(request.idempotency_key);
          startTurn({ id: messageId(), role: "user", content: request.prompt }, false);
        }
        if (faults.loseCreate) {
          faults.loseCreate = false;
          return Effect.fail(transportError("POST /tasks"));
        }
        return Effect.succeed(decode(AetherCreateTaskResponse, { id: TASK_ID }));
      }),
    respondToTask: (_taskId, request) =>
      Effect.suspend((): Effect.Effect<Responded, AetherApiError> => {
        calls.respond.push(request);
        const refused = faults.refuseRespond;
        if (refused !== undefined) {
          faults.refuseRespond = undefined;
          return Effect.fail(refused);
        }
        let id = messages.get(request.client_message_id);
        if (id === undefined) {
          const answer = request.tool_response;
          if (answer !== undefined) {
            if (faults.staleAnswer || task?.awaiting.tool_id !== answer.expected_tool_id) {
              faults.staleAnswer = false;
              return Effect.fail(
                new AetherApiConflictError({
                  endpoint: "POST /tasks/{taskID}/respond",
                  body: { error: "The pending input changed.", code: "stale_pending_input" },
                }),
              );
            }
          }
          id = messageId();
          messages.set(request.client_message_id, id);
          const row: Json = {
            id,
            role: "user",
            content: request.message,
            clientMessageId: request.client_message_id,
            ...(answer === undefined
              ? {}
              : { toolResponse: { tool_name: answer.tool_name, data: answer.data } }),
          };
          const current = running();
          if (answer !== undefined && current !== undefined) {
            current.items.push(row);
            current.state = "running";
            task = { status: "processing", awaiting: {} };
            bump(current);
            hooks.onTurn(current);
          } else if (current !== undefined && request.queue_for_later !== true) {
            current.items.push(row);
            bump(current);
          } else {
            startTurn(row, current !== undefined);
          }
        }
        if (faults.loseRespond) {
          faults.loseRespond = false;
          return Effect.fail(transportError("POST /tasks/{taskID}/respond"));
        }
        return Effect.succeed(decode(AetherRespondToTaskResponse, { message_id: id }));
      }),
    stopTask: (_taskId, request) =>
      Effect.sync(() => {
        calls.stop.push({ ...request });
        for (const message of open.values()) fake.close(String(message.id));
        turns.splice(0, turns.length, ...turns.filter((turn) => turn.state !== "queued"));
        for (const turn of turns) {
          if (turn.state === "running" || turn.state === "awaiting_input") {
            turn.state = "settled";
            bump(turn);
          }
        }
        idle();
      }),
    removeFromQueue: () => Effect.die("unused"),
    getConversationTurns: (_taskId, page) =>
      Effect.sync(() =>
        decode(AetherConversationTurnsPage, {
          task: taskBody(),
          head_revision: head,
          turns: turns.slice(-(page?.limit ?? turns.length)).map((turn) => ({
            id: turn.id,
            root_message: turn.root,
            members: [],
            state: turn.state,
            revision_sequence: turn.revision,
          })),
          has_more_older: false,
          oldest_sequence_loaded: null,
        }),
      ),
    getTurnActivity: (_taskId, turnId) =>
      Effect.sync(() => {
        const turn = turns.find((candidate) => candidate.id === turnId)!;
        return decode(AetherTurnActivityPage, {
          items: [turn.root, ...turn.items],
          turn_revision_sequence: turn.revision,
          has_more_older: false,
          oldest_sequence_loaded: null,
        });
      }),
    getLiveConversation: (_taskId, messageIds) =>
      Effect.sync(() => {
        const current = turns.find((turn) => turn.state === "running");
        return decode(AetherLiveConversationSnapshot, {
          ...(current === undefined
            ? {}
            : {
                activeExecution: {
                  conversationTurnId: current.id,
                  processingMessageId: current.root.id,
                },
              }),
          headRevision: head,
          resetSequence: 0,
          open: [...open.values()],
          replacements: messageIds.flatMap((id) => {
            const message = closed.get(id);
            return message === undefined ? [] : [message];
          }),
        });
      }),
    connectWorkspace: () => Effect.die("the adapter test opens the workspace itself"),
  };

  const fake = {
    client,
    calls,
    faults,
    hooks,
    turns,
    get task() {
      return task;
    },
    /** Streams text into an open message, or completes it as a closed row. */
    say: (
      turn: FakeTurn,
      id: string,
      content: string,
      options: { readonly open?: boolean } = {},
    ) => {
      const message = { id, conversationTurnId: turn.id, variant: "text", content };
      if (options.open === true) {
        open.set(id, message);
        bump();
      } else {
        turn.items.push({ id, role: "assistant", variant: "text", content });
        bump(turn);
      }
    },
    close: (id: string) => {
      const message = open.get(id)!;
      open.delete(id);
      closed.set(id, message);
      const turn = turns.find((candidate) => candidate.id === message.conversationTurnId)!;
      turn.items.push({ id, role: "assistant", variant: "text", content: message.content });
      bump(turn);
    },
    tool: (turn: FakeTurn, id: string, command: string) => {
      turn.items.push({
        id: `row-${id}`,
        role: "assistant",
        variant: "tool",
        tool: {
          id,
          name: "Bash",
          input: { command },
          status: "output-available",
          display: { label: "Run command", primary: command },
          itemType: "command_execution",
          result: "ok",
        },
      });
      bump(turn);
    },
    settle: (turn: FakeTurn) => {
      turn.state = "settled";
      idle();
      bump(turn);
      const queued = turns.find((candidate) => candidate.state === "queued");
      if (queued !== undefined) {
        queued.state = "running";
        task = { status: "processing", awaiting: {} };
        bump(queued);
        hooks.onTurn(queued);
      }
    },
    /** The run fails: the turn errors and the task carries Aether's message. */
    fail: (turn: FakeTurn, error: string) => {
      turn.state = "errored";
      task = { status: "errored", awaiting: {}, error };
      bump(turn);
    },
    ask: (turn: FakeTurn, toolId: string, labels: ReadonlyArray<string>) => {
      const input = {
        questions: [
          {
            header: "Label",
            question: "Which label should the button use?",
            options: labels.map((label) => ({ label, description: `Use "${label}".` })),
          },
        ],
      };
      turn.items.push({
        id: `row-${toolId}`,
        role: "assistant",
        variant: "tool",
        tool: {
          id: toolId,
          name: "ask_user",
          input,
          status: "input-available",
          display: { label: "Ask" },
        },
      });
      turn.state = "awaiting_input";
      task = { status: "awaiting_input", awaiting: { kind: "questions", tool_id: toolId, input } };
      bump(turn);
    },
    propose: (turn: FakeTurn, toolId: string, plan: string) => {
      turn.items.push({
        id: `row-${toolId}`,
        role: "assistant",
        variant: "tool",
        tool: {
          id: toolId,
          name: "propose_plan",
          input: { plan },
          status: "approval-requested",
          display: { label: "Plan" },
        },
      });
      turn.state = "awaiting_input";
      task = {
        status: "awaiting_input",
        awaiting: { kind: "plan", tool_id: toolId, input: { plan } },
      };
      bump(turn);
    },
    /** A message sent from Aether's own UI: a turn Synara never sent. */
    remoteTurn: (content: string, queued: boolean) =>
      startTurn({ id: messageId(), role: "user", content, clientMessageId: randomUUID() }, queued),
  };
  return fake;
}

export type FakeAetherTask = ReturnType<typeof makeFakeAetherTask>;

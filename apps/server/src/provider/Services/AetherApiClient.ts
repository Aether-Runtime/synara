/**
 * AetherApiClient - Typed client for the Aether REST API.
 *
 * The one way to reach Aether's REST API: the health check, and later the Aether adapter, use
 * this service. Wire schemas and errors live in `provider/aether/`.
 *
 * @module AetherApiClient
 */
import type { AetherApiBaseUrl } from "@synara/contracts";
import { type Effect, ServiceMap } from "effect";

import type { AetherApiError, AetherApiNotConfiguredError } from "../aether/aetherApiErrors.ts";
import type {
  AetherConversationTurnsPage,
  AetherCreateTaskRequest,
  AetherCreateTaskResponse,
  AetherLiveConversationSnapshot,
  AetherMessageId,
  AetherProject,
  AetherRespondToTaskRequest,
  AetherRespondToTaskResponse,
  AetherTaskId,
  AetherTurnActivityPage,
  AetherTurnId,
  AetherWorkspaceConnect,
  AetherWorkspaceId,
} from "../aether/aetherApiSchemas.ts";

/** Where to reach Aether and the key to send, read from settings and the secret store. */
export interface AetherApiConnection {
  readonly apiBaseUrl: AetherApiBaseUrl;
  readonly apiKey: string;
}

/** Newest page, or the page older than `before` (a page's `oldest_sequence_loaded`). */
export interface AetherPageQuery {
  readonly before?: number;
  readonly limit?: number;
}

export interface AetherApiClientShape {
  /** The base URL and key, resolved for each call; the workspace socket uses the same. */
  readonly connection: Effect.Effect<AetherApiConnection, AetherApiNotConfiguredError>;
  /** `GET /profile`. `timeoutMs` replaces the default request deadline (the health check's). */
  readonly getProfile: (options?: {
    readonly timeoutMs?: number;
  }) => Effect.Effect<{ readonly email: string }, AetherApiError>;
  /** `GET /projects`. */
  readonly listProjects: () => Effect.Effect<ReadonlyArray<AetherProject>, AetherApiError>;
  /** `POST /tasks` (202). */
  readonly createTask: (
    request: AetherCreateTaskRequest,
  ) => Effect.Effect<AetherCreateTaskResponse, AetherApiError>;
  /** `POST /tasks/{taskID}/respond` (202). */
  readonly respondToTask: (
    taskId: AetherTaskId,
    request: AetherRespondToTaskRequest,
  ) => Effect.Effect<AetherRespondToTaskResponse, AetherApiError>;
  /** `POST /tasks/{taskID}/stop`. */
  readonly stopTask: (
    taskId: AetherTaskId,
    request: { readonly discard_queued_messages: boolean },
  ) => Effect.Effect<void, AetherApiError>;
  /** `POST /tasks/{taskID}/remove-from-queue`. */
  readonly removeFromQueue: (
    taskId: AetherTaskId,
    messageId: AetherMessageId,
  ) => Effect.Effect<void, AetherApiError>;
  /** `GET /tasks/{taskID}/conversation/turns`: also carries the task and its workspace. */
  readonly getConversationTurns: (
    taskId: AetherTaskId,
    page?: AetherPageQuery,
  ) => Effect.Effect<AetherConversationTurnsPage, AetherApiError>;
  /** `GET /tasks/{taskID}/conversation/turns/{turnID}/activity`. */
  readonly getTurnActivity: (
    taskId: AetherTaskId,
    turnId: AetherTurnId,
    page?: AetherPageQuery,
  ) => Effect.Effect<AetherTurnActivityPage, AetherApiError>;
  /** `GET /tasks/{taskID}/conversation/live`, for the previously observed open messages. */
  readonly getLiveConversation: (
    taskId: AetherTaskId,
    messageIds: ReadonlyArray<AetherMessageId>,
  ) => Effect.Effect<AetherLiveConversationSnapshot, AetherApiError>;
  /**
   * `POST /workspaces/{workspaceID}/connect`. Only `start: true` may boot a stopped workspace;
   * `start: false` attaches to a usable one and answers 409 otherwise.
   */
  readonly connectWorkspace: (
    workspaceId: AetherWorkspaceId,
    options: { readonly start: boolean },
  ) => Effect.Effect<AetherWorkspaceConnect, AetherApiError>;
}

export class AetherApiClient extends ServiceMap.Service<AetherApiClient, AetherApiClientShape>()(
  "synara/provider/Services/AetherApiClient",
) {}

import type { DeletedThreadWorktreeRemovalFailure } from "@synara/contracts";
import { Effect, Layer, ServiceMap, Stream, SubscriptionRef } from "effect";

/**
 * The latest worktree the user chose to delete with its thread that the server could not remove.
 * The removal runs after the delete was accepted, possibly while the client is disconnected, so a
 * subscriber receives the latest failure first and then every later one.
 */
export interface DeletedThreadWorktreeRemovalFailuresShape {
  readonly publish: (failure: DeletedThreadWorktreeRemovalFailure) => Effect.Effect<void>;
  readonly stream: Stream.Stream<DeletedThreadWorktreeRemovalFailure>;
}

export class DeletedThreadWorktreeRemovalFailures extends ServiceMap.Service<
  DeletedThreadWorktreeRemovalFailures,
  DeletedThreadWorktreeRemovalFailuresShape
>()("synara/deletedThreadWorktreeRemovalFailures") {}

export const DeletedThreadWorktreeRemovalFailuresLive = Layer.effect(
  DeletedThreadWorktreeRemovalFailures,
  Effect.gen(function* () {
    const latest = yield* SubscriptionRef.make<DeletedThreadWorktreeRemovalFailure | null>(null);
    return {
      publish: (failure) => SubscriptionRef.set(latest, failure),
      get stream() {
        return SubscriptionRef.changes(latest).pipe(
          Stream.filter(
            (failure): failure is DeletedThreadWorktreeRemovalFailure => failure !== null,
          ),
        );
      },
    } satisfies DeletedThreadWorktreeRemovalFailuresShape;
  }),
);

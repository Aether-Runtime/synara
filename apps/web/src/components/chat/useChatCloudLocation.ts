// FILE: useChatCloudLocation.ts
// Purpose: The composer's switch between this machine and a cloud provider: the "Work in"
// menu's cloud rows, and the selection change each direction commits before the env mode.
// Layer: Chat composer state hooks
// Exports: useChatCloudLocation

import type {
  ModelSlug,
  ProviderKind,
  ProviderModelDescriptor,
  ServerProviderStatus,
  ServerSettingsView,
  ThreadId,
} from "@synara/contracts";
import { getDefaultModel } from "@synara/shared/model";
import {
  PROVIDER_DESCRIPTOR_BY_KIND,
  type ProviderGatedFeature,
  cloudHostsFor,
  parsePresentedProvider,
  resolvePresentedProvider,
} from "@synara/shared/providerMetadata";
import { useCallback, useMemo } from "react";

import { type DraftThreadEnvMode, useComposerDraftStore } from "../../composerDraftStore";
import { mapModelAcrossLocation } from "../../lib/cloudLocation";
import type { ProviderModelOption, ProviderOptions } from "../../providerModelOptions";
import { resolveCloudLocationRows } from "../BranchToolbar.logic";
import type { ComposerModelSelectionOptions } from "./ComposerModelPicker";

interface ChatCloudLocationInput {
  threadId: ThreadId;
  selectedProvider: ProviderKind;
  selectedModel: string;
  /** The composer's resolved reasoning effort, carried across when the target ladder has it. */
  selectedEffort: string | null;
  modelOptionsByProvider: Record<ProviderKind, ReadonlyArray<ProviderModelOption>>;
  runtimeModelsByProvider: Partial<
    Record<ProviderKind, ReadonlyArray<ProviderModelDescriptor> | null | undefined>
  >;
  providerStatuses: ReadonlyArray<ServerProviderStatus>;
  providerSettings: ServerSettingsView["providers"] | undefined;
  /** What this workspace demands of a provider (no git, or a local folder); null in a git repo. */
  workspaceFeature: ProviderGatedFeature | null;
  onProviderModelSelect: (
    provider: ProviderKind,
    model: ModelSlug,
    options?: ComposerModelSelectionOptions,
  ) => Promise<void> | void;
  onEnvModeChange: (mode: DraftThreadEnvMode) => void;
}

export function useChatCloudLocation({
  threadId,
  selectedProvider,
  selectedModel,
  selectedEffort,
  modelOptionsByProvider,
  runtimeModelsByProvider,
  providerStatuses,
  providerSettings,
  workspaceFeature,
  onProviderModelSelect,
  onEnvModeChange,
}: ChatCloudLocationInput) {
  const cloudRows = useMemo(
    () =>
      resolveCloudLocationRows({
        harness: selectedProvider,
        providerStatuses,
        providerSettings,
        workspaceFeature,
      }),
    [providerSettings, providerStatuses, selectedProvider, workspaceFeature],
  );

  // The options the target provider's draft already holds, so a carried effort lands on them.
  const providerOptionsFor = useCallback(
    (provider: ProviderKind): ProviderOptions | undefined => {
      const store = useComposerDraftStore.getState();
      return (
        store.draftsByThreadId[threadId]?.modelSelectionByProvider[provider]?.options ??
        store.stickyModelSelectionByProvider[provider]?.options
      );
    },
    [threadId],
  );

  const onCloudSelect = useCallback(
    (cloud: ProviderKind) => {
      const host = cloudHostsFor(selectedProvider).find((entry) => entry.cloud === cloud)?.host;
      if (!host) {
        throw new Error(
          `${PROVIDER_DESCRIPTOR_BY_KIND[cloud].displayName} does not run ${PROVIDER_DESCRIPTOR_BY_KIND[selectedProvider].displayName}.`,
        );
      }
      const mapped = mapModelAcrossLocation({
        from: { model: selectedModel, effort: selectedEffort },
        to: {
          provider: cloud,
          models: host.models,
          defaultModel: host.defaultModel,
          options: providerOptionsFor(cloud),
        },
      });
      void onProviderModelSelect(
        cloud,
        mapped.model,
        mapped.modelOptions ? { modelOptions: mapped.modelOptions } : undefined,
      );
    },
    [onProviderModelSelect, providerOptionsFor, selectedEffort, selectedModel, selectedProvider],
  );

  // Leaving the cloud for Local or a new worktree first returns the draft to the harness the
  // cloud was running, then applies the mode as it would for any local draft.
  const onLocationModeChange = useCallback(
    (mode: DraftThreadEnvMode) => {
      const presented = resolvePresentedProvider({
        provider: selectedProvider,
        model: selectedModel,
      });
      if (presented.cloud !== null) {
        const { harness } = presented;
        const mapped = mapModelAcrossLocation({
          from: { model: selectedModel, effort: selectedEffort },
          to: {
            provider: harness,
            models: modelOptionsByProvider[harness],
            defaultModel: getDefaultModel(harness),
            options: providerOptionsFor(harness),
            runtimeModels: runtimeModelsByProvider[harness],
          },
        });
        void onProviderModelSelect(
          harness,
          mapped.model,
          mapped.modelOptions ? { modelOptions: mapped.modelOptions } : undefined,
        );
      }
      onEnvModeChange(mode);
    },
    [
      modelOptionsByProvider,
      onEnvModeChange,
      onProviderModelSelect,
      providerOptionsFor,
      runtimeModelsByProvider,
      selectedEffort,
      selectedModel,
      selectedProvider,
    ],
  );

  // A cloud model no host lists has no harness to return to: the way back to this machine
  // waits for a hosted pick in the model picker.
  const cloudHarnessKnown =
    parsePresentedProvider({ provider: selectedProvider, model: selectedModel }).harness !== null;

  return { cloudRows, onCloudSelect, onEnvModeChange: onLocationModeChange, cloudHarnessKnown };
}

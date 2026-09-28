// FILE: cloudLocation.ts
// Purpose: Carries a draft's model and reasoning effort across a change of location, between
// a local harness and the cloud host that runs the same harness.
// Layer: Web domain helpers
// Exports: mapModelAcrossLocation

import type { ModelSlug, ProviderKind, ProviderModelDescriptor } from "@synara/contracts";
import { resolveSelectableModel, type SelectableModelOption } from "@synara/shared/model";

import {
  getComposerTraitSelection,
  planComposerEffortChange,
} from "../components/chat/composerTraits";
import { resolveRuntimeModelDescriptor } from "../components/chat/runtimeModelCapabilities";
import { buildNextProviderOptions, type ProviderOptions } from "../providerModelOptions";

/** The side a draft moves to: a cloud host's catalog, or a harness's own option list. */
export interface LocationTarget {
  readonly provider: ProviderKind;
  readonly models: ReadonlyArray<SelectableModelOption>;
  readonly defaultModel: ModelSlug;
  /** The target provider's current draft options; a carried effort is applied on top of them. */
  readonly options: ProviderOptions | undefined;
  readonly runtimeModels?: ReadonlyArray<ProviderModelDescriptor> | null | undefined;
}

/**
 * What a selection becomes on the other side of a location change: the same slug when the
 * target offers it (aliases included), else the target's default model; the same reasoning
 * effort when the target model's ladder has it, else no options, so the target's default
 * effort applies. The trigger shows the result; nothing else announces it.
 */
export function mapModelAcrossLocation(input: {
  readonly from: { readonly model: string; readonly effort: string | null };
  readonly to: LocationTarget;
}): { readonly model: ModelSlug; readonly modelOptions: ProviderOptions | undefined } {
  const { from, to } = input;
  const model = resolveSelectableModel(to.provider, from.model, to.models) ?? to.defaultModel;
  if (from.effort === null) {
    return { model, modelOptions: undefined };
  }
  const selection = getComposerTraitSelection(
    to.provider,
    model,
    "",
    to.options,
    resolveRuntimeModelDescriptor({
      provider: to.provider,
      model,
      runtimeModels: to.runtimeModels,
    }),
  );
  const plan = planComposerEffortChange({
    provider: to.provider,
    // The prompt is not carried, so no Ultrathink lock applies on the target.
    selection: { ...selection, ultrathinkPromptControlled: false },
    prompt: "",
    value: from.effort,
  });
  return {
    model,
    modelOptions:
      plan?.kind === "options"
        ? buildNextProviderOptions(to.provider, to.options, plan.patch)
        : undefined,
  };
}

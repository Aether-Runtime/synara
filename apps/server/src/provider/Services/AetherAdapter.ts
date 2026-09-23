/**
 * AetherAdapter - Aether cloud-agent implementation of the generic provider contract.
 *
 * @module AetherAdapter
 */
import { ServiceMap } from "effect";

import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "./ProviderAdapter.ts";

export interface AetherAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {
  readonly provider: "aether";
}

export class AetherAdapter extends ServiceMap.Service<AetherAdapter, AetherAdapterShape>()(
  "synara/provider/Services/AetherAdapter",
) {}

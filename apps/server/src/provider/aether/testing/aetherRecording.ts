// FILE: aetherRecording.ts
// Purpose: Loads a trimmed Aether recording (`recordings/*.json`) and replays it as an HTTP
// transport that serves the recorded responses strictly in recorded order.
// Layer: Test support.

import { readFileSync } from "node:fs";

import type { OutboundHttpRequest, OutboundHttpResponse } from "@synara/shared/outboundHttp";

export type RecordingName = "claude-question-edit-shell";

export interface RecordedExchange {
  /** Why the recorder made the request: `tick`, `head-moved` or `turn-moved`. */
  readonly why: string;
  /** Path and query, as sent. */
  readonly path: string;
  readonly status: number;
  readonly body: unknown;
}

interface RecordingFile {
  readonly log: ReadonlyArray<
    Omit<RecordedExchange, "body"> & {
      readonly body?: unknown;
      readonly sameAsPrevious?: boolean;
    }
  >;
}

/** Reads a recording; a `sameAsPrevious` entry gets the previous body for its path. */
export function loadRecording(name: RecordingName): ReadonlyArray<RecordedExchange> {
  const file = JSON.parse(
    readFileSync(new URL(`./recordings/${name}.json`, import.meta.url), "utf8"),
  ) as RecordingFile;
  const lastBodyByPath = new Map<string, unknown>();
  return file.log.map((entry, index) => {
    if (entry.sameAsPrevious === true && !lastBodyByPath.has(entry.path)) {
      throw new Error(`${name} entry ${index} repeats a body the recording never had`);
    }
    const body = entry.sameAsPrevious === true ? lastBodyByPath.get(entry.path) : entry.body;
    lastBodyByPath.set(entry.path, body);
    return { why: entry.why, path: entry.path, status: entry.status, body };
  });
}

export interface ReplayTransport {
  readonly request: (request: OutboundHttpRequest) => Promise<OutboundHttpResponse>;
  /** Paths requested, in order, including any that did not match the recording. */
  readonly requested: () => ReadonlyArray<string>;
  /** Recorded exchanges not requested yet. */
  readonly remaining: () => ReadonlyArray<RecordedExchange>;
  /** Requests that did not match the next recorded exchange. */
  readonly mismatches: () => ReadonlyArray<string>;
  /** The most requests ever in flight at once. */
  readonly maxInFlight: () => number;
  readonly inFlight: () => number;
}

/**
 * Serves `exchanges` in order. A request must ask for exactly the next recorded path and
 * query; anything else fails the request and is listed in `mismatches`.
 */
export function replayTransport(exchanges: ReadonlyArray<RecordedExchange>): ReplayTransport {
  let cursor = 0;
  let inFlight = 0;
  let maxInFlight = 0;
  const requested: string[] = [];
  const mismatches: string[] = [];
  return {
    request: async (request) => {
      inFlight += 1;
      maxInFlight = Math.max(maxInFlight, inFlight);
      try {
        await Promise.resolve();
        const url = new URL(request.url);
        const path = `${url.pathname}${url.search}`;
        requested.push(path);
        const next = exchanges[cursor];
        if (next === undefined || next.path !== path) {
          const problem = `request ${requested.length}: ${request.method ?? "GET"} ${path}, recorded ${next?.path ?? "nothing"}`;
          mismatches.push(problem);
          throw new Error(problem);
        }
        cursor += 1;
        return {
          status: next.status,
          headers: new Headers({ "content-type": "application/json" }),
          body: new TextEncoder().encode(JSON.stringify(next.body)),
          url: url.toString(),
        };
      } finally {
        inFlight -= 1;
      }
    },
    requested: () => requested,
    remaining: () => exchanges.slice(cursor),
    mismatches: () => mismatches,
    maxInFlight: () => maxInFlight,
    inFlight: () => inFlight,
  };
}

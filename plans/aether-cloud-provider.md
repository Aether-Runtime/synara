# Aether cloud provider for Synara — plan

Status: PLANNED — decisions D1–D5 made 2026-09-23. Planned at Synara `eaa61eded` / Aether `64ad2e26fb`, 2026-09-23.
Repo: `Aether-Runtime/synara` (private standalone copy; `upstream` = `Emanuele-web04/synara`).

## Goal

A Synara user picks **Aether** as the provider for a task. The agent (Claude Code or Codex) runs
in an Aether cloud VM; its transcript, tool activity, questions and plans stream into Synara's
conversation. At the end of every turn the VM's changes are mirrored into the task's local
worktree, so Synara's own diff panel, checkpoints, file viewer and revert work without
modification. The laptop can close mid-task; reopening Synara reattaches to the same cloud task.

This is the design proven in our earlier Aether provider (the "AetherDriver", built in August 2026 for
the app Synara was originally cloned from), ported to Synara's
provider model and rebuilt against Aether's current API.

## Current state

### The AetherDriver is the starting point, but it no longer works against Aether

The driver (~11.6k non-test lines, last changed 2026-08-14) broke against Aether main on four
server changes made after it was written:

| Date  | Aether change                                                                                                              | Driver effect                                      |
| ----- | -------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------- |
| 08-30 | `GET /tasks/{id}/conversation/messages` and `/delta` removed                                                               | transcript reads 404                               |
| 09-07 | workspace git diff files carry `kind` (`text`/`binary`/`too_large`), `isBinary` removed                                    | every diff decodes as malformed; mirror sync fails |
| 09-10 | `latest_sequence` removed from the task read model                                                                         | every `GET`/`PUT /tasks/{id}` decode fails         |
| 09-18 | workspace WS `agent` channel no longer streams the main agent (only subagent text/tool events, truncation, slash commands) | the live turn engine never sees an event           |

The vendored model catalog is also stale (no GPT-6 Sol/Luna/Astra or Opus 5.5; `claude-fable-5`
is now `claude-fable-5-1` and the old slug is rejected).

Consequence: the **event ingestion half of the driver is rebuilt, not ported**. Its replacement
reads the durable conversation API:

- `GET /tasks/{id}/conversation/turns` (paged, `headRevision`, per-turn `revision_sequence`,
  `resetSequence` for truncation, turn `state` = settled / errored / awaiting_input)
- `GET /tasks/{id}/conversation/turns/{turnId}/activity` (tool, text and seam rows of one turn)
- `GET /tasks/{id}/conversation/live?messageIds=[…]` (full accumulated text/thinking of open
  messages — not deltas)

API-key clients have **no push channel** today (Aether's web app gets refresh ticks from Supabase
Realtime; the CLI polls `GET /tasks/{id}`), so v1 polls. See decision D3.

Unchanged and still compatible: API-key auth (REST bearer, WS `?token=`), `POST /tasks`,
`/respond` + `client_message_id` dedupe, `/stop {discard_queued_messages}`, `/remove-from-queue`,
`GET /projects`, `GET /profile`, `POST /workspaces/{id}/connect` (200 path), workspace WS
`files`, `terminal`, `ports`, `activity`, port-preview URLs, canonical item types, tool-display
file-change parsing.

### Synara's provider model

Synara registers providers the pre-driver way: a closed `ProviderKind` literal
(`packages/contracts/src/orchestration.ts:68`), one service tag + layer per adapter, and
`ProviderAdapterRegistry` (`apps/server/src/provider/Layers/ProviderAdapterRegistry.ts`). We follow
that pattern. Adding Devin (`8b428c474`) touched ~64 non-test files (~1.3k lines of wiring).
Synara pins Effect `4.0.0-beta.25`; the AetherDriver was written on `4.0.0-beta.103`.

## Architecture decision: provider, not a "Cloud" environment

Two shapes were assessed:

1. **Aether is a provider** (as in the AetherDriver). The user picks "Aether" and an Aether agent/model
   combination; the task's environment is a worktree the driver owns.
2. **"Cloud" is a third environment** next to Local and Worktree, and the user still picks
   Claude or Codex as the provider.

(2) matches the mental model better, but nothing in Synara models _where_ a provider runs: the
adapter is resolved by provider name alone (registry, session directory binding, runtime event
base, 27 `getByProvider` call sites, capability lookups, the static descriptor table, health and
model listing). Cloud-as-environment means adding a runtime-location key through all of those, or
making the local Claude/Codex adapters speak two protocols. That is a provider-routing redesign of
the maintainer's codebase.

**Recommendation: (1) for v1.** It follows `.docs/architecture.md` rule 2 (provider-native
behavior stays behind the adapter), keeps every "unsupported" answer in one adapter, and "continue
this in the cloud" already falls out of Synara's provider handoff. (2) stays possible later, and
is a conversation to have with the maintainer, not something to build into a fork.

## How it works

### Session start (first turn)

1. Resolve the Aether project: normalize the worktree's `origin` remote (ssh↔https → `owner/repo`)
   and match `GET /projects` `repo_url`. No match, or more than one → fail loudly with an
   actionable message (no auto-creation).
2. Preflight: the thread must run in a **driver-owned worktree** (see "Worktree ownership"),
   on a branch that exists on `origin` at the local HEAD. Uncommitted or unpushed work → refuse
   (the VM clones from `origin` and would never see it).
3. `POST /tasks {project_id, prompt, base_branch, agent_type, model, reasoning_effort,
auto_fix_*: false}` → `{id}`. Persist the resume cursor `{schemaVersion, taskId, …}`.
4. Harvest the turn identity from `conversation/turns` (`turns[0].id`, `root_message.id`).

### Later turns, steering, stop

- Follow-up: `POST /tasks/{id}/respond {message, client_message_id}` (UUID, retried with the same
  id). Aether now steers a running agent natively on respond; `queue_for_later: true` restores
  queued semantics. Map Synara `steerTurn` → native steer and queued follow-ups →
  `queue_for_later` (settle in P4).
- Questions / plans: `awaiting_input {kind, tool_id, input}` from REST → `user-input.requested`;
  answers go back through `/respond` with `expected_tool_id` / `expected_input_digest` so a stale
  answer is refused (409 `stale_pending_input`).
- Interrupt: `POST /tasks/{id}/stop {discard_queued_messages: true}`; discarded text is surfaced
  as a warning.

### Ingestion and settlement

- While a turn is active, one single-flight poll loop per task: read `/conversation/live` for open
  message ids and re-read `/conversation/turns` page 1 when `headRevision` moves; re-read a turn's
  `/activity` when its `revision_sequence` moves. Backoff on error; never overlapping requests.
- A pure mapper turns rows into Synara runtime events (`item.*`, `content.delta` computed from
  accumulated text, `turn.tasks.updated`, `user-input.*`, `runtime.warning/error`). Event ids are
  deterministic from `(taskId, turnId, row id, status)` so replay never duplicates rows.
- **Settlement is durable-authoritative:** a turn ends when the REST turn state says so, exactly
  once. Before emitting `turn.completed`, the mirror sync runs.
- Turns that did not originate in Synara (someone answered from Aether's web app) are rendered as
  a warning card carrying the injected text.

### Mirror sync (the key piece)

At turn end: connect the workspace WS, request the cumulative git diff (merge-base → VM working
tree), reset the local worktree to the task base, apply the full cumulative diff, then emit
`turn.completed`. Synara's `CheckpointReactor` captures the checkpoint from the real local tree, so
diffs, checkpoints and revert work unchanged (`refs/synara/checkpoints`, same mechanism the AetherDriver relied on).

- `text` files: apply hunks. `binary`: read bytes over the WS `files` channel. `too_large`: fail
  the sync loudly with the file path (no silent skip).
- `git apply` needs stdin; Synara's `GitCore.ExecuteGitInput` has none → add it.
- If the VM is suspended at turn end, settle now and sync lazily at the next turn start (viewing
  never boots a VM).

### Worktree ownership and the mirror guard

- Synara creates worktrees from the **web client** on first send and has no server-side "managed"
  marker. Add a server-authoritative ownership marker for Aether threads, and forbid
  `copyChangesFrom` for them (the mirror reset would erase the copied changes).
- Synara already serializes every git mutation per repository through `GitCore.withMutation`
  (commit/push, stage, stash, handoff, PR checkout, worktree prune, gateway). The ownership check
  goes **inside `withMutation`**, and the mirror sync runs inside it. This replaces the AetherDriver's
  per-call-site guards (the part that took five review rounds there).
- Not covered by `withMutation`: file writes from the editor (400 ms autosave), checkpoint-revert
  restore, and the first-turn branch rename, which also pushes. Guard the first two; skip the
  rename+push for Aether threads.
- Synara's idle reaper stops sessions after 30 minutes. For Aether, `stopSession` detaches (the
  cloud task keeps running) and must be idempotent; the ownership marker must outlive the session.
- `startupTurnReconciliation` marks every running turn interrupted at boot. Aether turns must be
  exempt and reconciled from REST instead.

## Synara integration points

Compile-time exhaustive (the compiler lists them once `"aether"` is added): contracts
(`ProviderKind`, `ModelSelection`, `ProviderStartOptions`, `model.ts` tables, settings), shared
`PROVIDER_DESCRIPTORS`, server registry + `runtimeLayer.ts` + `ProviderHealth` child kind +
agent-gateway target resolver, ~27 web `Record<ProviderKind, …>` sites and typed switches.

**Silent-misroute traps (not type-checked — grep, don't trust the compiler):**

- `persistence/modelSelectionCompatibility.ts` re-infers unknown providers from the model slug, so
  a stored `{provider: "aether", model: "claude-…"}` is rewritten to `claudeAgent` on read.
- web `storeNormalization.ts` defaults unknown providers to `codex`; `composerProviderRegistry.tsx`
  silently drops options for a missing case.
- `ProviderDiscoveryKind` is a second literal list; `providerStartOptionsFromServerSettings` is not
  exhaustive; `resolveModelSlug` coerces non-built-in slugs to the default.
- A provider with no health status is marked unusable by the web.

Credentials: extend `ProviderCredentials` (secret store; today OpenCode-only) for the API key,
patch-only in settings with a `…Configured` flag in the view. Health: `GET /profile` through the
existing `checkProviderWhenEnabled` hook (the first provider with no local binary).

Text generation (commit messages, PR titles, branch names) is central in Synara and routed to
git-writing providers only; Aether supplies nothing. A user with only Aether enabled gets errors
there — acceptable, documented.

## Feature matrix

| Synara feature                                   | v1 behavior                                                                                                                                        |
| ------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| Conversation, tools, reasoning, questions, plans | Works                                                                                                                                              |
| Diffs, checkpoints, file viewer                  | Works via mirror sync                                                                                                                              |
| Revert / rollback                                | Refused before touching files — add `"unsupported"` to the rollback capability                                                                     |
| Model switch                                     | `restart-session` or `unsupported` (decide in P4)                                                                                                  |
| Steering                                         | Native steer via `/respond`                                                                                                                        |
| Thread fork                                      | Transcript replay (no native `forkThread`); fork starts from the branch, not the mirrored tree                                                     |
| Provider handoff to/from Aether                  | Needs design: to Aether requires a clean pushed tree; from Aether must release ownership first                                                     |
| Local ↔ worktree git handoff                     | Refused while the mirror owns the checkout (automatic via `withMutation`)                                                                          |
| Sidechats                                        | Disabled for Aether threads (a cloud sidechat's mirror would reset the parent's checkout)                                                          |
| Goals                                            | Disabled: a goal ends only when the agent calls `synara_set_thread_goal` on the local gateway, which the VM cannot reach, so it would loop forever |
| Computer use, browser automation, device tools   | Disabled (local gateway only); add a capability flag                                                                                               |
| Automations                                      | Disabled for Aether in v1 (they branch from local HEAD)                                                                                            |
| Voice, remote access, external MCP               | Work as-is                                                                                                                                         |
| Subagents                                        | Rendered from turn activity; live subagent transcripts are v2                                                                                      |
| Cloud terminal                                   | v2 (rewrite against Synara's ack/backpressure terminal manager; two terminal scopes per thread)                                                    |
| Port previews                                    | v2 (Synara has no port runtime event; add event + ingestion + row)                                                                                 |
| Usage / spend                                    | v2 (`/tasks/{id}/spend`, token usage into profile stats)                                                                                           |

## Aether-side work

- **AS1 — idempotency key on `POST /tasks`.** A lost 202 still creates a duplicate task and a
  second paid VM. Small, and the CLI benefits too.
- **AS2 — public model catalog endpoint.** The vendored catalog went stale within six weeks and a
  rename now rejects saved selections. Two clients (the AetherDriver, Synara) vendoring it guarantees drift.
- **AS3 — push channel for API-key clients** (decision D3). E.g. an SSE stream of task revision
  ticks, so clients fetch on change instead of polling.
- **AS4 — public failure kind for "out of inference balance"** (today only the message text
  reaches clients).
- **AS5 — keep third-party clients from breaking silently.** The AetherDriver was broken for 3+
  weeks unnoticed. Options: generate the Synara REST client from
  `apps/api/openapi/aether-api.openapi.json` (Synara already has `@effect/openapi-generator` in its
  catalog, unused), and/or a CI job that decodes recorded API responses with the client schemas.

## Port inventory

| Bucket                  | Files                                                                                                                            | Notes                                                                                                                                                                |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Carry over              | `vendored/toolDisplay`, `vendored/canonicalItemType`, `portPreview`, `terminalConnection`, `workspaceSocket` (WS transport only) | Mechanical Effect retargeting (`Schema.Defect()` → value, `decodeUnknownResult` → `decodeUnknownExit`, `Context.Service` → `ServiceMap.Service`, no `effect/Crypto`) |
| Carry over with changes | `restClient`, `restSchemas`, `mirrorSync`, `AetherMirrorRegistry`/`Guards` (→ `withMutation`)                                    | Current API shapes; `kind`-based diffs; `GitCore` instead of `GitVcsDriver`                                                                                          |
| Rebuild                 | `eventMapper` input side, `wireEvents`, turn engine in `AetherAdapter`, `vendored/catalog`                                       | Durable-conversation polling; catalog from AS2 or resynced                                                                                                           |
| Rewrite (v2)            | `AetherTerminalManager`, `AetherProvider` (→ `ProviderHealth` check)                                                             | Synara terminal/health models differ                                                                                                                                 |
| Drop                    | `Drivers/AetherDriver`, `AetherTextGeneration`, mobile                                                                           | Driver-SPI glue; Synara has no mobile app                                                                                                                            |

Rough size: the straight port was estimated at 2–3 weeks assuming the old event stream still
worked; the ingestion rebuild and Synara-specific gating add to that. Treat 3–4 weeks as the v1
order of magnitude, to be re-estimated after P3.

## Sequencing (PRs to `Aether-Runtime/synara`)

Sequential merges, each green on Synara's own bar (`bun run fmt:check`, `lint`, `typecheck`,
affected `bun run test`; `windows-runtime:check` / `migrations:check` where relevant).

| PR  | Scope                                                                                                                                                                                                           | Verify                                                                                        |
| --- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| P0  | Fork CI bootstrap: PR CI runs green on the fork; note jobs that need upstream secrets                                                                                                                           | CI green on a no-op PR                                                                        |
| P1  | Provider skeleton: `"aether"` in every exhaustive site + the silent-misroute list, settings + secret-store API key, `/profile` health check, static catalog (current models), adapter whose methods fail loudly | typecheck; a test per silent-misroute site; settings round-trip                               |
| P2  | REST client + schemas on current API (or generated from OpenAPI — AS5)                                                                                                                                          | decode tests against recorded current responses                                               |
| P3  | Ingestion: poll loop + row→event mapper + deterministic ids + truncation                                                                                                                                        | mapper fixture tests; replay idempotence                                                      |
| P4  | Lifecycle: project resolution, preflight, create/respond/steer/stop, questions/plans, durable settlement, reattach, startup-reconciliation exemption, idempotent detach                                         | adapter tests with a fake API; restart-mid-turn test                                          |
| P5  | Mirror sync + ownership marker + `withMutation` guard + file-write/revert guards + `GitCore` stdin                                                                                                              | real-git mirror tests; concurrent-mutation refusal tests                                      |
| P6  | Capability gating (goals, sidechats, computer/browser, automations, rollback) + web UX (icon, settings card, picker, worktree default, hidden handoff items)                                                    | web tests; `uiFontSize` guard                                                                 |
| P7  | Live E2E against Aether prod from an isolated Synara home                                                                                                                                                       | one real task: stream, question, settle, diff renders, revert refused, reattach after restart |

v2: cloud terminal, port previews, live subagent transcripts, usage/spend, VM size picker.

## Decisions (owner, 2026-09-23)

- **D1 — Where this ends up:** built and tested in our fork only; nothing goes to the maintainer
  while this is an experiment. Upstreaming into `Emanuele-web04/synara` is the eventual goal, so
  keep Synara's conventions and a small core footprint.
- **D2 — Provider vs Cloud environment:** Aether is a provider.
- **D3 — Live updates:** poll (the simplest option). Revisit after live testing.
- **D4 — v1 scope:** the feature matrix above; terminal, previews, subagent transcripts and usage
  are v2.
- **D5 — Aether-side asks:** AS1 (idempotency key on `POST /tasks`) yes, needed before P4. AS2–AS5
  not now: the vendored catalog is resynced by hand in P1.

Still open:

- **D6 — Billing / sign-in.** Assumed: Synara users bring an Aether account + API key (as
  the AetherDriver).
- **D7 — The broken AetherDriver** (see below): fix, freeze, or retire.

## Outside this project

- The AetherDriver (its private Aether-Runtime repo and upstream PR #5995) is broken
  against current Aether for the same four reasons. Fix, freeze, or retire — separate decision.

import { Exit } from "effect";
import { describe, expect, it } from "vitest";

import { PROJECT_ID, TASK_ID, TURN_ID } from "./testing/aetherApiFixtures.ts";
import { parseAetherResumeCursor } from "./aetherResumeCursor.ts";

const TASK_TURN = "8a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d";
const SENT_TURN = "1f2e3d4c-5b6a-4978-8a9b-0c1d2e3f4a5b";

const created = {
  schemaVersion: 1,
  status: "created",
  worktreePath: "/work/tree",
  taskId: TASK_ID,
  projectId: PROJECT_ID,
  baseBranch: "main",
  baseCommit: "0123456789abcdef0123456789abcdef01234567",
  model: "claude-opus-5",
  turns: [
    { turnId: TASK_TURN, aetherTurnId: TURN_ID },
    { turnId: SENT_TURN, clientMessageId: SENT_TURN, aetherTurnId: null },
  ],
  sends: 1,
  after: null,
  active: true,
  mirrorPending: false,
};

const parses = (raw: unknown) => Exit.isSuccess(parseAetherResumeCursor(raw));

describe("parseAetherResumeCursor", () => {
  it("reads a created cursor: the task's own turn first, then the turns Synara sent", () => {
    expect(parses(created)).toBe(true);
    expect(parses({ ...created, turns: created.turns.slice(0, 1), active: false })).toBe(true);
  });

  it("refuses a created cursor with no turns", () => {
    expect(parses({ ...created, turns: [] })).toBe(false);
  });

  it("refuses a first turn with a client message id, or a later turn without one", () => {
    const [task, sent] = created.turns;
    expect(parses({ ...created, turns: [{ ...task, clientMessageId: SENT_TURN }, sent] })).toBe(
      false,
    );
    expect(parses({ ...created, turns: [task, { turnId: SENT_TURN, aetherTurnId: null }] })).toBe(
      false,
    );
  });

  it("refuses keys it does not know, at any depth", () => {
    expect(parses({ ...created, extra: true })).toBe(false);
    const [task, sent] = created.turns;
    expect(parses({ ...created, turns: [task, { ...sent, extra: true }] })).toBe(false);
    expect(
      parses({
        schemaVersion: 1,
        status: "creating",
        worktreePath: "/work/tree",
        idempotencyKey: SENT_TURN,
        extra: true,
      }),
    ).toBe(false);
  });

  it("cannot name an active turn outside its turns", () => {
    // The turn in flight is the last one in `turns` while `active`: no id can dangle.
    const { active: _active, ...rest } = created;
    expect(parses({ ...rest, activeTurnId: "not-a-turn" })).toBe(false);
  });
});

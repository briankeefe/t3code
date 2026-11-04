import { describe, expect, it } from "vite-plus/test";

import { buildThreadActionMenuItems, type ThreadActionMenuState } from "./threadActionMenu.logic";

const baseState: ThreadActionMenuState = {
  canOperate: true,
  branch: null,
  projectFilter: null,
  isPinned: false,
  isSettled: false,
  autoSettleEnabled: true,
  isSnoozed: false,
  canSnoozeNow: true,
  isRegeneratingTitle: false,
  isRunning: false,
  supports: {
    settlement: true,
    autoSettleOptOut: true,
    snooze: true,
    pinning: true,
    titleRegeneration: true,
  },
  snoozePresets: [
    { id: "hour", label: "In 1 hour", whenLabel: "3:00 PM", snoozedUntil: "2026-08-07T15:00:00Z" },
  ],
};

describe("buildThreadActionMenuItems", () => {
  it.each([false, true])(
    "disables both lifecycle directions without permission (reversed: %s)",
    (reversed) => {
      const items = buildThreadActionMenuItems({
        ...baseState,
        canOperate: false,
        isPinned: reversed,
        isSettled: reversed,
        isSnoozed: reversed,
      });
      const expected = reversed
        ? [
            "unpin",
            "unsettle",
            "unsnooze",
            "toggle-waiting-on-others",
            "toggle-circle-back",
            "rename",
            "regenerate-title",
            "auto-settle",
            "archive",
            "delete",
          ]
        : [
            "pin",
            "settle",
            "snooze",
            "toggle-waiting-on-others",
            "toggle-circle-back",
            "rename",
            "regenerate-title",
            "auto-settle",
            "archive",
            "delete",
          ];
      expect(items.filter((item) => item.disabled).map((item) => item.id)).toEqual(expected);
      expect(
        items.find((item) => item.id === "snooze")?.children?.every((child) => child.disabled) ??
          true,
      ).toBe(true);
    },
  );

  it.each([null, "waiting-on-others", "circle-back"] as const)(
    "shows mutually exclusive follow-up checks for %s",
    (followUp) => {
      const items = buildThreadActionMenuItems({ ...baseState, followUp });
      expect(items.find((item) => item.id === "toggle-waiting-on-others")).toMatchObject({
        label:
          followUp === "waiting-on-others"
            ? "Clear waiting on someone else"
            : "Waiting on someone else",
        checked: followUp === "waiting-on-others",
      });
      expect(items.find((item) => item.id === "toggle-circle-back")).toMatchObject({
        label: followUp === "circle-back" ? "Dismiss Will circle back" : "Will circle back",
        checked: followUp === "circle-back",
      });
      expect(
        buildThreadActionMenuItems({ ...baseState, followUp, canOperate: false })
          .filter(
            (item) => item.id === "toggle-waiting-on-others" || item.id === "toggle-circle-back",
          )
          .every((item) => item.disabled),
      ).toBe(true);
    },
  );

  it("disables archive and turning into a project while the thread is running", () => {
    const items = buildThreadActionMenuItems({
      ...baseState,
      isRunning: true,
      canTurnIntoProject: true,
    });
    expect(items.find((item) => item.id === "archive")?.disabled).toBe(true);
    expect(items.find((item) => item.id === "turn-into-project")?.disabled).toBe(true);
  });
});

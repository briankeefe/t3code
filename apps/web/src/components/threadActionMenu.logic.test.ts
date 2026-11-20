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
  isRegeneratingTitle: false,
  isRunning: false,
  supports: {
    settlement: true,
    autoSettleOptOut: true,
    snooze: true,
    pinning: true,
    titleRegeneration: true,
  },
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
            "toggle-waiting-on-others",
            "toggle-circle-back",
            "rename",
            "regenerate-title",
            "auto-settle",
            "archive",
            "delete",
          ];
      expect(items.filter((item) => item.disabled).map((item) => item.id)).toEqual(expected);
      expect(items.some((item) => item.label === "Snooze")).toBe(false);
      expect(items.find((item) => item.id === "unsnooze")?.label).toBe(
        reversed ? "Wake thread" : undefined,
      );
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
        icon: "hourglass",
      });
      expect(items.find((item) => item.id === "toggle-circle-back")).toMatchObject({
        label: followUp === "circle-back" ? "Dismiss Will circle back" : "Will circle back",
        checked: followUp === "circle-back",
        icon: "rotate-ccw",
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

  it("disables only rename where no inline title editor exists", () => {
    const items = buildThreadActionMenuItems({ ...baseState, canRename: false });
    expect(items.filter((item) => item.disabled).map((item) => item.id)).toEqual(["rename"]);
  });
});

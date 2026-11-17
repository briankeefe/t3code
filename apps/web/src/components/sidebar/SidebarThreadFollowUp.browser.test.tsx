import { expect, it, vi } from "vite-plus/test";
import type { ComponentProps } from "react";
import type * as Session from "../../state/session";
import "~/index.css";

import { render } from "../../../test/render";
import { makeThreadFixture } from "../../test-fixtures";
import { SortableThreadRow } from "./SidebarThreadRow";

vi.mock("../../state/session", async (original) => ({
  ...(await original<typeof Session>()),
  useEnvironmentScope: () => true,
}));

it.each(["card", "slim", "folder", "folder-multiline"] as const)(
  "replaces snooze with exclusive manual follow-up toggles in %s rows",
  async (variant) => {
    const thread = makeThreadFixture({ title: "Contract review: follow up with the team" });
    const onToggleFollowUp = vi.fn();
    const onThreadClick = vi.fn();
    const noop = () => {};
    const props: ComponentProps<typeof SortableThreadRow> = {
      thread,
      variant,
      variantAction: "settle",
      settlementSupported: true,
      snoozeSupported: true,
      pinningSupported: false,
      isPinned: false,
      dropVerb: null,
      dragOverPinned: false,
      sweepAction: null,
      snoozeWakeLabelText: null,
      wokeAt: null,
      isActive: false,
      isRouteThread: false,
      readOpensPullRequestsInRightPanel: () => false,
      jumpLabel: null,
      currentEnvironmentId: thread.environmentId,
      environmentLabel: null,
      environmentMachine: "laptop",
      scratchMachineLabel: null,
      project: null,
      projectDisplayName: null,
      providerEntryByInstanceId: new Map(),
      timestampFormat: "locale",
      onThreadClick,
      onThreadActivate: noop,
      onStartRename: noop,
      onRenameTitleChange: noop,
      onCommitRename: noop,
      onCancelRename: noop,
      isRenaming: false,
      renamingTitle: "",
      onContextMenu: noop,
      onSettle: noop,
      onActionSweepStart: noop,
      onUnsettle: noop,
      onToggleFollowUp,
      onUnsnooze: noop,
      onUnpin: noop,
      onAcknowledgeWoke: noop,
      sortableId: "follow-up-row",
      sortableDisabled: true,
      contextDrag: false,
    };
    await using screen = await render(<SortableThreadRow {...props} />);
    screen.container.style.width = "320px";
    const row = screen.getByRole("button", { name: /^Contract review: follow up with the team,/ });
    await row.hover();
    await expect
      .element(screen.getByRole("button", { name: "Snooze thread" }))
      .not.toBeInTheDocument();
    const circleBack = screen.getByRole("button", { name: "Circle back", exact: true });
    const waiting = screen.getByRole("button", { name: "Waiting for someone else", exact: true });
    await expect.element(circleBack).toHaveAttribute("aria-pressed", "false");
    await expect.element(waiting).toHaveAttribute("aria-pressed", "false");
    await circleBack.click();
    expect(onToggleFollowUp).toHaveBeenLastCalledWith(
      { environmentId: thread.environmentId, threadId: thread.id },
      "circle-back",
    );
    expect(onThreadClick).not.toHaveBeenCalled();
    await screen.rerender(
      <SortableThreadRow {...props} thread={{ ...thread, followUp: "circle-back" }} />,
    );
    await expect.element(circleBack).toHaveAttribute("aria-pressed", "true");
    await waiting.click();
    expect(onToggleFollowUp).toHaveBeenLastCalledWith(
      { environmentId: thread.environmentId, threadId: thread.id },
      "waiting-on-others",
    );
    await screen.rerender(
      <SortableThreadRow {...props} thread={{ ...thread, followUp: "waiting-on-others" }} />,
    );
    await expect.element(circleBack).toHaveAttribute("aria-pressed", "false");
    await expect.element(waiting).toHaveAttribute("aria-pressed", "true");
    await waiting.click();
    expect(onToggleFollowUp).toHaveBeenCalledTimes(3);
    await screen.rerender(<SortableThreadRow {...props} thread={{ ...thread, followUp: null }} />);
    await expect.element(waiting).toHaveAttribute("aria-pressed", "false");
  },
);

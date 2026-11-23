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
  "keeps manual follow-up and background-wait status consistent in %s rows",
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
      environmentLabel: null,
      environmentMachine: "laptop",
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

    const runtime = {
      status: "idle" as const,
      providerInstanceId: thread.providerInstanceId,
      providerName: "Claude",
      activeRunId: null,
      activityStartedAt: null,
      lastError: null,
      updatedAt: thread.updatedAt,
    };
    await screen.rerender(
      <SortableThreadRow
        {...props}
        thread={{
          ...thread,
          runtime,
          pendingBackgroundTasks: [
            { taskId: "ci-watch", kind: "monitor", description: "Watching CI", awaited: true },
          ],
        }}
      />,
    );
    const circle = () =>
      getComputedStyle(screen.container.querySelector('svg[aria-label^="Agent:"] circle')!);
    expect(circle().fill).toBe("rgb(255, 255, 255)");
    await expect.element(screen.getByRole("img", { name: "Agent: Monitoring" })).toBeVisible();
    await screen.rerender(
      <SortableThreadRow
        {...props}
        thread={{
          ...thread,
          runtime: { ...runtime, status: "running" },
          blockedOnThreadWait: true,
        }}
      />,
    );
    expect(circle().fill).toBe("rgb(255, 255, 255)");
    await screen.rerender(
      <SortableThreadRow
        {...props}
        thread={{ ...thread, runtime: { ...runtime, status: "running" } }}
      />,
    );
    expect(circle().fill).toBe("rgb(224, 175, 104)");
    await screen.rerender(<SortableThreadRow {...props} thread={thread} />);
    expect(circle().fill).toBe("none");
    expect(circle().stroke).toBe("rgb(158, 206, 106)");
  },
);

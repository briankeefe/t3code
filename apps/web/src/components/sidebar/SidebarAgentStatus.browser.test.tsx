import { expect, it } from "vite-plus/test";
import "~/index.css";

import { render } from "../../../test/render";
import { SidebarAgentStatus } from "./SidebarAgentStatus";

it("keeps a previous unread completion from masking a new request, and clears the fill when read", async () => {
  const container = document.createElement("div");
  container.className = "[&:not(:hover):not(:focus-within)_*]:text-secondary-label";
  await using screen = await render(<SidebarAgentStatus status="ready" unread />, {
    router: false,
    container,
  });
  const circle = () => getComputedStyle(screen.container.querySelector("circle")!);
  expect(circle().fill).toBe("rgb(158, 206, 106)");

  await screen.rerender(<SidebarAgentStatus status="input" unread />);
  await expect.element(screen.getByRole("img", { name: "Agent: Needs input" })).toBeVisible();
  expect(circle().fill).toBe("rgb(247, 118, 142)");

  await screen.rerender(<SidebarAgentStatus status="working" unread />);
  const workingColor = circle().fill;
  expect(workingColor).toBe("rgb(224, 175, 104)");

  await screen.rerender(<SidebarAgentStatus status="waiting" unread />);
  await expect.element(screen.getByRole("img", { name: "Agent: Monitoring" })).toBeVisible();
  expect(circle().stroke).toBe("rgb(255, 255, 255)");
  expect(circle().fill).toBe("rgb(255, 255, 255)");

  await screen.rerender(<SidebarAgentStatus status="working" unread />);
  expect(circle().fill).toBe(workingColor);

  await screen.rerender(<SidebarAgentStatus status="ready" unread={false} />);
  await expect.element(screen.getByRole("img", { name: "Agent: Idle" })).toBeVisible();
  expect(circle().fill).toBe("none");
  expect(circle().stroke).toBe("rgb(158, 206, 106)");
});

it.each([
  ["waiting-on-others", "Waiting on someone else", "rgb(187, 154, 247)"],
  ["circle-back", "Will circle back", "rgb(34, 211, 238)"],
] as const)(
  "uses one circle for %s, with attention overriding it until resolved",
  async (followUp, label, color) => {
    await using screen = await render(<SidebarAgentStatus status="ready" unread={false} />, {
      router: false,
    });
    const circle = () => getComputedStyle(screen.container.querySelector("circle")!);
    for (const status of ["ready", "working", "waiting"] as const) {
      await screen.rerender(<SidebarAgentStatus status={status} unread={false} />);
      const originalFill = circle().fill;
      const originalStroke = circle().stroke;
      await screen.rerender(
        <SidebarAgentStatus status={status} unread={false} followUp={followUp} />,
      );
      expect(screen.container.querySelectorAll("circle").length).toBe(1);
      await expect.element(screen.getByRole("img", { name: label })).toBeVisible();
      expect(circle().fill).toBe(color);
      expect(circle().stroke).toBe(color);
      await screen.rerender(<SidebarAgentStatus status={status} unread={false} followUp={null} />);
      expect(screen.container.querySelectorAll("circle").length).toBe(1);
      expect(circle().fill).toBe(originalFill);
      expect(circle().stroke).toBe(originalStroke);
    }
    for (const [status, attentionLabel] of [
      ["approval", "Needs approval"],
      ["input", "Needs input"],
      ["request", "Guest request"],
      ["limited", "Usage limited"],
      ["failed", "Failed"],
    ] as const) {
      await screen.rerender(<SidebarAgentStatus status={status} unread followUp={followUp} />);
      expect(screen.container.querySelectorAll("circle").length).toBe(1);
      await expect
        .element(screen.getByRole("img", { name: `Agent: ${attentionLabel}` }))
        .toBeVisible();
      expect(circle().fill).toBe("rgb(247, 118, 142)");
      expect(circle().stroke).toBe("rgb(247, 118, 142)");
      await screen.rerender(<SidebarAgentStatus status="ready" unread followUp={followUp} />);
      await expect.element(screen.getByRole("img", { name: label })).toBeVisible();
      expect(circle().fill).toBe(color);
    }
    await screen.rerender(<SidebarAgentStatus status="ready" unread={false} followUp={followUp} />);
    expect(circle().fill).toBe(color);
    await screen.rerender(<SidebarAgentStatus status="ready" unread followUp={null} />);
    expect(circle().fill).toBe("rgb(158, 206, 106)");
    await screen.rerender(<SidebarAgentStatus status="ready" unread={false} followUp={null} />);
    expect(circle().fill).toBe("none");
  },
);

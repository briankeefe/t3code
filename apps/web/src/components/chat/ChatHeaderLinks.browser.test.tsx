import { EnvironmentId, ProjectId, ThreadId } from "@t3tools/contracts";
import { expect, it, vi } from "vite-plus/test";

const state = vi.hoisted(() => ({
  rows: [] as Array<{ item: { type: string; text: string } }>,
}));
vi.mock(import("~/state/entities"), () => ({
  useThreadVisibleTurnItems: (() => state.rows) as never,
}));
vi.mock(import("~/lib/openPullRequestLink"), () => ({
  useOpenPrLink: () => () => false,
}));
vi.mock(import("~/browser/useOpenLink"), () => ({
  useOpenLink: () => async () => {},
}));

import { render } from "../../../test/render";
import { ChatHeaderLinks } from "./ChatHeaderLinks";

const threadRef = {
  environmentId: EnvironmentId.make("environment"),
  threadId: ThreadId.make("thread"),
};
const linkedPullRequest = {
  projectId: ProjectId.make("project"),
  repository: "owner/repo",
  number: 123,
  url: "https://github.com/owner/repo/pull/123",
};

it("shows linked PR and newest assistant checklist labels and hrefs, and removes absent links", async () => {
  state.rows = [];
  await using screen = await render(
    <ChatHeaderLinks threadRef={threadRef} linkedPullRequest={null} />,
  );
  await expect.element(screen.getByRole("link")).not.toBeInTheDocument();

  state.rows = [
    { item: { type: "assistant_message", text: "http://localhost:3398/eng-100-test" } },
    {
      item: { type: "assistant_message", text: "[Checklist](http://127.0.0.1:3398/eng-123-test)" },
    },
    { item: { type: "user_message", text: "http://localhost:3398/eng-200-test" } },
  ];
  await screen.rerender(
    <ChatHeaderLinks threadRef={{ ...threadRef }} linkedPullRequest={linkedPullRequest} />,
  );
  const pr = screen.getByRole("link", { name: "Open pull request #123" });
  await expect.element(pr).toHaveTextContent("PR #123");
  await expect.element(pr).toHaveAttribute("href", linkedPullRequest.url);
  const brew = screen.getByRole("link", { name: "Open Brew test checklist" });
  await expect.element(brew).toHaveTextContent("Brew test");
  await expect.element(brew).toHaveAttribute("href", "http://127.0.0.1:3398/eng-123-test");

  state.rows = [{ item: { type: "user_message", text: "http://localhost:3398/eng-200-test" } }];
  await screen.rerender(<ChatHeaderLinks threadRef={{ ...threadRef }} linkedPullRequest={null} />);
  await expect.element(screen.getByRole("link")).not.toBeInTheDocument();
});

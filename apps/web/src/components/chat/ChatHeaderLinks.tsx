import type { ScopedThreadRef, ThreadLinkedPullRequest } from "@t3tools/contracts";
import { NotebookPenIcon } from "lucide-react";
import { useOpenLink } from "~/browser/useOpenLink";
import { useOpenPrLink } from "~/lib/openPullRequestLink";
import { useThreadVisibleTurnItems } from "~/state/entities";
import { PullRequestGlyph } from "../pullRequest/pullRequestIcons";
import { Badge } from "../ui/badge";
import { toastManager } from "../ui/toast";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";
import { newestBrewTestUrl } from "./ChatHeaderLinks.logic";

export function ChatHeaderLinks({
  threadRef,
  linkedPullRequest,
}: {
  threadRef: ScopedThreadRef;
  linkedPullRequest: ThreadLinkedPullRequest | null;
}) {
  const rows = useThreadVisibleTurnItems(threadRef);
  const brewTestUrl = newestBrewTestUrl(rows);
  const openPrLink = useOpenPrLink(threadRef);
  const openLink = useOpenLink(threadRef);
  if (linkedPullRequest === null && brewTestUrl === null) return null;
  return (
    <div
      className="flex shrink-0 items-center gap-1.5 [-webkit-app-region:no-drag]"
      onContextMenu={(event) => event.stopPropagation()}
    >
      {linkedPullRequest ? (
        <Tooltip>
          <TooltipTrigger
            render={
              <Badge
                variant="outline"
                size="sm"
                render={
                  <a
                    href={linkedPullRequest.url}
                    target="_blank"
                    rel="noopener noreferrer"
                    aria-label={`Open pull request #${linkedPullRequest.number}`}
                    onClick={(event) => openPrLink(event, linkedPullRequest.url)}
                  />
                }
              />
            }
          >
            <PullRequestGlyph.pullRequest />
            PR #{linkedPullRequest.number}
          </TooltipTrigger>
          <TooltipPopup>{linkedPullRequest.url}</TooltipPopup>
        </Tooltip>
      ) : null}
      {brewTestUrl ? (
        <Tooltip>
          <TooltipTrigger
            render={
              <Badge
                variant="outline"
                size="sm"
                render={
                  <a
                    href={brewTestUrl}
                    target="_blank"
                    rel="noopener noreferrer"
                    aria-label="Open Brew test checklist"
                    onClick={(event) => {
                      if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
                      event.preventDefault();
                      event.stopPropagation();
                      void openLink(brewTestUrl, { event }).catch((error: unknown) => {
                        toastManager.add({
                          type: "error",
                          title: "Unable to open Brew test checklist",
                          description:
                            error instanceof Error ? error.message : "An error occurred.",
                        });
                      });
                    }}
                  />
                }
              />
            }
          >
            <NotebookPenIcon />
            Brew test
          </TooltipTrigger>
          <TooltipPopup>{brewTestUrl}</TooltipPopup>
        </Tooltip>
      ) : null}
    </div>
  );
}

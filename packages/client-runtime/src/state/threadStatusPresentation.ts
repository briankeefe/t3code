import type { OrchestrationV2FollowUp } from "@t3tools/contracts";

const STATUS = {
  working: { label: "Working", color: "#e0af68", filled: true, manual: false },
  waiting: { label: "Monitoring", color: "#ffffff", filled: true, manual: false },
  approval: { label: "Needs approval", color: "#f7768e", filled: true, manual: false },
  input: { label: "Needs input", color: "#f7768e", filled: true, manual: false },
  request: { label: "Guest request", color: "#f7768e", filled: true, manual: false },
  limited: { label: "Usage limited", color: "#f7768e", filled: true, manual: false },
  failed: { label: "Failed", color: "#f7768e", filled: true, manual: false },
  ready: { label: "Idle", color: "#9ece6a", filled: false, manual: false },
} as const;
const COMPLETED = {
  label: "Completed, unread",
  color: "#9ece6a",
  filled: true,
  manual: false,
} as const;
const FOLLOW_UP = {
  "waiting-on-others": {
    label: "Waiting on someone else",
    color: "#bb9af7",
    filled: true,
    manual: true,
  },
  "circle-back": { label: "Will circle back", color: "#22d3ee", filled: true, manual: true },
} as const;

export function resolveThreadStatusPresentation(
  status: keyof typeof STATUS,
  unread: boolean,
  followUp?: OrchestrationV2FollowUp | null,
) {
  if (
    status === "approval" ||
    status === "input" ||
    status === "request" ||
    status === "limited" ||
    status === "failed"
  ) {
    return STATUS[status];
  }
  if (followUp != null) return FOLLOW_UP[followUp];
  return status === "ready" && unread ? COMPLETED : STATUS[status];
}

export function toggleThreadFollowUp(
  current: OrchestrationV2FollowUp | null | undefined,
  selected: OrchestrationV2FollowUp,
): OrchestrationV2FollowUp | null {
  return current === selected ? null : selected;
}

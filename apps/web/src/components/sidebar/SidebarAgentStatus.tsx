import type { OrchestrationV2FollowUp } from "@t3tools/contracts";
import { resolveThreadStatusPresentation } from "@t3tools/client-runtime/state/thread-status-presentation";
import type { SidebarThreadStatus } from "../Sidebar.logic";

export function SidebarAgentStatus({
  status,
  unread,
  followUp,
}: {
  status: SidebarThreadStatus;
  unread: boolean;
  followUp?: OrchestrationV2FollowUp | null;
}) {
  const {
    label: statusLabel,
    color,
    filled,
    manual,
  } = resolveThreadStatusPresentation(status, unread, followUp);
  const label = manual ? statusLabel : `Agent: ${statusLabel}`;

  return (
    <svg role="img" aria-label={label} viewBox="0 0 12 12" className="size-3 shrink-0">
      <title>{label}</title>
      <circle cx="6" cy="6" r="4" stroke={color} strokeWidth="2" fill={filled ? color : "none"} />
    </svg>
  );
}

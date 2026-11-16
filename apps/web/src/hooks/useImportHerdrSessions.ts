import {
  isAtomCommandInterrupted,
  settlePromise,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import type { HerdrSessionReimport, ScopedProjectRef, ThreadId } from "@t3tools/contracts";

import { stackedThreadToast, toastManager } from "../components/ui/toast";
import { readLocalApi } from "../localApi";
import { agentSessionImport } from "../state/agentSessions";
import { useAtomCommand } from "../state/use-atom-command";

const relationshipDescriptions = {
  herdr_ahead: "Herdr has newer history than this T3 thread.",
  t3_ahead: "This T3 thread has work that is not in Herdr.",
  diverged: "Herdr and this T3 thread have both changed independently.",
};

export function useImportHerdrSessions() {
  const importAgentSessions = useAtomCommand(agentSessionImport, { reportFailure: false });

  return async (projectRef: ScopedProjectRef, workspaceRoot: string) => {
    const api = readLocalApi();
    if (!api) return;

    const importSessions = async (reimport?: readonly HerdrSessionReimport[]) => {
      const result = await importAgentSessions({
        environmentId: projectRef.environmentId,
        input: {
          projectId: projectRef.projectId,
          expectedWorkspaceRoot: workspaceRoot,
          source: "herdr",
          ...(reimport ? { reimport } : {}),
        },
      });
      if (result._tag === "Success") return result.value;
      if (!isAtomCommandInterrupted(result)) {
        const error = squashAtomCommandFailure(result);
        toastManager.add(
          stackedThreadToast({
            type: "error",
            title: reimport
              ? "Failed to refresh Herdr OMP sessions"
              : "Failed to import Herdr OMP sessions",
            description: error instanceof Error ? error.message : "An error occurred.",
          }),
        );
      }
      return null;
    };

    const imported = await importSessions();
    if (imported === null) return;

    let importedCount = imported.importedCount;
    let refreshedCount = imported.reimportedCount ?? 0;
    const preservedThreadIds = new Set<ThreadId>(imported.preservedThreadIds);
    const showResults = () => {
      const branchCount = preservedThreadIds.size;
      toastManager.add({
        type: importedCount + refreshedCount > 0 ? "success" : "info",
        title: "Herdr OMP import results",
        description: `${importedCount} imported, ${refreshedCount} refreshed, ${branchCount} saved branch${branchCount === 1 ? "" : "es"}.`,
      });
    };

    const reimport: HerdrSessionReimport[] = [];
    for (const change of imported.herdrChanges ?? []) {
      if (change.blockedReason !== null) {
        toastManager.add(
          stackedThreadToast({
            type: "warning",
            title: `Cannot refresh "${change.title}"`,
            description: change.blockedReason,
          }),
        );
        continue;
      }
      const confirmed = await settlePromise(() =>
        api.dialogs.confirm(
          [
            `Restore "${change.title}" from Herdr?`,
            relationshipDescriptions[change.relationship],
            "Herdr is authoritative. This refreshes the same T3 thread with Herdr's history, rather than creating a new thread.",
            "Any T3-only work displaced by the restore will be preserved as a saved branch.",
            "Decline to leave this thread unchanged.",
          ].join("\n\n"),
        ),
      );
      if (confirmed._tag === "Failure") {
        showResults();
        return;
      }
      if (confirmed.value) {
        reimport.push({
          threadId: change.threadId,
          sourceVersion: change.sourceVersion,
          workingVersion: change.workingVersion,
        });
      }
    }

    if (reimport.length > 0) {
      const refreshed = await importSessions(reimport);
      if (refreshed !== null) {
        importedCount = refreshed.importedCount;
        refreshedCount = refreshed.reimportedCount ?? 0;
        for (const threadId of refreshed.preservedThreadIds ?? []) preservedThreadIds.add(threadId);
        for (const change of refreshed.herdrChanges ?? []) {
          if (!reimport.some((accepted) => accepted.threadId === change.threadId)) continue;
          toastManager.add(
            stackedThreadToast({
              type: "warning",
              title: `"${change.title}" was not refreshed`,
              description:
                change.blockedReason ??
                "The session changed after confirmation. Import Herdr OMP sessions again to review its latest history.",
            }),
          );
        }
      }
    }
    showResults();
  };
}

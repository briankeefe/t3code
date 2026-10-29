import type {
  OrchestrationV2UserInputQuestion,
  ProviderApprovalDecision,
} from "@t3tools/contracts";

export type OmpRecord = Record<string, unknown>;
export const field = (value: unknown, key: string): unknown =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as OmpRecord)[key]
    : undefined;
export const string = (value: unknown, key: string): string | undefined => {
  const result = field(value, key);
  return typeof result === "string" ? result : undefined;
};
export const number = (value: unknown, key: string): number | undefined => {
  const result = field(value, key);
  return typeof result === "number" && Number.isFinite(result) ? result : undefined;
};
export const contentText = (value: unknown): string =>
  typeof value === "string"
    ? value
    : Array.isArray(value)
      ? value
          .map((block) => (string(block, "type") === "text" ? (string(block, "text") ?? "") : ""))
          .join("")
      : "";

export function ompCompletion(
  frame: OmpRecord,
  promptId: string,
  admitted = false,
): "completed" | "interrupted" | "failed" | null {
  if (frame.type === "session_settled") return "completed";
  if (frame.id !== promptId) return null;
  if (frame.type === "response") {
    if (frame.success === false) return admitted ? null : "failed";
    return field(frame.data, "agentInvoked") === false ? "completed" : null;
  }
  if (frame.type !== "prompt_result" || frame.sessionSettled !== true) return null;
  return frame.status === "error"
    ? "failed"
    : frame.status === "aborted"
      ? "interrupted"
      : "completed";
}

export interface OmpEntry extends OmpRecord {
  readonly id: string;
}

export function walkOmpBranch(data: unknown): OmpEntry[] {
  const raw = field(data, "entries");
  if (!Array.isArray(raw)) throw new Error("OMP returned no session entries");
  const entries = new Map<string, OmpEntry>();
  for (const entry of raw) {
    const id = string(entry, "id");
    if (id !== undefined) entries.set(id, { ...entry, id });
  }
  const branch: OmpEntry[] = [];
  let id = string(data, "leafId");
  const visited = new Set<string>();
  while (id !== undefined) {
    const entry = entries.get(id);
    if (entry === undefined || visited.has(id))
      throw new Error("OMP session ancestry is incomplete or cyclic");
    visited.add(id);
    branch.push(entry);
    id = string(entry, "parentId");
  }
  return branch.reverse();
}

const labelOption = (label: string, description?: string) => ({
  label: label || "Empty value",
  description: description?.trim() || label || "Empty value",
  value: label,
});

export function ompQuestions(frame: OmpRecord): OrchestrationV2UserInputQuestion[] {
  if (frame.method === "ask" && Array.isArray(frame.questions)) {
    return frame.questions.map((question, index) => {
      const title = string(question, "question")?.trim() || "Question";
      const options = field(question, "options");
      return {
        id: string(question, "id") || `${frame.id}:${index}`,
        header: string(question, "header")?.trim() || title,
        question: title,
        options: Array.isArray(options)
          ? options.flatMap((option) => {
              const label = string(option, "label");
              return label === undefined
                ? []
                : [
                    {
                      ...labelOption(label, string(option, "description")),
                      ...(string(option, "preview") === undefined
                        ? {}
                        : { preview: string(option, "preview")! }),
                    },
                  ];
            })
          : [],
        multiSelect: field(question, "multi") === true,
        allowCustomAnswer: true,
      };
    });
  }
  const title = string(frame, "title")?.trim() || "OMP question";
  const options = Array.isArray(frame.options)
    ? frame.options.filter((option): option is string => typeof option === "string")
    : [];
  const details = Array.isArray(frame.optionDetails) ? frame.optionDetails : [];
  return [
    {
      id: string(frame, "id") || "question",
      header: title,
      question: string(frame, "message")?.trim() || string(frame, "placeholder")?.trim() || title,
      options: options.map((label, index) =>
        labelOption(label, string(details[index], "description")),
      ),
      allowCustomAnswer: frame.method !== "select",
      ...(frame.method === "editor" ? { initialAnswer: string(frame, "prefill") ?? "" } : {}),
    },
  ];
}

export function ompUiResponse(
  frame: OmpRecord,
  decision?: ProviderApprovalDecision,
  answers?: Record<string, unknown>,
): OmpRecord {
  const base = { type: "extension_ui_response", id: frame.id };
  if (frame.method === "confirm")
    return { ...base, confirmed: decision === "accept" || decision === "acceptForSession" };
  if (frame.method === "ask" && Array.isArray(frame.questions)) {
    return {
      ...base,
      answers: ompQuestions(frame).map((question) => {
        const answer = answers?.[question.id];
        const values =
          typeof answer === "string"
            ? [answer]
            : Array.isArray(answer)
              ? answer.filter((v): v is string => typeof v === "string")
              : [];
        const labels = new Set(question.options.map((option) => option.value ?? option.label));
        const selectedOptions = values.filter((value) => labels.has(value));
        const customInput = values
          .filter((value) => !labels.has(value))
          .join("\n")
          .trim();
        return { id: question.id, selectedOptions, ...(customInput ? { customInput } : {}) };
      }),
    };
  }
  const answer = answers?.[String(frame.id)];
  return typeof answer === "string" ? { ...base, value: answer } : { ...base, cancelled: true };
}

import { useState } from "react";
import { Button } from "../ui/button";
import { Textarea } from "../ui/textarea";
import { Dialog, DialogDescription, DialogHeader, DialogPopup, DialogTitle } from "../ui/dialog";
import { useCopyToClipboard } from "../../hooks/useCopyToClipboard";
import type { BackgroundQuestionsView } from "@t3tools/client-runtime/state/backgroundQuestionController";

export function BackgroundQuestionsPanel({
  questions,
}: {
  readonly questions: BackgroundQuestionsView;
}) {
  const record =
    questions.selectedId === ""
      ? undefined
      : (questions.records.find((entry) => entry.id === questions.selectedId) ??
        questions.records[0]);
  return (
    <>
      {questions.supported ? (
        <div className="flex justify-end pb-1">
          <Button variant="ghost" size="sm" onClick={() => questions.setOpen(true)}>
            Background answers
          </Button>
        </div>
      ) : null}
      <Dialog open={questions.open} onOpenChange={questions.setOpen}>
        <DialogPopup>
          <DialogHeader>
            <DialogTitle>Background answers</DialogTitle>
            <DialogDescription>
              Separate from the conversation. Draft attachments stay in the main composer. Closing
              does not cancel work.
            </DialogDescription>
          </DialogHeader>
          <div className="flex max-h-[65dvh] flex-col gap-4 overflow-y-auto px-6 pb-6">
            <div className="flex flex-wrap gap-2">
              <Button
                variant="outline"
                size="sm"
                disabled={!questions.canAsk || questions.pending}
                onClick={() => questions.select("")}
              >
                New topic
              </Button>
              <Button
                variant="outline"
                size="sm"
                disabled={!questions.supported || !questions.canAsk || questions.pending}
                onClick={() => void questions.ask({ kind: "pulse" })}
              >
                Check pulse
              </Button>
              <Button variant="ghost" size="sm" onClick={questions.refresh}>
                Refresh history
              </Button>
            </div>
            {questions.error ? (
              <p role="alert" className="text-sm text-destructive">
                {questions.error}
              </p>
            ) : null}
            {questions.loading ? <p role="status">Loading saved answers…</p> : null}
            {questions.records.length > 0 ? (
              <label className="flex flex-col gap-1 text-sm">
                Saved topics
                <select
                  className="w-full rounded-md border bg-background p-2 text-foreground"
                  value={record?.id ?? ""}
                  onChange={(event) => questions.select(event.target.value)}
                >
                  <option value="">New topic</option>
                  {questions.records.map((entry) => (
                    <option key={entry.id} value={entry.id}>
                      {entry.kind === "pulse" ? "Pulse: " : ""}
                      {entry.turns[0]?.question.slice(0, 100) || "Background question"}
                    </option>
                  ))}
                </select>
              </label>
            ) : !questions.loading ? (
              <p className="text-sm text-muted-foreground">No saved answers yet.</p>
            ) : null}
            <BackgroundTopic key={record?.id ?? "new"} record={record} questions={questions} />
          </div>
        </DialogPopup>
      </Dialog>
    </>
  );
}

function BackgroundTopic({
  record,
  questions,
}: {
  readonly record: BackgroundQuestionsView["records"][number] | undefined;
  readonly questions: BackgroundQuestionsView;
}) {
  const [question, setQuestion] = useState("");
  const { copyToClipboard, isCopied } = useCopyToClipboard({ target: "suggested steer" });
  const running = record?.turns.at(-1)?.status === "running";
  const ask = async () => {
    const text = question.trim();
    if (!text) return;
    const accepted = await questions.ask({
      kind: "btw",
      question: text,
      ...(record?.kind === "btw" ? { recordId: record.id } : {}),
    });
    if (accepted) setQuestion((current) => (current === question ? "" : current));
  };
  return (
    <>
      {record?.turns.map((turn, index) => (
        <section
          key={turn.createdAt}
          aria-label={`Background answer ${index + 1}`}
          className="space-y-2 border-t pt-3"
        >
          <h3 className="whitespace-pre-wrap text-sm font-medium">
            {record.kind === "pulse" ? "Agent pulse" : turn.question}
          </h3>
          <p role="status" className="text-xs text-muted-foreground">
            {turn.status}
          </p>
          {record.kind !== "pulse" || !record.report ? (
            <div className="whitespace-pre-wrap break-words text-sm">
              {turn.answer ||
                (turn.status === "running" ? "Waiting for answer…" : "No answer returned.")}
            </div>
          ) : null}
          {turn.error ? (
            <p role="alert" className="text-sm text-destructive">
              {turn.error}
            </p>
          ) : null}
        </section>
      ))}
      {record?.report ? (
        <section aria-label="Agent health report" className="space-y-3">
          <dl className="space-y-2 text-sm">
            {(["phase", "progress", "momentum", "evidence", "risk", "next"] as const).map(
              (field) => (
                <div key={field}>
                  <dt className="font-medium capitalize">{field}</dt>
                  <dd className="whitespace-pre-wrap">{record.report?.[field]}</dd>
                </div>
              ),
            )}
          </dl>
          {record.report.steer ? (
            <div className="space-y-2">
              <h3 className="text-sm font-medium">Suggested steer</h3>
              <p className="whitespace-pre-wrap text-sm">{record.report.steer}</p>
              <Button
                variant="outline"
                size="sm"
                onClick={() => copyToClipboard(record.report?.steer ?? "", undefined)}
              >
                {isCopied ? "Copied" : "Copy steer"}
              </Button>
              <p className="text-xs text-muted-foreground">
                Copy only. Review and send it yourself if needed.
              </p>
            </div>
          ) : null}
        </section>
      ) : null}
      {record?.reportError ? (
        <p role="alert" className="text-sm text-destructive">
          {record.reportError}
        </p>
      ) : null}
      {running && record ? (
        <Button
          variant="outline"
          size="sm"
          disabled={!questions.canCancel}
          onClick={() => void questions.cancel(record.id)}
        >
          Cancel background answer
        </Button>
      ) : null}
      <form
        className="space-y-2"
        onSubmit={(event) => {
          event.preventDefault();
          void ask();
        }}
      >
        <label htmlFor="background-question" className="text-sm font-medium">
          {record?.kind === "btw" ? "Follow up on this topic" : "Ask a background question"}
        </label>
        <Textarea
          id="background-question"
          value={question}
          onChange={(event) => setQuestion(event.target.value)}
          placeholder="Ask without interrupting the agent"
        />
        <Button
          type="submit"
          size="sm"
          disabled={
            !questions.supported ||
            !questions.canAsk ||
            questions.pending ||
            running ||
            !question.trim()
          }
        >
          Ask in background
        </Button>
      </form>
    </>
  );
}

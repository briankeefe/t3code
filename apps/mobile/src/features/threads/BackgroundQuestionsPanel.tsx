import type { BackgroundQuestionsView } from "@t3tools/client-runtime/state/backgroundQuestionController";
import * as Clipboard from "expo-clipboard";
import { useState } from "react";
import { ScrollView, TextInput, View } from "react-native";
import { AppText as Text } from "../../components/ui/AppText";
import { Button } from "../../components/ui/Button";
import { Sheet } from "../../components/ui/Sheet";

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
        <Button tone="text" label="Background answers" onPress={() => questions.setOpen(true)} />
      ) : null}
      {questions.open ? (
        <Sheet title="Background answers" onClose={() => questions.setOpen(false)}>
          <ScrollView
            keyboardShouldPersistTaps="handled"
            contentInsetAdjustmentBehavior="automatic"
            contentContainerClassName="gap-4 px-5 pb-8"
          >
            <Text className="text-sm text-foreground-muted">
              Separate from the conversation. Draft attachments stay in the main composer. Closing
              does not cancel work.
            </Text>
            <View className="flex-row flex-wrap gap-2">
              <Button
                label="New topic"
                disabled={!questions.supported || !questions.canAsk || questions.pending}
                onPress={() => questions.select("")}
              />
              <Button
                label="Check pulse"
                disabled={!questions.supported || !questions.canAsk || questions.pending}
                onPress={() => void questions.ask({ kind: "pulse" })}
              />
              <Button tone="text" label="Refresh history" onPress={questions.refresh} />
            </View>
            {questions.error ? (
              <Text accessibilityRole="alert" className="text-sm text-danger">
                {questions.error}
              </Text>
            ) : null}
            {questions.loading ? (
              <Text accessibilityLiveRegion="polite">Loading saved answers…</Text>
            ) : null}
            {questions.records.length > 0 ? (
              <View className="gap-2">
                <Text accessibilityRole="header" className="font-t3-medium text-foreground">
                  Saved topics
                </Text>
                {questions.records.map((entry) => (
                  <Button
                    key={entry.id}
                    tone={record?.id === entry.id ? "primary" : "secondary"}
                    label={`${entry.kind === "pulse" ? "Pulse: " : ""}${entry.turns[0]?.question.slice(0, 100) || "Background question"}`}
                    onPress={() => questions.select(entry.id)}
                  />
                ))}
              </View>
            ) : !questions.loading ? (
              <Text className="text-sm text-foreground-muted">No saved answers yet.</Text>
            ) : null}
            <BackgroundTopic key={record?.id ?? "new"} record={record} questions={questions} />
          </ScrollView>
        </Sheet>
      ) : null}
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
  const [copyStatus, setCopyStatus] = useState<string | null>(null);
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
    <View className="gap-4">
      {record?.turns.map((turn) => (
        <View key={turn.createdAt} className="gap-2 border-t border-border pt-3">
          <Text accessibilityRole="header" selectable className="font-t3-medium text-foreground">
            {record.kind === "pulse" ? "Agent pulse" : turn.question}
          </Text>
          <Text accessibilityLiveRegion="polite" className="text-xs text-foreground-muted">
            {turn.status}
          </Text>
          {record.kind !== "pulse" || !record.report ? (
            <Text selectable className="text-sm text-foreground">
              {turn.answer ||
                (turn.status === "running" ? "Waiting for answer…" : "No answer returned.")}
            </Text>
          ) : null}
          {turn.error ? (
            <Text accessibilityRole="alert" className="text-sm text-danger">
              {turn.error}
            </Text>
          ) : null}
        </View>
      ))}
      {record?.report ? (
        <View accessibilityLabel="Agent health report" className="gap-3">
          {(["phase", "progress", "momentum", "evidence", "risk", "next"] as const).map((field) => (
            <View key={field} className="gap-1">
              <Text className="font-t3-medium capitalize text-foreground">{field}</Text>
              <Text selectable className="text-sm text-foreground">
                {record.report?.[field]}
              </Text>
            </View>
          ))}
          {record.report.steer ? (
            <View className="gap-2">
              <Text accessibilityRole="header" className="font-t3-medium text-foreground">
                Suggested steer
              </Text>
              <Text selectable className="text-sm text-foreground">
                {record.report.steer}
              </Text>
              <Button
                label="Copy steer"
                onPress={() => {
                  void Clipboard.setStringAsync(record.report?.steer ?? "").then(
                    () => setCopyStatus("Copied"),
                    () => setCopyStatus("Could not copy suggested steer."),
                  );
                }}
              />
              {copyStatus ? (
                <Text accessibilityLiveRegion="polite" className="text-sm text-foreground">
                  {copyStatus}
                </Text>
              ) : null}
              <Text className="text-xs text-foreground-muted">
                Copy only. Review and send it yourself if needed.
              </Text>
            </View>
          ) : null}
        </View>
      ) : null}
      {record?.reportError ? (
        <Text accessibilityRole="alert" className="text-sm text-danger">
          {record.reportError}
        </Text>
      ) : null}
      {running && record ? (
        <Button
          label="Cancel background answer"
          disabled={!questions.canCancel}
          onPress={() => void questions.cancel(record.id)}
        />
      ) : null}
      <View className="gap-2">
        <Text className="font-t3-medium text-foreground">
          {record?.kind === "btw" ? "Follow up on this topic" : "Ask a background question"}
        </Text>
        <TextInput
          accessibilityLabel={
            record?.kind === "btw" ? "Follow up on this topic" : "Ask a background question"
          }
          multiline
          value={question}
          onChangeText={setQuestion}
          placeholder="Ask without interrupting the agent"
          placeholderTextColorClassName="accent-placeholder"
          className="min-h-24 rounded-xl border border-border bg-screen p-3 text-foreground"
        />
        <Button
          label="Ask in background"
          disabled={
            !questions.supported ||
            !questions.canAsk ||
            questions.pending ||
            running ||
            !question.trim()
          }
          onPress={() => void ask()}
        />
      </View>
    </View>
  );
}

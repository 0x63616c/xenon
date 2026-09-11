package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/proto"
)

func generatedCompleted() *historypb.History {
	inner, _ := proto.Marshal(&commonpb.Payload{})
	return &historypb.History{Events: []*historypb.HistoryEvent{
		{EventId: 1, EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{WorkflowType: &commonpb.WorkflowType{Name: "kitchenSink"}}}},
		{EventId: 2, EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{Result: &commonpb.Payloads{Payloads: []*commonpb.Payload{{Metadata: map[string][]byte{"encoding": []byte("_passthrough")}, Data: inner}}}}}},
	}}
}
func validGeneratedFile() *generatedIntentFile {
	raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(&commonpb.Payload{})
	return &generatedIntentFile{Schema: 1, InputSHA256: strings.Repeat("a", 64), IntentSHA256: strings.Repeat("b", 64), Intent: generatedIntent{Schema: 1, InputSHA256: strings.Repeat("a", 64), ExpandedInput: json.RawMessage(`{}`), Nodes: []generatedNode{{ID: "root", Kind: "root", InputSHA256: strings.Repeat("c", 64), Terminal: "completed", ResultSHA256: digestGenerated(raw)}}, Counts: generatedCounts{Roots: 1}, Limits: generatedFanout{2048, 2048, 2048}}}
}
func validGeneratedRun() ([]runAudit, map[string]*historypb.History) {
	run := runAudit{WorkflowID: "w-case-1", RunID: "run-1", Status: enums.WORKFLOW_EXECUTION_STATUS_COMPLETED.String(), Events: map[string]int{}}
	return []runAudit{run}, map[string]*historypb.History{run.RunID: generatedCompleted()}
}

func TestGeneratedIntentRejectsNamedGraphMutants(t *testing.T) {
	runs, histories := validGeneratedRun()
	if _, err := checkGeneratedIntent(validGeneratedFile(), runs, histories, "test"); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*generatedIntentFile, *[]runAudit, map[string]*historypb.History){
		"omitted execution": func(_ *generatedIntentFile, r *[]runAudit, _ map[string]*historypb.History) { *r = nil },
		"wrong effect count": func(_ *generatedIntentFile, _ *[]runAudit, h map[string]*historypb.History) {
			h["run-1"].Events = append(h["run-1"].Events[:1],
				&historypb.HistoryEvent{EventId: 2, EventType: enums.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{}}},
				h["run-1"].Events[1])
		},
		"corrupt result": func(_ *generatedIntentFile, _ *[]runAudit, h map[string]*historypb.History) {
			h["run-1"].Events[1].GetWorkflowExecutionCompletedEventAttributes().Result.Payloads[0].Data = []byte("bad")
		},
		"fanout limit": func(f *generatedIntentFile, r *[]runAudit, h map[string]*historypb.History) {
			f.Intent.Nodes[0].Activities, f.Intent.Counts.Activities, f.Intent.Limits.Activities = 1, 1, 0
			h["run-1"].Events = append(h["run-1"].Events[:1], &historypb.HistoryEvent{EventId: 2, EventType: enums.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED}, &historypb.HistoryEvent{EventId: 3, EventType: enums.EVENT_TYPE_ACTIVITY_TASK_COMPLETED}, h["run-1"].Events[1])
			(*r)[0].Events[enums.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED.String()] = 1
		},
	} {
		t.Run(name, func(t *testing.T) {
			fileRaw, _ := json.Marshal(validGeneratedFile())
			var file generatedIntentFile
			_ = json.Unmarshal(fileRaw, &file)
			runRaw, _ := json.Marshal(runs)
			var changedRuns []runAudit
			_ = json.Unmarshal(runRaw, &changedRuns)
			changedHistories := map[string]*historypb.History{"run-1": proto.Clone(histories["run-1"]).(*historypb.History)}
			mutate(&file, &changedRuns, changedHistories)
			if _, err := checkGeneratedIntent(&file, changedRuns, changedHistories, "test"); err == nil {
				t.Fatal("generated graph mutant accepted")
			}
		})
	}
}

func TestGeneratedIntentFileBindsExactInputAndIntent(t *testing.T) {
	dir := t.TempDir()
	input := []byte("input")
	inputPath := filepath.Join(dir, "input.proto")
	intentPath := filepath.Join(dir, "intent.json")
	if err := os.WriteFile(inputPath, input, 0600); err != nil {
		t.Fatal(err)
	}
	file := validGeneratedFile()
	file.InputSHA256 = digestGenerated(input)
	file.Intent.InputSHA256 = file.InputSHA256
	intentRaw, _ := json.Marshal(file.Intent)
	file.IntentSHA256 = digestGenerated(intentRaw)
	raw, _ := json.Marshal(file)
	if err := os.WriteFile(intentPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadGeneratedIntent(intentPath, inputPath); err != nil {
		t.Fatal(err)
	}
	file.Intent.Counts.Roots = 2
	raw, _ = json.Marshal(file)
	if err := os.WriteFile(intentPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadGeneratedIntent(intentPath, inputPath); err == nil {
		t.Fatal("changed intent accepted")
	}
}

func TestGeneratedFanoutUsesScheduledIdentity(t *testing.T) {
	h := &historypb.History{Events: []*historypb.HistoryEvent{
		{EventId: 1, EventType: enums.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED},
		{EventId: 2, EventType: enums.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED},
		{EventId: 3, EventType: enums.EVENT_TYPE_ACTIVITY_TASK_COMPLETED, Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{ScheduledEventId: 1}}},
		{EventId: 4, EventType: enums.EVENT_TYPE_ACTIVITY_TASK_COMPLETED, Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{ScheduledEventId: 2}}},
	}}
	if peak, err := peakGenerated(h, enums.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED); err != nil || peak != 2 {
		t.Fatal(peak, err)
	}
	h.Events[3].GetActivityTaskCompletedEventAttributes().ScheduledEventId = 1
	if _, err := peakGenerated(h, enums.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED); err == nil {
		t.Fatal("duplicate terminal accepted")
	}
}

func TestGeneratedIntentPanicInputsReturnErrors(t *testing.T) {
	runs, histories := validGeneratedRun()
	histories["run-1"] = &historypb.History{}
	if _, err := checkGeneratedIntent(validGeneratedFile(), runs, histories, "test"); err == nil {
		t.Fatal("empty history accepted")
	}
	if _, err := generatedToken(nil, "test"); err == nil {
		t.Fatal("nil Nexus start accepted")
	}
}

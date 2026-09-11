package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"

	commonpb "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/proto"
)

const generatedIntentContract = "omes-generated-intent-v1"

type generatedCounts struct {
	Roots           int `json:"roots"`
	Children        int `json:"children"`
	Continuations   int `json:"continuations"`
	Activities      int `json:"activities"`
	NexusOperations int `json:"nexus_operations"`
	NexusHandlers   int `json:"nexus_handlers"`
}
type generatedFanout struct {
	Children   int `json:"children"`
	Activities int `json:"activities"`
	Nexus      int `json:"nexus"`
}
type generatedNode struct {
	ID              string   `json:"id"`
	Kind            string   `json:"kind"`
	Parent          string   `json:"parent,omitempty"`
	Previous        string   `json:"previous,omitempty"`
	InputSHA256     string   `json:"input_sha256"`
	Children        []string `json:"children,omitempty"`
	NexusHandlers   []string `json:"nexus_handlers,omitempty"`
	Next            string   `json:"next,omitempty"`
	Activities      int      `json:"activities"`
	NexusOperations int      `json:"nexus_operations"`
	Terminal        string   `json:"terminal"`
	ResultSHA256    string   `json:"result_sha256,omitempty"`
	ResultString    string   `json:"result_string,omitempty"`
}
type generatedIntent struct {
	Schema        int             `json:"schema"`
	InputSHA256   string          `json:"input_sha256"`
	ExpandedInput json.RawMessage `json:"expanded_input"`
	Nodes         []generatedNode `json:"nodes"`
	Counts        generatedCounts `json:"counts"`
	MaxFanout     generatedFanout `json:"max_fanout"`
	Limits        generatedFanout `json:"limits"`
}
type generatedIntentFile struct {
	Schema       int             `json:"schema"`
	InputSHA256  string          `json:"input_sha256"`
	IntentSHA256 string          `json:"intent_sha256"`
	Intent       generatedIntent `json:"intent"`
}
type generatedAudit struct {
	Contract          string          `json:"contract"`
	InputSHA256       string          `json:"input_sha256"`
	IntentSHA256      string          `json:"intent_sha256"`
	Expected          generatedCounts `json:"expected"`
	Observed          generatedCounts `json:"observed"`
	ExpectedMaxFanout generatedFanout `json:"expected_max_fanout"`
	ObservedMaxFanout generatedFanout `json:"observed_max_fanout"`
	Limits            generatedFanout `json:"limits"`
	CountSemantics    string          `json:"count_semantics"`
}

func strictGenerated(raw []byte, value any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return err
	}
	var trailing any
	if err := d.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing generated intent data")
		}
		return err
	}
	return nil
}
func boundedRead(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(f, limit+1))
	closeErr := f.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if int64(len(raw)) > limit {
		return nil, fmt.Errorf("generated intent input exceeds bound")
	}
	return raw, nil
}
func digestGenerated(raw []byte) string { return fmt.Sprintf("%x", sha256.Sum256(raw)) }
func loadGeneratedIntent(path, inputPath string) (*generatedIntentFile, error) {
	raw, err := boundedRead(path, 8<<20)
	if err != nil {
		return nil, err
	}
	var file generatedIntentFile
	if err = strictGenerated(raw, &file); err != nil {
		return nil, err
	}
	input, err := boundedRead(inputPath, 1<<20)
	if err != nil {
		return nil, err
	}
	intentRaw, err := json.Marshal(file.Intent)
	if err != nil {
		return nil, err
	}
	if file.Schema != 1 || file.InputSHA256 != digestGenerated(input) || file.IntentSHA256 != digestGenerated(intentRaw) || file.Intent.InputSHA256 != file.InputSHA256 || file.Intent.Schema != 1 || !json.Valid(file.Intent.ExpandedInput) {
		return nil, fmt.Errorf("generated intent hash binding")
	}
	if file.Intent.Counts.Roots != 1 || file.Intent.Limits != (generatedFanout{2048, 2048, 2048}) || len(file.Intent.Nodes) != file.Intent.Counts.Roots+file.Intent.Counts.Children+file.Intent.Counts.Continuations+file.Intent.Counts.NexusHandlers {
		return nil, fmt.Errorf("generated intent inventory")
	}
	ids := map[string]generatedNode{}
	for _, node := range file.Intent.Nodes {
		_, digestErr := hex.DecodeString(node.InputSHA256)
		if node.ID == "" || ids[node.ID].ID != "" || digestErr != nil || len(node.InputSHA256) != 64 || node.Activities < 0 || node.NexusOperations < 0 {
			return nil, fmt.Errorf("generated intent node")
		}
		ids[node.ID] = node
	}
	if root := ids["root"]; root.Kind != "root" || root.Parent != "" || root.Previous != "" {
		return nil, fmt.Errorf("generated intent root")
	}
	seen, incoming := map[string]bool{}, map[string]int{}
	var walk func(string) error
	walk = func(id string) error {
		if seen[id] {
			return fmt.Errorf("generated intent cycle")
		}
		seen[id] = true
		n := ids[id]
		if n.Kind == "nexus-handler" {
			if n.Terminal != "completed-nexus" {
				return fmt.Errorf("generated intent terminal")
			}
		} else if n.Next != "" {
			if n.Terminal != "continued-as-new" {
				return fmt.Errorf("generated intent terminal")
			}
		} else if n.Terminal != "completed" || len(n.ResultSHA256) != 64 {
			return fmt.Errorf("generated intent terminal")
		}
		for _, ref := range append(append(append([]string(nil), n.Children...), n.NexusHandlers...), n.Next) {
			if ref == "" {
				continue
			}
			target, ok := ids[ref]
			incoming[ref]++
			if !ok || ref == id || incoming[ref] != 1 || target.Parent != id && target.Previous != id {
				return fmt.Errorf("generated intent edge")
			}
			if err := walk(ref); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk("root"); err != nil || len(seen) != len(ids) {
		return nil, fmt.Errorf("generated intent graph")
	}
	return &file, nil
}

func unwrapGeneratedPayload(p *commonpb.Payload) *commonpb.Payload {
	if p == nil || string(p.Metadata["encoding"]) != "_passthrough" {
		return nil
	}
	inner := new(commonpb.Payload)
	if proto.Unmarshal(p.Data, inner) != nil {
		return nil
	}
	return inner
}
func generatedWorkflowDigest(payloads *commonpb.Payloads) string {
	return generatedMessageDigest(payloads, "temporal.omes.kitchen_sink.WorkflowInput")
}
func generatedMessageDigest(payloads *commonpb.Payloads, messageType string) string {
	if payloads == nil || len(payloads.Payloads) != 1 {
		return ""
	}
	inner := unwrapGeneratedPayload(payloads.Payloads[0])
	if inner == nil || string(inner.Metadata["encoding"]) != "binary/protobuf" || string(inner.Metadata["messageType"]) != messageType {
		return ""
	}
	return digestGenerated(inner.Data)
}
func checkGeneratedTerminal(node generatedNode, run runAudit, h *historypb.History) error {
	if h == nil || len(h.Events) == 0 {
		return fmt.Errorf("generated_graph/missing_terminal")
	}
	last := h.Events[len(h.Events)-1]
	switch node.Terminal {
	case "continued-as-new":
		if run.Status != enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW.String() || last.GetWorkflowExecutionContinuedAsNewEventAttributes() == nil {
			return fmt.Errorf("generated_graph/terminal")
		}
	case "completed":
		if node.Kind == "child" && run.Status == enums.WORKFLOW_EXECUTION_STATUS_TERMINATED.String() && last.GetWorkflowExecutionTerminatedEventAttributes() != nil {
			return nil
		}
		if run.Status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED.String() || last.GetWorkflowExecutionCompletedEventAttributes() == nil || generatedResultDigest(h) != node.ResultSHA256 {
			return fmt.Errorf("generated_graph/result")
		}
	case "completed-nexus":
		a := last.GetWorkflowExecutionCompletedEventAttributes()
		if run.Status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED.String() || a == nil || len(a.GetResult().GetPayloads()) != 1 {
			return fmt.Errorf("generated_graph/nexus_result")
		}
		var result string
		p := a.Result.Payloads[0]
		if string(p.Metadata["encoding"]) != "json/plain" || json.Unmarshal(p.Data, &result) != nil || result != node.ResultString {
			return fmt.Errorf("generated_graph/nexus_result")
		}
	default:
		return fmt.Errorf("generated_graph/unknown_terminal")
	}
	return nil
}
func generatedResultDigest(h *historypb.History) string {
	if h == nil || len(h.Events) == 0 {
		return ""
	}
	a := h.Events[len(h.Events)-1].GetWorkflowExecutionCompletedEventAttributes()
	if a == nil || len(a.GetResult().GetPayloads()) != 1 {
		return ""
	}
	inner := unwrapGeneratedPayload(a.Result.Payloads[0])
	if inner == nil {
		return ""
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(inner)
	if err != nil {
		return ""
	}
	return digestGenerated(raw)
}
func generatedToken(event *historypb.HistoryEvent, namespace string) (string, error) {
	a := event.GetNexusOperationStartedEventAttributes()
	if a == nil || len(a.OperationToken) == 0 || len(a.OperationToken) > 4096 {
		return "", fmt.Errorf("generated Nexus start/token")
	}
	raw, err := base64.RawURLEncoding.DecodeString(a.OperationToken)
	if err != nil {
		return "", err
	}
	var token struct {
		Type       int    `json:"t"`
		Namespace  string `json:"ns"`
		WorkflowID string `json:"wid"`
		Version    int    `json:"v"`
	}
	if json.Unmarshal(raw, &token) != nil || token.Type != 1 || token.Version != 0 || token.Namespace != namespace || token.WorkflowID == "" {
		return "", fmt.Errorf("invalid generated Nexus token")
	}
	return token.WorkflowID, nil
}

func peakGenerated(h *historypb.History, scheduled enums.EventType) (int, error) {
	active, peak := map[int64]bool{}, 0
	for _, event := range h.Events {
		if event.EventType == scheduled {
			if active[event.EventId] {
				return 0, fmt.Errorf("generated duplicate schedule")
			}
			active[event.EventId] = true
			if len(active) > peak {
				peak = len(active)
			}
			continue
		}
		var id int64
		switch scheduled {
		case enums.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			if a := event.GetActivityTaskCompletedEventAttributes(); a != nil {
				id = a.ScheduledEventId
			}
			if a := event.GetActivityTaskFailedEventAttributes(); a != nil {
				id = a.ScheduledEventId
			}
			if a := event.GetActivityTaskTimedOutEventAttributes(); a != nil {
				id = a.ScheduledEventId
			}
			if a := event.GetActivityTaskCanceledEventAttributes(); a != nil {
				id = a.ScheduledEventId
			}
		case enums.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED:
			if a := event.GetNexusOperationCompletedEventAttributes(); a != nil {
				id = a.ScheduledEventId
			}
			if a := event.GetNexusOperationFailedEventAttributes(); a != nil {
				id = a.ScheduledEventId
			}
			if a := event.GetNexusOperationTimedOutEventAttributes(); a != nil {
				id = a.ScheduledEventId
			}
			if a := event.GetNexusOperationCanceledEventAttributes(); a != nil {
				id = a.ScheduledEventId
			}
		case enums.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED:
			if a := event.GetChildWorkflowExecutionCompletedEventAttributes(); a != nil {
				id = a.InitiatedEventId
			}
			if a := event.GetChildWorkflowExecutionFailedEventAttributes(); a != nil {
				id = a.InitiatedEventId
			}
			if a := event.GetChildWorkflowExecutionTimedOutEventAttributes(); a != nil {
				id = a.InitiatedEventId
			}
			if a := event.GetChildWorkflowExecutionCanceledEventAttributes(); a != nil {
				id = a.InitiatedEventId
			}
			if a := event.GetChildWorkflowExecutionTerminatedEventAttributes(); a != nil {
				id = a.InitiatedEventId
			}
			if a := event.GetStartChildWorkflowExecutionFailedEventAttributes(); a != nil {
				id = a.InitiatedEventId
			}
		}
		if id != 0 {
			if !active[id] {
				return 0, fmt.Errorf("generated fanout terminal lacks schedule")
			}
			delete(active, id)
		}
	}
	return peak, nil
}

func checkGeneratedIntent(file *generatedIntentFile, runs []runAudit, histories map[string]*historypb.History, namespace string) (generatedAudit, error) {
	audit := generatedAudit{Contract: generatedIntentContract, InputSHA256: file.InputSHA256, IntentSHA256: file.IntentSHA256, Expected: file.Intent.Counts, ExpectedMaxFanout: file.Intent.MaxFanout, Limits: file.Intent.Limits, CountSemantics: "input-derived counts are upper bounds; observed executions and effects must map to declared nodes, while cancel, abandon and parent-close races may prevent declared actions from starting"}
	if len(runs) > len(file.Intent.Nodes) {
		return audit, fmt.Errorf("generated_graph/execution_inventory")
	}
	byRun, byWorkflow, nodes := map[string]runAudit{}, map[string]runAudit{}, map[string]generatedNode{}
	for _, run := range runs {
		byRun[run.RunID] = run
		if old := byWorkflow[run.WorkflowID]; old.WorkflowID != "" {
			// Continue-As-New shares a workflow ID and is resolved by run links.
			byWorkflow[run.WorkflowID] = runAudit{}
		} else {
			byWorkflow[run.WorkflowID] = run
		}
	}
	for _, node := range file.Intent.Nodes {
		nodes[node.ID] = node
	}
	var root runAudit
	for _, run := range runs {
		h := histories[run.RunID]
		if h == nil || len(h.Events) == 0 {
			return audit, fmt.Errorf("generated_graph/missing_history")
		}
		start := h.Events[0].GetWorkflowExecutionStartedEventAttributes()
		if start.GetParentWorkflowExecution() == nil && start.GetContinuedExecutionRunId() == "" && start.GetWorkflowType().GetName() != "NexusHandlerWorkflow" {
			if root.RunID != "" {
				return audit, fmt.Errorf("generated_graph/root_inventory")
			}
			root = run
		}
	}
	if root.RunID == "" {
		return audit, fmt.Errorf("generated_graph/root_inventory")
	}
	bound, used := map[string]runAudit{}, map[string]bool{}
	var bind func(string, runAudit) error
	bind = func(id string, run runAudit) error {
		if used[run.RunID] || run.RunID == "" {
			return fmt.Errorf("generated_graph/execution_identity")
		}
		node, ok := nodes[id]
		if !ok {
			return fmt.Errorf("generated_graph/unknown_node")
		}
		used[run.RunID] = true
		bound[id] = run
		h := histories[run.RunID]
		if h == nil {
			return fmt.Errorf("generated_graph/missing_history")
		}
		if err := checkGeneratedTerminal(node, run, h); err != nil {
			return err
		}
		startAttrs := h.Events[0].GetWorkflowExecutionStartedEventAttributes()
		if startAttrs == nil {
			return fmt.Errorf("generated_graph/missing_start")
		}
		if node.Kind == "nexus-handler" && generatedMessageDigest(startAttrs.Input, "temporal.omes.kitchen_sink.NexusHandlerInput") != node.InputSHA256 {
			return fmt.Errorf("generated_graph/nexus_handler_input")
		}
		activities, nexus := 0, 0
		childStarts := []*historypb.HistoryEvent{}
		childInitiated := map[int64]*historypb.HistoryEvent{}
		nexusSchedules := map[int64]*historypb.HistoryEvent{}
		nexusStarts := map[int64]*historypb.HistoryEvent{}
		for _, event := range h.Events {
			if event.GetActivityTaskScheduledEventAttributes() != nil {
				activities++
			}
			if event.GetNexusOperationScheduledEventAttributes() != nil {
				nexus++
				nexusSchedules[event.EventId] = event
			}
			if a := event.GetNexusOperationStartedEventAttributes(); a != nil {
				nexusStarts[a.ScheduledEventId] = event
			}
			if event.GetChildWorkflowExecutionStartedEventAttributes() != nil {
				childStarts = append(childStarts, event)
			}
			if event.GetStartChildWorkflowExecutionInitiatedEventAttributes() != nil {
				childInitiated[event.EventId] = event
			}
		}
		if activities > node.Activities || nexus > node.NexusOperations {
			return fmt.Errorf("generated_graph/effect_counts")
		}
		aPeak, e := peakGenerated(h, enums.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED)
		if e != nil {
			return e
		}
		nPeak, e := peakGenerated(h, enums.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED)
		if e != nil {
			return e
		}
		cPeak, e := peakGenerated(h, enums.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED)
		if e != nil {
			return e
		}
		if aPeak > audit.ObservedMaxFanout.Activities {
			audit.ObservedMaxFanout.Activities = aPeak
		}
		if nPeak > audit.ObservedMaxFanout.Nexus {
			audit.ObservedMaxFanout.Nexus = nPeak
		}
		if cPeak > audit.ObservedMaxFanout.Children {
			audit.ObservedMaxFanout.Children = cPeak
		}
		sort.Slice(childStarts, func(i, j int) bool { return childStarts[i].EventId < childStarts[j].EventId })
		if len(childInitiated) > len(node.Children) || len(childStarts) > len(childInitiated) {
			return fmt.Errorf("generated_graph/child_inventory")
		}
		remainingChildren := append([]string(nil), node.Children...)
		initiatedNodes := map[int64]string{}
		for eventID, event := range childInitiated {
			digest := generatedWorkflowDigest(event.GetStartChildWorkflowExecutionInitiatedEventAttributes().GetInput())
			matched := -1
			for i, childID := range remainingChildren {
				if childNode, ok := nodes[childID]; ok && childNode.InputSHA256 == digest {
					matched = i
					break
				}
			}
			if matched < 0 {
				return fmt.Errorf("generated_graph/child_input")
			}
			initiatedNodes[eventID] = remainingChildren[matched]
			remainingChildren = append(remainingChildren[:matched], remainingChildren[matched+1:]...)
		}
		for _, event := range childStarts {
			a := event.GetChildWorkflowExecutionStartedEventAttributes()
			child := byRun[a.GetWorkflowExecution().GetRunId()]
			if a.InitiatedEventId < 1 || a.InitiatedEventId > int64(len(h.Events)) || h.Events[a.InitiatedEventId-1].EventId != a.InitiatedEventId {
				return fmt.Errorf("generated_graph/child_initiated_reference")
			}
			childID := initiatedNodes[a.InitiatedEventId]
			if childID == "" {
				return fmt.Errorf("generated_graph/child_input")
			}
			if e = bind(childID, child); e != nil {
				return e
			}
		}
		async := []int64{}
		for eventID, event := range nexusSchedules {
			if event.GetNexusOperationScheduledEventAttributes().GetOperation() == "echo-async" {
				async = append(async, eventID)
			}
		}
		sort.Slice(async, func(i, j int) bool { return async[i] < async[j] })
		if len(async) != len(node.NexusHandlers) {
			return fmt.Errorf("generated_graph/nexus_handler_inventory")
		}
		for i, eventID := range async {
			start := nexusStarts[eventID]
			wid, e := generatedToken(start, namespace)
			if e != nil {
				return e
			}
			handler := byWorkflow[wid]
			if handler.RunID == "" {
				return fmt.Errorf("generated_graph/nexus_handler_identity")
			}
			linked := false
			for _, link := range start.Links {
				if w := link.GetWorkflowEvent(); w != nil && w.WorkflowId == handler.WorkflowID && w.RunId == handler.RunID {
					linked = true
				}
			}
			if !linked {
				return fmt.Errorf("generated_graph/nexus_handler_link")
			}
			if e = bind(node.NexusHandlers[i], handler); e != nil {
				return e
			}
		}
		if node.Next != "" {
			next := byRun[run.NextRun]
			if next.RunID == "" {
				return fmt.Errorf("generated_graph/omitted_continuation")
			}
			last := h.Events[len(h.Events)-1].GetWorkflowExecutionContinuedAsNewEventAttributes()
			if last == nil || generatedWorkflowDigest(last.Input) != nodes[node.Next].InputSHA256 {
				return fmt.Errorf("generated_graph/continuation_input")
			}
			if e = bind(node.Next, next); e != nil {
				return e
			}
		}
		return nil
	}
	if err := bind("root", root); err != nil {
		return audit, err
	}
	if len(used) != len(runs) {
		return audit, fmt.Errorf("generated_graph/execution_inventory")
	}
	for _, run := range runs {
		h := histories[run.RunID]
		s := h.Events[0].GetWorkflowExecutionStartedEventAttributes()
		if s.GetWorkflowType().GetName() == "NexusHandlerWorkflow" {
			audit.Observed.NexusHandlers++
		} else if s.GetContinuedExecutionRunId() != "" {
			audit.Observed.Continuations++
		} else if s.GetParentWorkflowExecution() != nil {
			audit.Observed.Children++
		} else {
			audit.Observed.Roots++
		}
		audit.Observed.Activities += run.Events[enums.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED.String()]
		audit.Observed.NexusOperations += run.Events[enums.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED.String()]
	}
	if audit.Observed.Roots != audit.Expected.Roots || audit.Observed.Children > audit.Expected.Children || audit.Observed.Continuations > audit.Expected.Continuations || audit.Observed.NexusHandlers > audit.Expected.NexusHandlers || audit.Observed.Activities > audit.Expected.Activities || audit.Observed.NexusOperations > audit.Expected.NexusOperations {
		return audit, fmt.Errorf("generated_graph/typed_counts")
	}
	if audit.ObservedMaxFanout.Children > audit.Limits.Children || audit.ObservedMaxFanout.Activities > audit.Limits.Activities || audit.ObservedMaxFanout.Nexus > audit.Limits.Nexus {
		return audit, fmt.Errorf("generated_graph/fanout_limit")
	}
	return audit, nil
}

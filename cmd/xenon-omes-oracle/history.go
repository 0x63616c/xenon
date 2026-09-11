package main

import (
	"context"
	"fmt"
	history "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/proto"
)

// readCompleteHistory is the actual API pagination path used before graph checks.
func readCompleteHistory(ctx context.Context, fetch func(context.Context, []byte) (*workflowservice.GetWorkflowExecutionHistoryResponse, error)) (*history.History, error) {
	h := &history.History{}
	seen := map[string]bool{}
	var token []byte
	size := 0
	for page := 0; page < 1000; page++ {
		call, done := bounded(ctx)
		response, e := fetch(call, token)
		done()
		if e != nil {
			return nil, e
		}
		if response == nil || response.History == nil {
			return nil, fmt.Errorf("missing history page")
		}
		for _, event := range response.History.Events {
			if event == nil {
				return nil, fmt.Errorf("nil history event")
			}
			size += proto.Size(event)
			if size > 16<<20 || len(h.Events) >= 100000 {
				return nil, fmt.Errorf("history size bound exceeded")
			}
			h.Events = append(h.Events, event)
		}
		token = response.NextPageToken
		if len(token) == 0 {
			return h, nil
		}
		if seen[string(token)] {
			return nil, fmt.Errorf("history cursor cycle")
		}
		seen[string(token)] = true
	}
	return nil, fmt.Errorf("history pagination bound exceeded")
}

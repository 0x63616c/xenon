package simulation

import (
	"bytes"
	"context"
	"testing"
)

func TestDefaultDSTCatalogRunsThousandSchedulesAcrossHundredSeeds(t *testing.T) {
	generator, err := DefaultDSTGenerator()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	familyCount := len(generator.(*dstCatalog).generators)
	families := make([]int, familyCount)
	distinct := map[string]struct{}{}
	limits := WorkloadLimits{MaxOperations: 256, MaxDepth: 1, MaxPayloadBytes: 1 << 20, Features: []string{CoupledKind}}
	for seed := uint64(1); seed <= 100; seed++ {
		for member := uint64(0); member < 10; member++ {
			index := (seed-1)*10 + member
			request := GenerateRequest{Index: index, WorkloadSeed: streamSeed("workload/v1", seed, member), FaultSeed: streamSeed("fault/v1", seed, member), Limits: limits}
			scenario, err := generator.Next(ctx, request)
			if err != nil {
				t.Fatalf("seed=%d member=%d: %v", seed, member, err)
			}
			replay, err := generator.Next(ctx, request)
			if err != nil || !bytes.Equal(scenario.Faults, replay.Faults) {
				t.Fatalf("expanded replay changed seed=%d member=%d: %v", seed, member, err)
			}
			coupled, err := decodeCoupled(scenario)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = runCoupled(ctx, coupled, "", nil); err != nil {
				t.Fatalf("seed=%d member=%d family=%d: %v", seed, member, index%uint64(familyCount), err)
			}
			families[index%uint64(familyCount)]++
			distinct[hash(scenario.Faults)] = struct{}{}
		}
	}
	for family, count := range families {
		if count == 0 {
			t.Fatalf("DST family %d did not execute", family)
		}
	}
	if len(distinct) < 100 {
		t.Fatalf("insufficient schedule diversity: %d", len(distinct))
	}
	t.Logf("schedules=1000 seeds=100 families=%v distinct=%d", families, len(distinct))
}

func TestDefaultDSTCatalogContainsDeclaredFaultFamilies(t *testing.T) {
	generator, err := DefaultDSTGenerator()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	familyCount := len(generator.(*dstCatalog).generators)
	for index := uint64(0); index < uint64(familyCount); index++ {
		scenario, err := generator.Next(context.Background(), GenerateRequest{Index: index, FaultSeed: 42})
		if err != nil {
			t.Fatal(err)
		}
		coupled, err := decodeCoupled(scenario)
		if err != nil {
			t.Fatal(err)
		}
		result, err := RunCoupled(coupled, "")
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range result.Trace {
			if entry.Result == "unknown_publication" {
				seen["lost_response_observed"] = true
			}
			if entry.Result == "storage_read_error" {
				seen["storage_error_injected"] = true
			}
			if entry.Result == "storage_error" {
				seen["storage_error_delivered"] = true
			}
		}
		for _, step := range coupled.Steps {
			if step.Fault != "" {
				seen[step.Fault] = true
			}
			if step.Action == "open" {
				seen["open_boundary"] = true
			}
			if step.Action == "publish" {
				seen["reservation_or_ready_boundary"] = true
			}
			if step.Action == "commit" {
				seen["commit_boundary"] = true
			}
			if step.Membership != nil {
				for _, member := range step.Membership.Members {
					if member.Incarnation == "inc_0000000000000000000003" && member.Address == "node-1:8080" {
						seen["same_address_new_incarnation"] = true
					}
				}
			}
		}
		moves := 0
		for _, entry := range result.Trace {
			if entry.Input.Actor != "coordinator-new" || entry.Input.Action != "publish" || (entry.Input.Effect != 7 && entry.Input.Effect != 9) || !entry.Accepted {
				continue
			}
			before, beforeErr := checkerControl(entry.Before)
			after, afterErr := checkerControl(entry.After)
			if beforeErr != nil || afterErr != nil {
				t.Fatal(beforeErr, afterErr)
			}
			if before.Partitions[coupled.RequiredPartition].Desired != after.Partitions[coupled.RequiredPartition].Desired {
				moves++
			}
		}
		if moves == 2 {
			seen["assignment_aba"] = true
		}
	}
	for _, family := range []string{"lost_publish_response", "storage_read_error", "stale_plan", "old_ready", "post_fence_commit", "open_boundary", "reservation_or_ready_boundary", "commit_boundary", "same_address_new_incarnation", "assignment_aba", "lost_response_observed", "storage_error_injected", "storage_error_delivered", "crash_before_commit", "response_lost_after_commit", "drop_then_retry", "duplicate_delivery", "route_refresh", "closed_admission", "overlapping_join", "partition_before_reservation", "partition_after_reservation", "partition_before_open", "partition_after_open", "partition_before_ready", "partition_after_ready"} {
		if !seen[family] {
			t.Errorf("missing declared DST family %s", family)
		}
	}
}

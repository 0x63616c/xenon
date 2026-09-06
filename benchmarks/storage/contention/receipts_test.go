package main

import (
	"github.com/0x63616c/xenon/internal/registry"
	"testing"
)

func TestReceiptReconciliation(t *testing.T) {
	old := Receipt{"trn_old", "old-intent"}
	attempted := Receipt{"trn_attempt", "new-intent"}
	for _, tc := range []struct {
		name, version string
		receipt       Receipt
		want          string
		invalid       bool
	}{
		{"retained across unrelated update", "new", attempted, "published", false},
		{"original version exact retry", "old", old, "retry-exact", false},
		{"preserved receipt proves nonpublication", "new", old, "not-published", false},
		{"changed digest", "new", Receipt{attempted.Transition, "different"}, "", true},
		{"actor overwritten while pending", "new", Receipt{"trn_other", "other"}, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, e := classify("old", registry.Version(tc.version), old, attempted, tc.receipt)
			if got != tc.want || (e != nil) != tc.invalid {
				t.Fatalf("got %q,%v want %q invalid=%v", got, e, tc.want, tc.invalid)
			}
		})
	}
}

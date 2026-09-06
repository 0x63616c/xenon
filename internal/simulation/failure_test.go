package simulation

import (
	"errors"
	"fmt"
	"testing"
)

func TestFailureFingerprintPreservesMechanismNotDiagnostics(t *testing.T) {
	f := FailureFingerprint{"acknowledged_state", "missing_after_recovery"}
	a, err := NewInvariantFailure(f, errors.New("node A at 12:00"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewInvariantFailure(f, errors.New("node B at 13:00"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range []error{a, b, fmt.Errorf("case 4: %w", a)} {
		got, ok := FingerprintOf(e)
		if !ok || got != f {
			t.Fatalf("fingerprint %v %v", got, ok)
		}
	}
	other, _ := NewInvariantFailure(FailureFingerprint{"acknowledged_state", "wrong_value"}, nil)
	if _, ok := FingerprintOf(errors.Join(a, other)); ok {
		t.Fatal("ambiguous failure accepted")
	}
	if _, ok := FingerprintOf(errors.Join(a, errors.New("cleanup failed"))); ok {
		t.Fatal("secondary failure accepted as clean fingerprint")
	}
	if _, ok := FingerprintOf(errors.New(a.Error())); ok {
		t.Fatal("message parsed as identity")
	}
	if _, ok := FingerprintOf(&InvariantFailure{Fingerprint: FailureFingerprint{"bad name", "x"}}); ok {
		t.Fatal("invalid fingerprint accepted")
	}
	if _, err := NewInvariantFailure(FailureFingerprint{"x", ""}, nil); err == nil {
		t.Fatal("missing mechanism accepted")
	}
}

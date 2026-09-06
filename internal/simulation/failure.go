package simulation

import (
	"errors"
	"fmt"
	"regexp"
)

// FailureFingerprint identifies an assertion and its failure mechanism. Producers
// assign stable names; timestamps, addresses and error messages are not identity.
type FailureFingerprint struct {
	Invariant string `json:"invariant"`
	Mechanism string `json:"mechanism"`
}

var failureName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

func (f FailureFingerprint) Validate() error {
	if !failureName.MatchString(f.Invariant) || !failureName.MatchString(f.Mechanism) {
		return errors.New("failure fingerprint requires stable invariant and mechanism names")
	}
	return nil
}

// InvariantFailure preserves diagnostic context separately from reduction identity.
// Construct through NewInvariantFailure; invalid fingerprints cannot match replay.
type InvariantFailure struct {
	Fingerprint FailureFingerprint
	Cause       error
}

func NewInvariantFailure(f FailureFingerprint, cause error) (*InvariantFailure, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}
	return &InvariantFailure{Fingerprint: f, Cause: cause}, nil
}
func (e *InvariantFailure) Error() string {
	if e == nil {
		return "nil invariant failure"
	}
	if e.Cause == nil {
		return e.Fingerprint.Invariant + ":" + e.Fingerprint.Mechanism
	}
	return fmt.Sprintf("%s:%s: %v", e.Fingerprint.Invariant, e.Fingerprint.Mechanism, e.Cause)
}
func (e *InvariantFailure) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// FingerprintOf rejects ambiguous joined failures. A primary failure must be
// selected before secondary errors are joined; a reducer must not accept any
// matching error buried among unrelated failures.
func FingerprintOf(err error) (FailureFingerprint, bool) {
	var found FailureFingerprint
	valid := false
	ambiguous := false
	var walk func(error)
	walk = func(e error) {
		if e == nil {
			return
		}
		if v, ok := e.(*InvariantFailure); ok {
			if v == nil || v.Fingerprint.Validate() != nil {
				ambiguous = true
				return
			}
			if valid && found != v.Fingerprint {
				ambiguous = true
			}
			found = v.Fingerprint
			valid = true
			// Cause is diagnostic context, not another independently selected assertion.
			return
		}
		switch v := e.(type) {
		case interface{ Unwrap() []error }:
			children := v.Unwrap()
			count := 0
			for _, child := range children {
				if child != nil {
					count++
				}
				walk(child)
			}
			if count > 1 {
				ambiguous = true
			}
		case interface{ Unwrap() error }:
			walk(v.Unwrap())
		}
	}
	walk(err)
	return found, valid && !ambiguous
}

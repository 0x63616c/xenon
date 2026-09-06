package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"time"

	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/registry"
	regS3 "github.com/0x63616c/xenon/internal/registry/s3"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3"
)

// This is protocol-specific. Every actor has one pending attempt and every
// publication preserves all OTHER actor receipts; generic Store lacks this rule.
func classify(expected, observed registry.Version, original, attempted, actual Receipt) (string, error) {
	if actual.Transition == attempted.Transition {
		if actual.IntentDigest != attempted.IntentDigest {
			return "", errors.New("receipt transition digest mismatch")
		}
		return "published", nil
	}
	if actual != original {
		return "", errors.New("actor receipt changed while its sole attempt was unresolved")
	}
	if observed == expected {
		return "retry-exact", nil
	}
	// A committed receipt cannot disappear: this actor has not made a later
	// write, other actors preserve it, and the original CAS cannot now publish.
	return "not-published", nil
}
func publish(ctx context.Context, s registry.Store, key registry.Key, expected registry.Version, w registry.Write, worker int, original, attempted Receipt) (registry.Record, []string, error) {
	var notes []string
	r, e := s.Replace(ctx, key, expected, w)
	var unknown *registry.UnknownOutcome
	if !errors.As(e, &unknown) {
		return r, notes, e
	}
	notes = append(notes, "unknown: "+detail(e))
	for {
		if ctx.Err() != nil {
			return r, notes, errors.Join(e, ctx.Err())
		}
		observed, c, readErr := read(ctx, s, key)
		if readErr != nil {
			notes = append(notes, "read-pending: "+detail(readErr))
			var unavailable *registry.Unavailable
			if !errors.As(readErr, &unavailable) {
				return r, notes, readErr
			}
		} else {
			decision, err := classify(expected, observed.Version, original, attempted, c.Receipts[worker])
			notes = append(notes, fmt.Sprintf("%s version=%s receipt=%s digest=%s", decision, observed.Version, c.Receipts[worker].Transition, c.Receipts[worker].IntentDigest))
			if err != nil {
				return r, notes, err
			}
			switch decision {
			case "published":
				return observed, notes, nil
			case "not-published":
				return registry.Record{}, notes, &registry.Conflict{Key: key}
			case "retry-exact":
				r, retryErr := s.Replace(ctx, key, expected, w)
				if retryErr == nil {
					return r, notes, nil
				}
				notes = append(notes, "exact-retry: "+detail(retryErr))
				// A retry conflict does not erase the earlier unknown. Reconcile receipts.
			}
		}
		if err := pause(ctx, time.Millisecond); err != nil {
			return r, notes, errors.Join(e, err)
		}
	}
}

type injectedTransport struct {
	base      http.RoundTripper
	before    bool
	armed     bool
	intervene func() error
}

func (t *injectedTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method != "PUT" || !t.armed {
		return t.base.RoundTrip(r)
	}
	t.armed = false
	if !t.before {
		response, e := t.base.RoundTrip(r)
		if e != nil {
			return response, e
		}
		if response.StatusCode != 200 {
			return response, nil
		}
		io.Copy(io.Discard, response.Body)
		response.Body.Close()
	}
	if e := t.intervene(); e != nil {
		return nil, e
	}
	return nil, io.ErrUnexpectedEOF
}

// Real MinIO cases force an unrelated publication before registry readback.
// The third deliberately violates preservation; the independent state oracle
// must detect it rather than trusting classify's protocol assumption.
func receiptCases(c config, out string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	makeClient := func(t http.RoundTripper) *sdk.Client {
		return sdk.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("xenon-local", "xenon-local-test-only", ""), HTTPClient: &http.Client{Transport: t}, RetryMaxAttempts: 1}, func(o *sdk.Options) { o.BaseEndpoint = aws.String(c.Endpoint); o.UsePathStyle = true })
	}
	f, e := os.Create(out + "/receipt-cases.jsonl")
	if e != nil {
		return e
	}
	defer f.Close()
	encoder := json.NewEncoder(f)
	for _, mode := range []string{"commit-lost-response", "not-committed-lost-response", "negative-drop-other-receipt"} {
		base := http.DefaultTransport.(*http.Transport).Clone()
		clean, e := regS3.New(makeClient(base), c.Bucket, "receipts/"+mode)
		if e != nil {
			return e
		}
		fault := &injectedTransport{base: base, before: mode == "not-committed-lost-response"}
		store, e := regS3.New(makeClient(fault), c.Bucket, "receipts/"+mode)
		if e != nil {
			return e
		}
		key := registry.Key("control")
		initial := fixture(2, shapeConfig{2, c.Base})
		w, e := write(key, "", 1, initial)
		if e != nil {
			return e
		}
		r, e := clean.Create(ctx, key, w)
		if e != nil {
			return e
		}
		original := initial.Receipts[0]
		attempted := Receipt{identity.TransitionID("trn_0000000000000000000002"), fmt.Sprintf("%064d", 2)}
		initial.Coordinator.RenewalSequence++
		initial.Receipts[0] = attempted
		w, e = write(key, r.Version, 2, initial)
		if e != nil {
			return e
		}
		fault.intervene = func() error {
			r, body, e := read(ctx, clean, key)
			if e != nil {
				return e
			}
			id := identity.PartitionID("prt_0000000000000000000001")
			p := body.Partitions[id]
			p.Ready.Generation++
			body.Partitions[id] = p
			body.Receipts[1] = Receipt{"trn_0000000000000000000003", fmt.Sprintf("%064d", 3)}
			if mode == "negative-drop-other-receipt" {
				body.Receipts[0] = original
			}
			w, e := write(key, r.Version, 3, body)
			if e != nil {
				return e
			}
			_, e = clean.Replace(ctx, key, r.Version, w)
			return e
		}
		fault.armed = true
		_, notes, publishErr := publish(ctx, store, key, r.Version, w, 0, original, attempted)
		var conflict *registry.Conflict
		_, actual, e := read(ctx, clean, key)
		if e != nil {
			return e
		}
		want := fixture(2, shapeConfig{2, c.Base})
		id := identity.PartitionID("prt_0000000000000000000001")
		p := want.Partitions[id]
		p.Ready.Generation++
		want.Partitions[id] = p
		want.Receipts[1] = Receipt{"trn_0000000000000000000003", fmt.Sprintf("%064d", 3)}
		if mode == "commit-lost-response" {
			want.Coordinator.RenewalSequence++
			want.Receipts[0] = attempted
			if publishErr != nil {
				return fmt.Errorf("committed receipt not reconciled: %w", publishErr)
			}
		} else if !errors.As(publishErr, &conflict) {
			return fmt.Errorf("expected proven absence under receipt preservation: %v", publishErr)
		}
		matched := reflect.DeepEqual(actual, want)
		if matched == (mode == "negative-drop-other-receipt") {
			return fmt.Errorf("receipt oracle failed mode=%s matched=%v", mode, matched)
		}
		if e := encoder.Encode(map[string]any{"mode": mode, "reconciliation": notes, "observed_error": detail(publishErr), "whole_state_matches_expected": matched, "negative_control_detected": mode == "negative-drop-other-receipt", "passed": true}); e != nil {
			return e
		}
		base.CloseIdleConnections()
	}
	return nil
}

package slatedb

import (
	"context"
	"errors"
	"slices"
	"testing"

	p "github.com/0x63616c/xenon/internal/partitions"
)

// Actual native iterators qualify range/order semantics; these are not a model
// of the native scan. The durable barrier key must never count toward More.
func TestNativeScanBoundsAndOrder(t *testing.T) {
	_, open := setup(t)
	w := open("scan", false)
	tx := begin(t, w)
	for _, key := range []string{"a", "b", "b\x00", "c", "d", "e"} {
		stage(t, tx, key, key)
	}
	finish(t, w, tx)
	tx = begin(t, w)
	defer tx.Abort()
	b := func(s string) []byte { return []byte(s) }
	for _, tc := range []struct {
		name string
		req  p.ScanRequest
		keys []string
		more bool
	}{
		{"ascending", p.ScanRequest{Limit: 2}, []string{"a", "b"}, true},
		{"descending", p.ScanRequest{Reverse: true, Limit: 2}, []string{"e", "d"}, true},
		{"defaults", p.ScanRequest{Start: b("b"), End: b("d"), Limit: 3}, []string{"b", "b\x00", "c"}, false},
		{"reverse defaults", p.ScanRequest{Start: b("b"), End: b("d"), Reverse: true, Limit: 3}, []string{"c", "b\x00", "b"}, false},
		{"exclusive cursor", p.ScanRequest{Start: b("b"), StartExclusive: true, Limit: 2}, []string{"b\x00", "c"}, true},
		{"reverse cursor", p.ScanRequest{End: b("c"), Reverse: true, Limit: 2}, []string{"b\x00", "b"}, true},
		{"exclusive lower reverse", p.ScanRequest{Start: b("b"), StartExclusive: true, End: b("d"), Reverse: true, Limit: 2}, []string{"c", "b\x00"}, false},
		{"inclusive upper", p.ScanRequest{Start: b("c"), End: b("d"), EndInclusive: true, Limit: 2}, []string{"c", "d"}, false},
		{"inclusive upper reverse", p.ScanRequest{Start: b("c"), End: b("d"), EndInclusive: true, Reverse: true, Limit: 1}, []string{"d"}, true},
		{"singleton", p.ScanRequest{Start: b("b"), End: b("b"), EndInclusive: true, Limit: 1}, []string{"b"}, false},
		{"singleton reverse", p.ScanRequest{Start: b("b"), End: b("b"), EndInclusive: true, Reverse: true, Limit: 1}, []string{"b"}, false},
		{"equal empty", p.ScanRequest{Start: b("b"), End: b("b"), Limit: 1}, nil, false},
		{"exclusive equal empty", p.ScanRequest{Start: b("b"), StartExclusive: true, End: b("b"), EndInclusive: true, Reverse: true, Limit: 1}, nil, false},
		{"empty gap", p.ScanRequest{Start: b("c1"), End: b("c2"), Reverse: true, Limit: 1}, nil, false},
		{"empty upper bound", p.ScanRequest{End: []byte{}, Limit: 1}, nil, false},
		{"reserved key only", p.ScanRequest{End: b("a"), Reverse: true, Limit: 1}, nil, false},
		{"last page skips barrier", p.ScanRequest{End: b("b"), Reverse: true, Limit: 1}, []string{"a"}, false},
		{"full exact page", p.ScanRequest{Limit: 6}, []string{"a", "b", "b\x00", "c", "d", "e"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := tx.Scan(context.Background(), tc.req)
			if err != nil {
				t.Fatal(err)
			}
			keys := []string{}
			for _, entry := range result.Entries {
				keys = append(keys, string(entry.Key))
				if string(entry.Value) != string(entry.Key) {
					t.Fatal("key/value mismatch", entry)
				}
			}
			if !slices.Equal(keys, tc.keys) || result.More != tc.more {
				t.Fatalf("keys=%q More=%v; want %q More=%v", keys, result.More, tc.keys, tc.more)
			}
		})
	}
	for _, req := range []p.ScanRequest{{Limit: 0}, {Limit: 100001}, {Start: b("d"), End: b("b"), Limit: 1}, {Start: b("d"), End: b("b"), Reverse: true, Limit: 1}} {
		if _, err := tx.Scan(context.Background(), req); !errors.Is(err, p.ErrInvalid) {
			t.Fatal("invalid scan accepted", req, err)
		}
	}
}

func TestNativeScanPagesRetainTransactionView(t *testing.T) {
	_, open := setup(t)
	w := open("pages", false)
	tx := begin(t, w)
	for _, key := range []string{"a", "b", "c", "d"} {
		stage(t, tx, key, key)
	}
	finish(t, w, tx)
	tx = begin(t, w)
	defer tx.Abort()
	// Transaction scans must retain read-your-writes, including tombstones.
	stage(t, tx, "b\x00", "staged")
	if err := tx.Delete([]byte("c")); err != nil {
		t.Fatal(err)
	}
	for _, reverse := range []bool{false, true} {
		req := p.ScanRequest{Start: []byte("a"), End: []byte("e"), Reverse: reverse, Limit: 2}
		keys := []string{}
		for page := 0; ; page++ {
			if page > 2 {
				t.Fatal("pagination failed to terminate")
			}
			result, err := tx.Scan(context.Background(), req)
			if err != nil || len(result.Entries) != 2 {
				t.Fatal("scan page", result, err)
			}
			for _, entry := range result.Entries {
				keys = append(keys, string(entry.Key))
			}
			if !result.More {
				break
			}
			last := result.Entries[len(result.Entries)-1].Key
			if reverse {
				req.End, req.EndInclusive = last, false
			} else {
				req.Start, req.StartExclusive = last, true
			}
		}
		want := []string{"a", "b", "b\x00", "d"}
		if reverse {
			slices.Reverse(want)
		}
		if !slices.Equal(keys, want) {
			t.Fatalf("reverse=%v got %q want %q", reverse, keys, want)
		}
	}
}

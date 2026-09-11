//go:build slatedb

package slatedb

import (
	"context"
	"encoding/json"
	p "github.com/0x63616c/xenon/internal/partitions"
	"testing"
	"time"
)

func TestNativeConfiguredFlushSettingsAndReplacement(t *testing.T) {
	for _, ms := range []int{1, 10, 100, 1000} {
		e, err := NewWithWALFlushInterval("s3://fixture", ms)
		if err != nil {
			t.Fatal(err)
		}
		settings, err := e.settings()
		if err != nil {
			t.Fatal(err)
		}
		raw, err := settings.ToJsonString()
		settings.Destroy()
		if err != nil {
			t.Fatal(err)
		}
		var values map[string]any
		if err = json.Unmarshal([]byte(raw), &values); err != nil {
			t.Fatal(err)
		}
		d, err := time.ParseDuration(values["flush_interval"].(string))
		if err != nil || d != time.Duration(ms)*time.Millisecond {
			t.Fatal(raw, err)
		}
	}
	for _, ms := range []int{0, -1, 1001} {
		if _, err := NewWithWALFlushInterval("s3://fixture", ms); err == nil {
			t.Fatal("invalid accepted")
		}
	}
	e, _ := New("s3://fixture")
	if e.flushIntervalMS != 100 {
		t.Fatal("default changed")
	}
	e = &Engine{backend: "memory://", flushIntervalMS: 10}
	req := p.OpenRequest{Path: "configured-replacement", Partition: "prt_0000000000000000000000", AssignmentRevision: 1, Reservation: "trn_0000000000000000000000", Incarnation: "inc_0000000000000000000000", Generation: 1}
	for i := 0; i < 2; i++ {
		w, err := e.Open(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		tx := begin(t, w)
		stage(t, tx, "key", "value")
		finish(t, w, tx)
		if err = w.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

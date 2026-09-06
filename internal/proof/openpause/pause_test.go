package openpause

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/0x63616c/xenon/internal/directory"
)

func TestOpenPause(t *testing.T) {
	for _, mode := range []string{"release", "wrong", "expired"} {
		t.Run(mode, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "control")
			c, e := New(dir, "matching", "session")
			if e != nil {
				t.Fatal(e)
			}
			r := directory.Record{Partition: "matching", Node: "c", Incarnation: "boot", Generation: 1, Transition: "transition", State: "opening"}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if mode == "expired" {
				cancel()
			}
			done := make(chan error, 1)
			go func() { done <- c.Before(ctx, r) }()
			deadline := time.Now().Add(time.Second)
			for {
				if _, e := os.Stat(filepath.Join(dir, "receipt.json")); e == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("no pause")
				}
				time.Sleep(time.Millisecond)
			}
			release := Receipt{Session: "session", Record: r}
			if mode == "wrong" {
				release.Record.Incarnation = "other"
			}
			b, _ := json.Marshal(release)
			tmp := filepath.Join(dir, "release.tmp")
			if e = os.WriteFile(tmp, b, 0600); e != nil {
				t.Fatal(e)
			}
			if e = os.Rename(tmp, filepath.Join(dir, "release.json")); e != nil {
				t.Fatal(e)
			}
			e = <-done
			if mode != "release" {
				if e == nil {
					t.Fatal("invalid release accepted")
				}
				b, _ = os.ReadFile(filepath.Join(dir, "receipt.json"))
				var receipt Receipt
				json.Unmarshal(b, &receipt)
				if receipt.Events[len(receipt.Events)-1] != "FAILED-no-build" {
					t.Fatal(receipt)
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			for _, stage := range []string{"build-succeeded", "superseded-before-ready-retired"} {
				if e = c.Observe(r, stage); e != nil {
					t.Fatal(e)
				}
			}
			r.Generation++
			if e = c.Before(ctx, r); e != nil {
				t.Fatal(e)
			}
			if e = c.Observe(r, "build-succeeded"); e != nil {
				t.Fatal(e)
			}
			b, _ = os.ReadFile(filepath.Join(dir, "receipt.json"))
			var receipt Receipt
			json.Unmarshal(b, &receipt)
			if !reflect.DeepEqual(receipt.Events, []string{"paused-before-build", "released", "build-succeeded", "superseded-before-ready-retired"}) || receipt.Record.Generation != 1 {
				t.Fatal(receipt)
			}
		})
	}
}

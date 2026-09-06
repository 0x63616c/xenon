// Package openpause is an opt-in, local proof control. It supplies no authority.
package openpause

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/0x63616c/xenon/internal/directory"
)

type Receipt struct {
	Session string           `json:"session"`
	Record  directory.Record `json:"record"`
	Events  []string         `json:"events"`
}
type Control struct {
	mu                      sync.Mutex
	dir, partition, session string
	used                    bool
	receipt                 Receipt
}

func New(dir, partition, session string) (*Control, error) {
	if !filepath.IsAbs(dir) || partition == "" || session == "" {
		return nil, fmt.Errorf("invalid open pause plan")
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		return nil, err
	}
	return &Control{dir: dir, partition: partition, session: session}, nil
}
func (c *Control) event(event string) error {
	c.receipt.Events = append(c.receipt.Events, event)
	b, e := json.Marshal(c.receipt)
	if e != nil {
		return e
	}
	p := filepath.Join(c.dir, "receipt.tmp")
	if e = os.WriteFile(p, b, 0600); e != nil {
		return e
	}
	return os.Rename(p, filepath.Join(c.dir, "receipt.json"))
}
func (c *Control) Before(ctx context.Context, r directory.Record) error {
	c.mu.Lock()
	if c.used || r.Partition != c.partition {
		c.mu.Unlock()
		return nil
	}
	c.used = true
	c.receipt = Receipt{Session: c.session, Record: r}
	if e := c.event("paused-before-build"); e != nil {
		c.mu.Unlock()
		return e
	}
	c.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	fail := func(e error) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		if save := c.event("FAILED-no-build"); save != nil {
			return fmt.Errorf("%w; receipt: %v", e, save)
		}
		return e
	}
	for {
		if e := ctx.Err(); e != nil {
			return fail(e)
		}
		b, e := os.ReadFile(filepath.Join(c.dir, "release.json"))
		if e == nil {
			if e = ctx.Err(); e != nil {
				return fail(e)
			}
			var release Receipt
			if len(b) > 8192 || json.Unmarshal(b, &release) != nil || release.Session != c.session || release.Record != r || len(release.Events) != 0 {
				return fail(fmt.Errorf("release identity mismatch"))
			}
			c.mu.Lock()
			if e = ctx.Err(); e == nil {
				e = c.event("released")
			}
			c.mu.Unlock()
			if e != nil {
				return fail(e)
			}
			return nil
		}
		if !errors.Is(e, os.ErrNotExist) {
			return fail(e)
		}
		select {
		case <-ctx.Done():
			return fail(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}
func (c *Control) Observe(r directory.Record, event string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.used || c.receipt.Record != r {
		return nil
	}
	events := c.receipt.Events
	if len(events) == 0 || events[len(events)-1] == "FAILED-no-build" {
		return fmt.Errorf("terminal failed pause")
	}
	return c.event(event)
}

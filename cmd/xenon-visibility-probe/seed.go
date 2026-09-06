package main

import (
	"context"
	"fmt"
)

// seedPartitions serializes each partition while allowing independent owners to
// progress concurrently. A peer failure never cancels another admitted write.
func seedPartitions(ctx context.Context, groups map[string][]int, write func(context.Context, int) error) (map[string]int, error) {
	type result struct {
		partition string
		count     int
		err       error
	}
	results := make(chan result, len(groups))
	for partition, indexes := range groups {
		go func() {
			r := result{partition: partition}
			for _, index := range indexes {
				if r.err = ctx.Err(); r.err != nil {
					break
				}
				if r.err = write(ctx, index); r.err != nil {
					r.err = fmt.Errorf("seed partition %s record %d: %w", partition, index, r.err)
					break
				}
				r.count++
			}
			results <- r
		}()
	}
	counts := make(map[string]int, len(groups))
	var first error
	for range groups {
		r := <-results
		counts[r.partition] = r.count
		if first == nil {
			first = r.err
		}
	}
	return counts, first
}

package persistence

import (
	"bytes"
	"context"

	"github.com/0x63616c/xenon/internal/partitions"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// scanMatchingRange seeks by queue-relative bounds, preserving the legacy
// cursor format without rescanning preceding tasks on each one-row page.
func scanMatchingRange(ctx context.Context, tx ClusterTransaction, prefix string, bounds partitions.ScanRequest, visit func([]byte, []byte) (bool, error)) error {
	scan := partitions.ScanRequest{Start: append([]byte(prefix), bounds.Start...), StartExclusive: bounds.StartExclusive, Limit: 1}
	if bounds.End != nil {
		scan.End = append([]byte(prefix), bounds.End...)
		scan.EndInclusive = bounds.EndInclusive
	} else {
		scan.End = []byte(prefix)
		scan.End[len(scan.End)-1]++ // all matching domain prefixes end in '/'
	}
	for {
		rows, err := tx.Scan(ctx, scan)
		if err != nil {
			return err
		}
		for _, row := range rows.Entries {
			if !bytes.HasPrefix(row.Key, []byte(prefix)) {
				return status.Error(codes.Unavailable, "matching scan returned out-of-range key")
			}
			stop, err := visit(row.Key[len(prefix):], row.Value)
			if err != nil {
				return err
			}
			if stop {
				return nil
			}
			scan.Start = bytes.Clone(row.Key)
			scan.StartExclusive = true
		}
		if !rows.More {
			return nil
		}
		if len(rows.Entries) == 0 {
			return status.Error(codes.Unavailable, "matching scan did not advance")
		}
	}
}

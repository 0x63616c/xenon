package adapter

import (
	"context"
	"errors"
	"github.com/0x63616c/xenon/internal/rpctrace"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"time"
)

// retryOperation spends the caller's existing operation budget on transport
// Unavailable. The closure must retain one exact request identity/body/digest.
// Router hop/owner bounds remain independent. Logical outcomes are returned to
// the family unchanged and are never transport retries.
func retryOperation[T proto.Message](ctx context.Context, invoke func(context.Context) (T, error)) (T, error) {
	var zero T
	if _, ok := ctx.Deadline(); !ok {
		return zero, serviceerror.NewInvalidArgument("operation deadline required")
	}
	var firstUnknown error
	finish := func(err error) error {
		if firstUnknown != nil {
			return errors.Join(firstUnknown, err)
		}
		return err
	}
	delay := 20 * time.Millisecond
	for {
		if ctx.Err() != nil {
			return zero, finish(ctx.Err())
		}
		result, err := invoke(ctx)
		if err == nil {
			if any(result) == nil {
				return zero, finish(serviceerror.NewInternal("nil persistence RPC result"))
			}
			message := result.ProtoReflect()
			if !message.IsValid() {
				return zero, finish(serviceerror.NewInternal("nil persistence RPC result"))
			}
			field := message.Descriptor().Fields().ByName("error")
			if field == nil || field.Enum() == nil || field.Enum().Values().ByNumber(message.Get(field).Enum()) == nil {
				return zero, finish(serviceerror.NewInternal("invalid persistence RPC result error"))
			}
			// Every valid family result comes from its durable journal, including a
			// logical failure. It resolves prior ambiguity for this same operation.
			return result, nil
		}
		code := status.Code(err)
		if code == codes.Unavailable && firstUnknown == nil {
			for _, detail := range status.Convert(err).Details() {
				if info, ok := detail.(*errdetails.ErrorInfo); ok && info.Domain == "xenon.routing.v1" && info.Reason == "UNKNOWN_OUTCOME" {
					firstUnknown = serviceerror.FromStatus(status.Convert(err))
					break
				}
			}
		}
		if observationErr := rpctrace.ObservationError(ctx); observationErr != nil {
			return zero, finish(errors.Join(serviceerror.FromStatus(status.Convert(err)), observationErr))
		}
		if ctx.Err() != nil {
			return zero, finish(ctx.Err())
		}
		switch code {
		case codes.Canceled:
			return zero, finish(context.Canceled)
		case codes.DeadlineExceeded:
			return zero, finish(context.DeadlineExceeded)
		case codes.Unavailable:
		default:
			return zero, finish(serviceerror.FromStatus(status.Convert(err)))
		}
		select {
		case <-ctx.Done():
			return zero, finish(ctx.Err())
		case <-time.After(delay):
		}
		if delay < 250*time.Millisecond {
			delay *= 2
			if delay > 250*time.Millisecond {
				delay = 250 * time.Millisecond
			}
		}
	}
}

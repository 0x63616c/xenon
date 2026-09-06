package routing

import (
	"github.com/0x63616c/xenon/internal/identity"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const errorDomain = "xenon.routing.v1"

// StaleOwner reports that this destination did not execute the operation. It
// grants no ownership: the origin must resolve authoritative state again.
func StaleOwner() error { return routingError("STALE_OWNER", "destination is not the current owner") }

// UnknownOutcome reports an operation whose completion is uncertain. Only
// requests carrying a valid stable replay reference and digest may retry it.
// Callers must retain the operation's identity and durable replay contract.
func UnknownOutcome() error { return routingError("UNKNOWN_OUTCOME", "operation outcome is unknown") }

func routingError(reason, message string) error {
	s, err := status.New(codes.Unavailable, message).WithDetails(&errdetails.ErrorInfo{Domain: errorDomain, Reason: reason})
	if err != nil {
		return status.Error(codes.Internal, "cannot encode routing failure")
	}
	return s.Err()
}

func retryable(err error, request any) bool {
	s, ok := status.FromError(err)
	if !ok || s.Code() != codes.Unavailable {
		return false
	}
	for _, detail := range s.Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if !ok || info.Domain != errorDomain {
			continue
		}
		switch info.Reason {
		case "STALE_OWNER":
			return true
		case "UNKNOWN_OUTCOME":
			r, ok := request.(interface {
				GetOperationId() string
				GetCommandSha256() []byte
			})
			return ok && identity.ValidateOperationReference(r.GetOperationId()) == nil && len(r.GetCommandSha256()) == 32
		}
	}
	return false
}

// A later failed routing attempt cannot resolve an earlier unknown operation.
func isUnknownRouting(err error) bool {
	s, ok := status.FromError(err)
	if !ok {
		return false
	}
	for _, detail := range s.Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok && info.Domain == errorDomain && info.Reason == "UNKNOWN_OUTCOME" {
			return true
		}
	}
	return false
}

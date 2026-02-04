package log_v1

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// interface compliance check
var _ error = (*ErrOffsetOutOfRange)(nil)

type ErrOffsetOutOfRange struct {
	Offset uint64
}

func (e ErrOffsetOutOfRange) GRPCStatus() *status.Status {
	msg := fmt.Sprintf("offset out of range: offset %d", e.Offset)

	st := status.New(
		codes.NotFound,
		msg,
	)

	d := &errdetails.LocalizedMessage{
		Locale:  "en-US",
		Message: msg,
	}
	statusWithDetails, err := st.WithDetails(d)

	if err != nil {
		return st
	}

	return statusWithDetails
}

func (e ErrOffsetOutOfRange) Error() string {
	// not-nil safety: the status code of `ErrOffsetOutOfRange` is never OK
	return e.GRPCStatus().Err().Error()
}

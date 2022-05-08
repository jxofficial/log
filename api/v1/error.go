package log_v1

import (
	"fmt"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ErrOffsetOutOfRange struct {
	Offset uint64
}

func (e ErrOffsetOutOfRange) GRPCStatus() *status.Status {
	st := status.New(codes.NotFound, fmt.Sprintf("offset out of range: %d", e.Offset))
	userFriendlyMessage := fmt.Sprintf(
		"The requested offset is outside the log's range: %d",
		e.Offset,
	)
	d := &errdetails.LocalizedMessage{
		Locale:  "en-SG",
		Message: userFriendlyMessage,
	}
	statusWithDetails, err := st.WithDetails(d)
	if err != nil {
		return st
	}
	return statusWithDetails
}

func (e ErrOffsetOutOfRange) Error() string {
	return "error offset out of range"
}

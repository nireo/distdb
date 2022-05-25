package store_v1

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

type ErrKeyNotFound struct {
	Key []byte
}

func (e ErrKeyNotFound) GRPCStatus() *status.Status {
	st := status.New(
		404,
		fmt.Sprintf("key is not found %s", string(e.Key)),
	)

	msg := fmt.Sprintf("The requested key is not stored in the database: %s", string(e.Key))
	d := &errdetails.LocalizedMessage{
		Locale:  "en-US",
		Message: msg,
	}
	std, err := st.WithDetails(d)
	if err != nil {
		return st
	}

	return std
}

func (e ErrKeyNotFound) Error() string {
	return e.GRPCStatus().Err().Error()
}

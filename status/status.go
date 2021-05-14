// Package status implements errors returned by AMQP RPC.  These errors are
// serialized and transmitted on the wire between server and client, and allow
// for additional data to be transmitted via the Details field in the status
// proto.  AMQP RPC service handlers should return an error created by this
// package, and AMQP RPC clients should expect a corresponding error to be
// returned from the RPC call.
//
// This package upholds the invariants that a non-nil error may not
// contain an OK code, and an OK code must result in a nil error.
package status

import (
	"context"
	"fmt"

	spb "google.golang.org/genproto/googleapis/rpc/status"

	"github.com/levakin/amqp-rpc/v0/codes"
	"github.com/levakin/amqp-rpc/v0/internal/status"
)

// Status references github.com/levakin/amqp-rpc/internal/status. It represents an
// RPC status code, message, and details.  It is immutable and should be
// created with New, Newf, or FromProto.
// https://godoc.org/github.com/levakin/amqp-rpc/internal/status
type Status = status.Status

// New returns a Status representing c and msg.
func New(c codes.Code, msg string) *Status {
	return status.New(c, msg)
}

// Newf returns New(c, fmt.Sprintf(format, a...)).
func Newf(c codes.Code, format string, a ...interface{}) *Status {
	return New(c, fmt.Sprintf(format, a...))
}

// Error returns an error representing c and msg.  If c is OK, returns nil.
func Error(c codes.Code, msg string) error {
	return New(c, msg).Err()
}

// Errorf returns Error(c, fmt.Sprintf(format, a...)).
func Errorf(c codes.Code, format string, a ...interface{}) error {
	return Error(c, fmt.Sprintf(format, a...))
}

// ErrorProto returns an error representing s.  If s.Code is OK, returns nil.
func ErrorProto(s *spb.Status) error {
	return FromProto(s).Err()
}

// FromProto returns a Status representing s.
func FromProto(s *spb.Status) *Status {
	return status.FromProto(s)
}

// FromError returns a Status representing err if it was produced by this
// package or has a method `RPCStatus() *Status`.
// If err is nil, a Status is returned with codes.OK and no message.
// Otherwise, ok is false and a Status is returned with codes.Unknown and
// the original error message.
func FromError(err error) (s *Status, ok bool) {
	if err == nil {
		return nil, true
	}
	if se, ok := err.(interface {
		RPCStatus() *Status
	}); ok {
		return se.RPCStatus(), true
	}
	return New(codes.Unknown, err.Error()), false
}

// Convert is a convenience function which removes the need to handle the
// boolean return value from FromError.
func Convert(err error) *Status {
	s, _ := FromError(err)
	return s
}

// Code returns the Code of the error if it is a Status error, codes.OK if err
// is nil, or codes.Unknown otherwise.
func Code(err error) codes.Code {
	// Don't use FromError to avoid allocation of OK status.
	if err == nil {
		return codes.OK
	}
	if se, ok := err.(interface {
		RPCStatus() *Status
	}); ok {
		return se.RPCStatus().Code()
	}
	return codes.Unknown
}

// FromContextError converts a context error into a Status.  It returns a
// Status with codes.OK if err is nil, or a Status with codes.Unknown if err is
// non-nil and not a context error.
func FromContextError(err error) *Status {
	switch err {
	case nil:
		return nil
	case context.DeadlineExceeded:
		return New(codes.DeadlineExceeded, err.Error())
	case context.Canceled:
		return New(codes.Canceled, err.Error())
	default:
		return New(codes.Unknown, err.Error())
	}
}
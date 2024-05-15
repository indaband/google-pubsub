package google_pubsub

import (
	"errors"

	"google.golang.org/grpc/codes"
)

var (
	ErrValidation = errors.New("validation error")
)

// PubsubError is the struct which implements error
type (
	PubsubError struct {
		retry      bool
		innerError error
	}

	ErrCodeAlias codes.Code
)

// Retriable check if should retry or not
func (p PubsubError) Retriable() bool { return p.retry }

// Retry specify that this erro should be retriable
func (p PubsubError) Retry() PubsubError {
	p.retry = true
	return p
}

// Error implements Error interface
func (p PubsubError) Error() string { return p.innerError.Error() }

// Is implements interface to enable to compare error with given type
func (p PubsubError) Is(target error) bool {
	if _, ok := target.(PubsubError); ok {
		return ok
	}
	return false
}

// NewPubsubError retrieve instance of error type
func NewPubsubError(err error) PubsubError {
	return PubsubError{innerError: err, retry: false}
}

func (ec ErrCodeAlias) IsDeadLineOrCanceledError() bool {
	return ec == ErrCodeAlias(codes.DeadlineExceeded) ||
		ec == ErrCodeAlias(codes.Canceled)
}

func (ec ErrCodeAlias) AlreadyExists() bool {
	return ec == ErrCodeAlias(codes.AlreadyExists)
}

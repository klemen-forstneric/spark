package spark

import "errors"

var (
	// ErrHandlerNotFound is returned by Dispatch when no handler is registered
	// for the command's runtime type.
	ErrHandlerNotFound = errors.New("spark: no handler registered for command")

	// ErrAlreadyRegistered is returned by Register when a handler is already
	// registered for a given command type.
	ErrAlreadyRegistered = errors.New("spark: handler already registered for command")

	// ErrInvalidHandler is returned by Register when the supplied value does
	// not expose any method matching the handler signature.
	ErrInvalidHandler = errors.New("spark: value has no methods matching func(context.Context, Command) (R, error)")
)

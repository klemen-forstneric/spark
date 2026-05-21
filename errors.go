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
	ErrInvalidHandler = errors.New("spark: invalid handler")

	// ErrStale signals that a command operated on a stale view of its entity —
	// the underlying state moved between load and save, detected by the
	// repository or event store as a version/ETag/CAS mismatch.
	//
	// spark itself never returns this; it is the contract between a handler
	// (or the companion event/aggregate library) and middleware that wants to
	// react to the conflict — e.g. retrying the command against a freshly
	// loaded entity. Producers wrap it (fmt.Errorf("%w: ...", spark.ErrStale, ...));
	// consumers detect it with errors.Is.
	ErrStale = errors.New("spark: stale entity")
)

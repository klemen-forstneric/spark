package spark

// Command is the marker interface implemented by every command. A command
// declares a stable type identifier used for observability and runtime
// lookup.
//
// Concrete command types also bind a result type by embedding Result[R].
// See Result for the typed-dispatch contract.
type Command interface {
	Type() string
}

// Result binds a command struct to its result type R. Embed it as a
// zero-size field; it adds no bytes to the command and exposes the unexported
// marker that Dispatch and Register use for type inference.
//
//	type CreateUser struct {
//	    spark.Result[*User]
//	    Email string
//	}
//	func (CreateUser) Type() string { return "user.create" }
//
// For commands with no meaningful return, use Result[Empty].
type Result[R any] struct{}

func (Result[R]) resultType() R {
	var zero R
	return zero
}

// Empty is the result type for commands that do not return a value.
type Empty = struct{}

// typedCommand is the internal constraint binding a command to its result
// type R. It is unexported on purpose — the only way a user type satisfies
// it is by embedding Result[R], which prevents accidental conformance.
type typedCommand[R any] interface {
	Command
	resultType() R
}

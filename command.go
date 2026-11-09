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
// marker that Bus.Dispatch uses to infer the result type.
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

// ResultCommand is a command whose result type is R. A type satisfies it only
// by embedding Result[R], which prevents accidental conformance. Declare a
// variable as ResultCommand[R] to hold any of several commands that share a
// result type; Bus.Dispatch still infers R from it.
//
//	var cmd spark.ResultCommand[*Order]
//	switch ... { case ...: cmd = &CompleteOrder{...}; case ...: cmd = &FailOrder{...} }
//	order, err := bus.Dispatch(ctx, cmd)
type ResultCommand[R any] interface {
	Command
	resultType() R
}

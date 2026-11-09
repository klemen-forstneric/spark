package spark

import (
	"context"
	"fmt"
)

// Dispatcher routes a command to its handler and returns the handler's result
// untyped. Router implements it; tests mock it to drive a Bus without handlers.
type Dispatcher interface {
	Dispatch(ctx context.Context, cmd Command) (any, error)
}

// Bus sends commands through a Dispatcher and returns typed results.
type Bus struct {
	dispatcher Dispatcher
}

func NewBus(d Dispatcher) *Bus {
	return &Bus{dispatcher: d}
}

// Dispatch sends cmd and returns its result as the R bound by the command's
// embedded Result[R].
func (b *Bus) Dispatch[R any](ctx context.Context, cmd ResultCommand[R]) (R, error) {
	var zero R
	out, err := b.dispatcher.Dispatch(ctx, cmd)
	if err != nil || out == nil {
		return zero, err
	}
	r, ok := out.(R)
	if !ok {
		return zero, fmt.Errorf("%w: %s returned %T, want %T", ErrResultType, cmd.Type(), out, zero)
	}
	return r, nil
}

// DispatchUntyped sends cmd and returns its result unchecked. Use it when the
// result type is not known statically; prefer Dispatch otherwise.
func (b *Bus) DispatchUntyped(ctx context.Context, cmd Command) (any, error) {
	return b.dispatcher.Dispatch(ctx, cmd)
}

// Notify sends cmd and discards its result.
func (b *Bus) Notify(ctx context.Context, cmd Command) error {
	_, err := b.dispatcher.Dispatch(ctx, cmd)
	return err
}

package spark

import (
	"context"
	"fmt"
	"reflect"
	"sync"
)

// Bus is an in-memory command bus. The zero value is not usable; construct
// one with NewBus.
type Bus struct {
	mu         sync.RWMutex
	handlers   map[reflect.Type]Next
	middleware []Middleware
}

// NewBus constructs a Bus. Middleware passed via WithMiddleware is fixed for
// the lifetime of the bus; subsequent Register calls bake the same chain
// around each handler.
func NewBus(opts ...Option) *Bus {
	b := &Bus{handlers: map[reflect.Type]Next{}}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

var (
	ctxType     = reflect.TypeOf((*context.Context)(nil)).Elem()
	errType     = reflect.TypeOf((*error)(nil)).Elem()
	commandType = reflect.TypeOf((*Command)(nil)).Elem()
)

// Register installs handlers on the bus. h may be either:
//
//   - A function value with signature func(context.Context, C) (R, error)
//     where C implements Command — registered as the sole handler for C.
//
//   - A struct (or pointer to one); each exported method matching the same
//     signature is registered as a handler for its command type. Method
//     names are not significant — the structural signature is the contract.
//
// Returns ErrInvalidHandler if h is nil or no method/signature matches.
// Returns ErrAlreadyRegistered if h contains two methods handling the same
// command type, or if any matched command type is already registered on
// the bus. On any error no partial registration takes effect.
//
// Pass a pointer (Register(&svc)) to pick up both value- and
// pointer-receiver methods.
//
// Commands may be value or pointer types: handlers are keyed by whatever
// type the handler declares as its command argument. If a handler takes
// *CreateUser, callers must Dispatch &CreateUser{...} — and vice versa.
// CreateUser and *CreateUser are independent registrations.
func (b *Bus) Register(h any) error {
	if h == nil {
		return fmt.Errorf("%w: nil handler", ErrInvalidHandler)
	}
	hv := reflect.ValueOf(h)

	switch hv.Kind() {
	case reflect.Func:
		return b.registerFunc(hv)
	case reflect.Ptr:
		if hv.IsNil() {
			return fmt.Errorf("%w: nil pointer to %s", ErrInvalidHandler, hv.Type().Elem())
		}
	}
	return b.registerMethods(hv)
}

func (b *Bus) registerFunc(fn reflect.Value) error {
	if fn.IsNil() {
		return fmt.Errorf("%w: nil func", ErrInvalidHandler)
	}
	cmdT, ok := matchesHandlerSig(fn.Type())
	if !ok {
		return fmt.Errorf("%w: %s does not match func(context.Context, Command) (R, error)",
			ErrInvalidHandler, fn.Type())
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.handlers[cmdT]; exists {
		return fmt.Errorf("%w: %s", ErrAlreadyRegistered, cmdT)
	}
	b.handlers[cmdT] = b.wrapReflected(fn)
	return nil
}

func (b *Bus) registerMethods(hv reflect.Value) error {
	type pending struct {
		cmdType reflect.Type
		method  reflect.Value
	}

	ht := hv.Type()
	var matched []pending
	seen := map[reflect.Type]bool{}

	for i := 0; i < ht.NumMethod(); i++ {
		mv := hv.Method(i)
		cmdT, ok := matchesHandlerSig(mv.Type())
		if !ok {
			continue
		}
		if seen[cmdT] {
			return fmt.Errorf("%w: %s has two methods handling %s",
				ErrAlreadyRegistered, ht, cmdT)
		}
		seen[cmdT] = true
		matched = append(matched, pending{cmdType: cmdT, method: mv})
	}

	if len(matched) == 0 {
		return fmt.Errorf("%w: %s", ErrInvalidHandler, ht)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	for _, p := range matched {
		if _, exists := b.handlers[p.cmdType]; exists {
			return fmt.Errorf("%w: %s", ErrAlreadyRegistered, p.cmdType)
		}
	}
	for _, p := range matched {
		b.handlers[p.cmdType] = b.wrapReflected(p.method)
	}
	return nil
}

// matchesHandlerSig reports whether mt is the signature
// func(context.Context, C) (R, error) for some C implementing Command, and
// returns C if so.
func matchesHandlerSig(mt reflect.Type) (reflect.Type, bool) {
	if mt.NumIn() != 2 || mt.NumOut() != 2 {
		return nil, false
	}
	if mt.In(0) != ctxType {
		return nil, false
	}
	if !mt.In(1).Implements(commandType) {
		return nil, false
	}
	if mt.Out(1) != errType {
		return nil, false
	}
	return mt.In(1), true
}

// Dispatch sends cmd to its registered handler and returns the handler's
// result as an opaque any. The caller asserts to the concrete result type.
//
//	raw, err := bus.Dispatch(ctx, CreateUser{Email: "..."})
//	if err != nil { return err }
//	user := raw.(*User)
//
// Returns ErrHandlerNotFound if no handler is registered for cmd's runtime
// type.
func (b *Bus) Dispatch(ctx context.Context, cmd Command) (any, error) {
	cmdType := reflect.TypeOf(cmd)

	b.mu.RLock()
	fn, ok := b.handlers[cmdType]
	b.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrHandlerNotFound, cmdType)
	}
	return fn(ctx, cmd)
}

// wrapReflected builds the dispatch closure for a handler (a method value
// or a func value), with the bus's middleware chain baked in
// (outermost first).
func (b *Bus) wrapReflected(callable reflect.Value) Next {
	inner := Next(func(ctx context.Context, cmd Command) (any, error) {
		out := callable.Call([]reflect.Value{
			reflect.ValueOf(ctx),
			reflect.ValueOf(cmd),
		})
		var err error
		if !out[1].IsNil() {
			err = out[1].Interface().(error)
		}
		return out[0].Interface(), err
	})
	return b.applyMiddleware(inner)
}

func (b *Bus) applyMiddleware(inner Next) Next {
	next := inner
	for i := len(b.middleware) - 1; i >= 0; i-- {
		next = b.middleware[i](next)
	}
	return next
}

package spark

import (
	"context"
	"fmt"
	"reflect"
	"sync"
)

// Router is an in-memory command router. The zero value is not usable;
// construct one with NewRouter.
type Router struct {
	mu         sync.RWMutex
	handlers   map[reflect.Type]Next
	middleware []Middleware
}

// NewRouter constructs a Router. Middleware passed via WithMiddleware is fixed
// for the lifetime of the router; subsequent Register calls bake the same
// chain around each handler.
func NewRouter(opts ...Option) *Router {
	r := &Router{handlers: map[reflect.Type]Next{}}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

var (
	ctxType     = reflect.TypeOf((*context.Context)(nil)).Elem()
	errType     = reflect.TypeOf((*error)(nil)).Elem()
	commandType = reflect.TypeOf((*Command)(nil)).Elem()
)

// Register installs handlers on the router. h may be either:
//
//   - A function value with signature func(context.Context, C) (R, error),
//     or func(context.Context, C) error for handlers with no result, where C
//     implements Command — registered as the sole handler for C. The
//     result-less form dispatches as an Empty result.
//
//   - A struct (or pointer to one); each exported method matching the same
//     signature is registered as a handler for its command type. Method
//     names are not significant — the structural signature is the contract.
//
// Returns ErrInvalidHandler if h is nil or no method/signature matches.
// Returns ErrAlreadyRegistered if h contains two methods handling the same
// command type, or if any matched command type is already registered on
// the router. On any error no partial registration takes effect.
//
// Pass a pointer (Register(&svc)) to pick up both value- and
// pointer-receiver methods.
//
// Commands may be value or pointer types, and the two forms are
// interchangeable: a command is keyed by its underlying (element) type, so a
// handler declared for CreateUser also serves Dispatch(&CreateUser{...}) and
// one declared for *CreateUser also serves Dispatch(CreateUser{...}). The
// router adapts the argument to whatever kind the handler declares. Consequently,
// registering separate handlers for CreateUser and *CreateUser is a conflict
// (ErrAlreadyRegistered), not two independent registrations.
func (r *Router) Register(h any) error {
	if h == nil {
		return fmt.Errorf("%w: nil handler", ErrInvalidHandler)
	}
	hv := reflect.ValueOf(h)

	switch hv.Kind() {
	case reflect.Func:
		return r.registerFunc(hv)
	case reflect.Ptr:
		if hv.IsNil() {
			return fmt.Errorf("%w: nil pointer to %s", ErrInvalidHandler, hv.Type().Elem())
		}
	}
	return r.registerMethods(hv)
}

func (r *Router) registerFunc(fn reflect.Value) error {
	if fn.IsNil() {
		return fmt.Errorf("%w: nil func", ErrInvalidHandler)
	}
	cmdT, ok := matchesHandlerSig(fn.Type())
	if !ok {
		return fmt.Errorf("%w: %s does not match the handler shape", ErrInvalidHandler, fn.Type())
	}
	key := commandKey(cmdT)

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.handlers[key]; exists {
		return fmt.Errorf("%w: %s", ErrAlreadyRegistered, key)
	}
	r.handlers[key] = r.wrapReflected(fn)
	return nil
}

func (r *Router) registerMethods(hv reflect.Value) error {
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
		key := commandKey(cmdT)
		if seen[key] {
			return fmt.Errorf("%w: %s has two methods handling %s", ErrAlreadyRegistered, ht, key)
		}
		seen[key] = true
		matched = append(matched, pending{cmdType: key, method: mv})
	}

	if len(matched) == 0 {
		return fmt.Errorf("%w: %s", ErrInvalidHandler, ht)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, p := range matched {
		if _, exists := r.handlers[p.cmdType]; exists {
			return fmt.Errorf("%w: %s", ErrAlreadyRegistered, p.cmdType)
		}
	}
	for _, p := range matched {
		r.handlers[p.cmdType] = r.wrapReflected(p.method)
	}
	return nil
}

// matchesHandlerSig reports whether mt is one of the handler signatures
//
//	func(context.Context, C) (R, error)
//	func(context.Context, C) error
//
// for some C implementing Command, and returns C if so. The result-less form
// dispatches as an Empty result.
func matchesHandlerSig(mt reflect.Type) (reflect.Type, bool) {
	if mt.NumIn() != 2 {
		return nil, false
	}
	if mt.In(0) != ctxType {
		return nil, false
	}
	if !mt.In(1).Implements(commandType) {
		return nil, false
	}
	switch mt.NumOut() {
	case 1:
		if mt.Out(0) != errType {
			return nil, false
		}
	case 2:
		if mt.Out(1) != errType {
			return nil, false
		}
	default:
		return nil, false
	}
	return mt.In(1), true
}

// commandKey normalizes a command type to its canonical map key. The pointer
// and value forms of a command collapse to the same key (the element type),
// so a handler registered for one form serves dispatches of the other.
func commandKey(t reflect.Type) reflect.Type {
	if t.Kind() == reflect.Ptr {
		return t.Elem()
	}
	return t
}

// Dispatch sends cmd to its registered handler and returns the handler's
// result as an opaque any. Wrap the router in a Bus for a typed result.
//
// Returns ErrHandlerNotFound if no handler is registered for cmd's runtime
// type.
func (r *Router) Dispatch(ctx context.Context, cmd Command) (any, error) {
	t := reflect.TypeOf(cmd)
	if t == nil {
		return nil, fmt.Errorf("%w: <nil>", ErrHandlerNotFound)
	}
	cmdType := commandKey(t)

	r.mu.RLock()
	fn, ok := r.handlers[cmdType]
	r.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrHandlerNotFound, cmdType)
	}
	return fn(ctx, cmd)
}

// wrapReflected builds the dispatch closure for a handler (a method value
// or a func value), with the router's middleware chain baked in
// (outermost first).
func (r *Router) wrapReflected(callable reflect.Value) Next {
	ct := callable.Type()
	hasResult := ct.NumOut() == 2
	wantType := ct.In(1)
	inner := Next(func(ctx context.Context, cmd Command) (any, error) {
		out := callable.Call([]reflect.Value{
			reflect.ValueOf(ctx),
			coerceCommand(reflect.ValueOf(cmd), wantType),
		})
		errVal := out[len(out)-1]
		var err error
		if !errVal.IsNil() {
			err = errVal.Interface().(error)
		}
		if !hasResult {
			return Empty{}, err
		}
		return out[0].Interface(), err
	})
	return r.applyMiddleware(inner)
}

// coerceCommand adapts a dispatched command value to the kind (value or
// pointer) the handler declares, so pointer and value forms are
// interchangeable at the dispatch boundary. When the handler wants a pointer
// but a value was dispatched, it operates on an addressable copy; when it
// wants a value but a pointer was dispatched, the pointer is dereferenced.
func coerceCommand(arg reflect.Value, want reflect.Type) reflect.Value {
	if arg.Type() == want {
		return arg
	}
	if want.Kind() == reflect.Ptr {
		p := reflect.New(arg.Type())
		p.Elem().Set(arg)
		return p
	}
	return arg.Elem()
}

func (r *Router) applyMiddleware(inner Next) Next {
	next := inner
	for i := len(r.middleware) - 1; i >= 0; i-- {
		next = r.middleware[i](next)
	}
	return next
}

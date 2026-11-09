package spark

import "context"

// Next is the type-erased function signature middleware receives and returns.
// The result is opaque (any) at this layer; the outermost Bus.Send asserts it
// back to the command's result type.
type Next func(ctx context.Context, cmd Command) (any, error)

// Middleware wraps a Next, returning a new Next that runs additional logic
// before and/or after the inner call.
//
// Middlewares are applied in the order passed to WithMiddleware: the first
// listed becomes the outermost wrapper. They are fixed at NewRouter
// construction time and baked into each handler at Register time.
type Middleware func(next Next) Next

// Option configures a Router at construction time.
type Option func(*Router)

// WithMiddleware installs the given middlewares on the router. The first
// middleware is the outermost wrapper; the handler's typed call is the
// innermost. Calls to WithMiddleware are additive — later WithMiddleware
// options append to the chain.
func WithMiddleware(mw ...Middleware) Option {
	return func(r *Router) {
		r.middleware = append(r.middleware, mw...)
	}
}

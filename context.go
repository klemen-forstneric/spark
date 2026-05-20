package spark

import "context"

// Keyed is an optional interface a command can implement to declare its own
// idempotency key. The Idempotency middleware checks the context first and
// falls back to this method when no context key is set.
type Keyed interface {
	IdempotencyKey() string
}

type idempotencyKeyCtx struct{}

// WithIdempotencyKey returns a context carrying the given idempotency key.
// The Idempotency middleware reads it via IdempotencyKeyFromContext.
func WithIdempotencyKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, idempotencyKeyCtx{}, key)
}

// IdempotencyKeyFromContext returns the idempotency key stored on ctx, if any.
// The second return value is false when no key was set or when the key is
// the empty string.
func IdempotencyKeyFromContext(ctx context.Context) (string, bool) {
	k, ok := ctx.Value(idempotencyKeyCtx{}).(string)
	if !ok || k == "" {
		return "", false
	}
	return k, true
}

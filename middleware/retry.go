package middleware

import (
	"context"
	"time"

	"github.com/klemen-forstneric/spark"
)

// Retry returns a middleware that retries the inner handler up to attempts
// total times (i.e. attempts-1 retries after the initial call) with a
// constant backoff between attempts.
//
// Retry stops early if ctx is cancelled or the handler returns nil. attempts
// less than 1 is normalised to 1.
//
// Place Retry inside Idempotency so retried attempts share a single
// idempotency claim — see middleware/idempotency.go for the rationale.
func Retry(attempts int, backoff time.Duration) spark.Middleware {
	if attempts < 1 {
		attempts = 1
	}
	return func(next spark.Next) spark.Next {
		return func(ctx context.Context, cmd spark.Command) (any, error) {
			var (
				result any
				err    error
			)
			for i := 0; i < attempts; i++ {
				if i > 0 {
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-time.After(backoff):
					}
				}
				result, err = next(ctx, cmd)
				if err == nil {
					return result, nil
				}
			}
			return result, err
		}
	}
}

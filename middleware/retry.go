package middleware

import (
	"context"
	"errors"
	"time"

	"github.com/klemen-forstneric/spark"
)

// RetryConfig configures the Retry middleware.
type RetryConfig struct {
	// Attempts is the total number of tries (initial + retries). Less than 1
	// is normalised to 1.
	Attempts int
	// Backoff is the constant delay between attempts.
	Backoff time.Duration
	// NonRetryable lists errors that must not be retried. If the handler
	// returns an error matching any of these (via errors.Is), Retry returns
	// immediately. Everything else is retried until Attempts is exhausted.
	NonRetryable []error
}

// Retry returns a middleware that retries the inner handler up to
// cfg.Attempts total times (i.e. cfg.Attempts-1 retries after the initial
// call) with a constant backoff between attempts.
//
// Retry stops early if ctx is cancelled, the handler returns nil, or the
// handler returns an error matching one of cfg.NonRetryable (via errors.Is).
//
// Place Retry inside Idempotency so retried attempts share a single
// idempotency claim — see middleware/idempotency.go for the rationale.
func Retry(cfg RetryConfig) spark.Middleware {
	if cfg.Attempts < 1 {
		cfg.Attempts = 1
	}
	return func(next spark.Next) spark.Next {
		return func(ctx context.Context, cmd spark.Command) (any, error) {
			var (
				result any
				err    error
			)
			for i := 0; i < cfg.Attempts; i++ {
				if i > 0 {
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-time.After(cfg.Backoff):
					}
				}
				result, err = next(ctx, cmd)
				if err == nil {
					return result, nil
				}
				for _, nr := range cfg.NonRetryable {
					if errors.Is(err, nr) {
						return result, err
					}
				}
			}
			return result, err
		}
	}
}

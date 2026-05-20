package middleware

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/klemen-forstneric/spark"
)

// Recover returns a middleware that converts panics inside the inner handler
// into errors. The recovered value and a stack trace are wrapped in the
// returned error so the caller can inspect or log them.
//
// Place Recover innermost (closest to the handler) so other middleware can
// observe the synthesised error like any other failure.
func Recover() spark.Middleware {
	return func(next spark.Next) spark.Next {
		return func(ctx context.Context, cmd spark.Command) (result any, err error) {
			defer func() {
				if r := recover(); r != nil {
					result = nil
					err = fmt.Errorf("spark: handler panic in %s: %v\n%s",
						cmd.Type(), r, debug.Stack())
				}
			}()
			return next(ctx, cmd)
		}
	}
}

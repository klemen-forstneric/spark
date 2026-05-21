package middleware

import (
	"context"
	"time"

	"github.com/klemen-forstneric/spark"
)

// Log returns a middleware that logs every command dispatch via logger.
// Successful dispatches log at Info; failures log at Error and pass the
// error as the positional err argument.
func Log(log spark.LoggerCtx) spark.Middleware {
	return func(next spark.Next) spark.Next {
		return func(ctx context.Context, cmd spark.Command) (any, error) {
			start := time.Now()
			result, err := next(ctx, cmd)
			dur := time.Since(start)

			if err != nil {
				log.Error(ctx, "spark: command failed", err,
					"command", cmd.Type(),
					"duration", dur,
				)
			} else {
				log.Info(ctx, "spark: command handled",
					"command", cmd.Type(),
					"duration", dur,
				)
			}
			return result, err
		}
	}
}

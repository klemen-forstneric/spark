package middleware

import (
	"context"
	"log/slog"
	"time"

	"github.com/klemen-forstneric/spark"
)

// Log returns a middleware that logs every command dispatch via logger.
// Successful dispatches log at Info; failures log at Error. Each log line
// includes the command name and elapsed duration.
func Log(logger *slog.Logger) spark.Middleware {
	return func(next spark.Next) spark.Next {
		return func(ctx context.Context, cmd spark.Command) (any, error) {
			start := time.Now()
			result, err := next(ctx, cmd)
			dur := time.Since(start)

			if err != nil {
				logger.LogAttrs(ctx, slog.LevelError, "spark: command failed",
					slog.String("command", cmd.Type()),
					slog.Duration("duration", dur),
					slog.String("error", err.Error()),
				)
			} else {
				logger.LogAttrs(ctx, slog.LevelInfo, "spark: command handled",
					slog.String("command", cmd.Type()),
					slog.Duration("duration", dur),
				)
			}
			return result, err
		}
	}
}

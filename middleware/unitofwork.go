package middleware

import (
	"context"

	"github.com/klemen-forstneric/spark"
)

// Transactor runs fn inside a transaction, passing a context bound to that
// transaction. The concrete impl lives in a driver package (spark/mongo),
// mirroring how Idempotent depends on IdempotencyStore.
type Transactor interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}

// UnitOfWork wraps each write command in a transaction. A command embedding
// spark.ReadOnly skips the transaction. The transaction-bound ctx flows to the
// handler, so downstream repository writes join the same transaction.
func UnitOfWork(t Transactor) spark.Middleware {
	return func(next spark.Next) spark.Next {
		return func(ctx context.Context, cmd spark.Command) (any, error) {
			if spark.IsReadOnly(cmd) {
				return next(ctx, cmd)
			}
			var result any
			err := t.WithinTx(ctx, func(txCtx context.Context) error {
				var e error
				result, e = next(txCtx, cmd)
				return e
			})
			return result, err
		}
	}
}

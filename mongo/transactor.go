package mongo

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Transactor runs work inside a mongo multi-document transaction. It satisfies
// spark/middleware.Transactor structurally (no import of spark). Requires a
// replica set.
type Transactor struct {
	client *mongo.Client
}

func NewTransactor(client *mongo.Client) *Transactor {
	return &Transactor{client: client}
}

// WithinTx starts a session and runs fn inside a transaction. The mongo v2
// driver propagates the session through the context it hands the callback, so
// any collection op run with that ctx auto-joins the transaction.
func (t *Transactor) WithinTx(ctx context.Context, fn func(context.Context) error) error {
	sess, err := t.client.StartSession()
	if err != nil {
		return err
	}
	defer sess.EndSession(ctx)

	_, err = sess.WithTransaction(ctx, func(txCtx context.Context) (any, error) {
		return nil, fn(txCtx)
	})
	return err
}

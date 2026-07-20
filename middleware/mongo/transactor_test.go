package mongo_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
	mongodriver "go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	sparkmongo "github.com/klemen-forstneric/spark/middleware/mongo"
)

type TransactorSuite struct {
	suite.Suite
	client *mongodriver.Client
	col    *mongodriver.Collection
	tx     *sparkmongo.Transactor
}

func TestTransactorSuite(t *testing.T) {
	suite.Run(t, new(TransactorSuite))
}

// SetupTest dials a local mongo replica set (transactions require one) and
// scopes a per-test collection. Skips when mongo is unreachable or not a
// replica set, mirroring ember/mongo's connectTestMongo.
func (s *TransactorSuite) SetupTest() {
	ctx := context.Background()
	client, err := mongodriver.Connect(options.Client().ApplyURI("mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"))
	if err != nil {
		s.T().Skipf("mongo unavailable: %v", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(ctx)
		s.T().Skipf("mongo unavailable: %v", err)
	}
	s.client = client
	s.col = client.Database("spark_test").Collection(fmt.Sprintf("tx_%s", s.T().Name()))
	s.tx = sparkmongo.NewTransactor(client)
	s.T().Cleanup(func() {
		_ = s.col.Drop(context.Background())
		_ = client.Disconnect(context.Background())
	})
}

func (s *TransactorSuite) TestCommitPersistsWrites() {
	err := s.tx.WithinTx(context.Background(), func(txCtx context.Context) error {
		_, e := s.col.InsertOne(txCtx, map[string]any{"_id": "a"})
		return e
	})
	s.Require().NoError(err)

	n, err := s.col.CountDocuments(context.Background(), map[string]any{"_id": "a"})
	s.Require().NoError(err)
	s.Equal(int64(1), n)
}

func (s *TransactorSuite) TestHandlerErrorRollsBack() {
	wantErr := errors.New("boom")
	err := s.tx.WithinTx(context.Background(), func(txCtx context.Context) error {
		if _, e := s.col.InsertOne(txCtx, map[string]any{"_id": "b"}); e != nil {
			return e
		}
		return wantErr // abort after writing
	})
	s.Require().ErrorIs(err, wantErr)

	n, err := s.col.CountDocuments(context.Background(), map[string]any{"_id": "b"})
	s.Require().NoError(err)
	s.Equal(int64(0), n, "aborted transaction must not persist the write")
}

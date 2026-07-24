package middleware_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/klemen-forstneric/spark"
	"github.com/klemen-forstneric/spark/middleware"
)

// mockTransactor is a testify mock for middleware.Transactor. WithinTx executes
// the callback (as a real transaction boundary does) and returns the handler's
// error if any, otherwise the configured transaction-level error (Return arg 0).
type mockTransactor struct {
	mock.Mock
}

func (m *mockTransactor) WithinTx(ctx context.Context, fn func(context.Context) error) error {
	args := m.Called(ctx)
	if err := fn(ctx); err != nil {
		return err
	}
	return args.Error(0)
}

type writeCmd struct{}

func (writeCmd) Type() string { return "write" }

type nonAtomicCmd struct{ spark.NonAtomic }

func (nonAtomicCmd) Type() string { return "read" }

type AtomicSuite struct {
	suite.Suite
	tx *mockTransactor
}

func TestAtomicSuite(t *testing.T) {
	suite.Run(t, new(AtomicSuite))
}

func (s *AtomicSuite) SetupTest() {
	s.tx = &mockTransactor{}
}

func (s *AtomicSuite) TestWriteCommandRunsInsideTransaction() {
	s.tx.On("WithinTx", mock.Anything).Return(nil).Once()
	var handlerCtx context.Context
	next := func(ctx context.Context, cmd spark.Command) (any, error) {
		handlerCtx = ctx
		return "ok", nil
	}

	result, err := middleware.Atomic(s.tx)(next)(context.Background(), writeCmd{})

	s.Require().NoError(err)
	s.Equal("ok", result)
	s.NotNil(handlerCtx, "handler must receive the transaction ctx")
	s.tx.AssertExpectations(s.T())
}

func (s *AtomicSuite) TestNonAtomicCommandSkipsTransaction() {
	next := func(ctx context.Context, cmd spark.Command) (any, error) { return "ro", nil }

	result, err := middleware.Atomic(s.tx)(next)(context.Background(), nonAtomicCmd{})

	s.Require().NoError(err)
	s.Equal("ro", result)
	s.tx.AssertNotCalled(s.T(), "WithinTx", mock.Anything)
}

func (s *AtomicSuite) TestHandlerErrorPropagates() {
	s.tx.On("WithinTx", mock.Anything).Return(nil).Once()
	wantErr := errors.New("boom")
	next := func(ctx context.Context, cmd spark.Command) (any, error) { return nil, wantErr }

	_, err := middleware.Atomic(s.tx)(next)(context.Background(), writeCmd{})

	s.ErrorIs(err, wantErr)
	s.tx.AssertExpectations(s.T())
}

package spark_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/klemen-forstneric/spark"
)

type dispatcherMock struct{ mock.Mock }

func (m *dispatcherMock) Dispatch(ctx context.Context, cmd spark.Command) (any, error) {
	args := m.Called(ctx, cmd)
	return args.Get(0), args.Error(1)
}

type BusSuite struct {
	suite.Suite
	ctx        context.Context
	dispatcher *dispatcherMock
	bus        *spark.Bus
}

func TestBus(t *testing.T) { suite.Run(t, new(BusSuite)) }

func (s *BusSuite) SetupTest() {
	s.ctx = context.Background()
	s.dispatcher = &dispatcherMock{}
	s.bus = spark.NewBus(s.dispatcher)
}

func (s *BusSuite) TearDownTest() { s.dispatcher.AssertExpectations(s.T()) }

func (s *BusSuite) TestDispatchReturnsTypedResult() {
	cmd := CreateUser{Email: "a@b.c"}
	want := &User{ID: "u1", Email: "a@b.c"}
	s.dispatcher.On("Dispatch", s.ctx, cmd).Return(want, nil).Once()

	got, err := s.bus.Dispatch(s.ctx, cmd)

	s.Require().NoError(err)
	s.Same(want, got)
}

func (s *BusSuite) TestDispatchEmptyResult() {
	cmd := DeleteUser{ID: "u1"}
	s.dispatcher.On("Dispatch", s.ctx, cmd).Return(spark.Empty{}, nil).Once()

	got, err := s.bus.Dispatch(s.ctx, cmd)

	s.Require().NoError(err)
	s.Equal(spark.Empty{}, got)
}

func (s *BusSuite) TestDispatchNilResultIsZero() {
	cmd := CreateUser{Email: "a@b.c"}
	s.dispatcher.On("Dispatch", s.ctx, cmd).Return(nil, nil).Once()

	got, err := s.bus.Dispatch(s.ctx, cmd)

	s.Require().NoError(err)
	s.Nil(got)
}

func (s *BusSuite) TestDispatchReturnsDispatchError() {
	cmd := CreateUser{Email: "a@b.c"}
	boom := errors.New("boom")
	s.dispatcher.On("Dispatch", s.ctx, cmd).Return(&User{ID: "u1"}, boom).Once()

	got, err := s.bus.Dispatch(s.ctx, cmd)

	s.Require().ErrorIs(err, boom)
	s.Nil(got)
}

func (s *BusSuite) TestDispatchRejectsWrongResultType() {
	cmd := CreateUser{Email: "a@b.c"}
	s.dispatcher.On("Dispatch", s.ctx, cmd).Return(User{ID: "u1"}, nil).Once()

	got, err := s.bus.Dispatch(s.ctx, cmd)

	s.Require().ErrorIs(err, spark.ErrResultType)
	s.Nil(got)
}

func (s *BusSuite) TestDispatchThroughRouter() {
	router := spark.NewRouter()
	s.Require().NoError(router.Register(&userService{}))
	s.Require().NoError(router.Register(&updateService{}))
	bus := spark.NewBus(router)

	created, err := bus.Dispatch(s.ctx, CreateUser{Email: "a@b.c"})
	s.Require().NoError(err)
	s.Equal(&User{ID: "u1", Email: "a@b.c"}, created)

	updated, err := bus.Dispatch(s.ctx, &UpdateUser{ID: "u2", Email: "x@y.z"})
	s.Require().NoError(err)
	s.Equal(&User{ID: "u2", Email: "normalized:x@y.z"}, updated)

	deleted, err := bus.Dispatch(s.ctx, DeleteUser{ID: "u1"})
	s.Require().NoError(err)
	s.Equal(spark.Empty{}, deleted)
}

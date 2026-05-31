package spark_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/klemen-forstneric/spark"
	"github.com/klemen-forstneric/spark/middleware"
)

// fakeStore is a minimal stateful middleware.IdempotencyStore for tests.
// Users testing application code with idempotency can roll a similar
// double, or wire a testify mock with stateful callbacks.
type fakeStore struct {
	mu       sync.Mutex
	entries  map[string]middleware.IdempotencyResult
	inflight map[string]bool
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		entries:  map[string]middleware.IdempotencyResult{},
		inflight: map[string]bool{},
	}
}

func (s *fakeStore) Claim(_ context.Context, key string) (*middleware.IdempotencyResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.entries[key]; ok {
		return &r, nil
	}
	if s.inflight[key] {
		return nil, middleware.ErrCommandInFlight
	}
	s.inflight[key] = true
	return nil, nil
}

func (s *fakeStore) Save(_ context.Context, key string, r middleware.IdempotencyResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[key] = r
	delete(s.inflight, key)
	return nil
}

func (s *fakeStore) Release(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, key)
	delete(s.inflight, key)
	return nil
}

func (s *fakeStore) isInflight(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inflight[key]
}

type User struct {
	ID    string
	Email string
}

type CreateUser struct {
	spark.Result[*User]
	Email string
}

func (CreateUser) Type() string { return "user.create" }

type DeleteUser struct {
	spark.Result[spark.Empty]
	ID string
}

func (DeleteUser) Type() string { return "user.delete" }

type userService struct {
	created []string
	deleted []string
}

func (s *userService) Create(_ context.Context, cmd CreateUser) (*User, error) {
	s.created = append(s.created, cmd.Email)
	return &User{ID: "u1", Email: cmd.Email}, nil
}

func (s *userService) Delete(_ context.Context, cmd DeleteUser) error {
	s.deleted = append(s.deleted, cmd.ID)
	return nil
}

// Healthcheck is exported but has a non-matching signature; the structural
// scan must skip it.
func (s *userService) Healthcheck() error { return nil }

func TestDispatchTypedResult(t *testing.T) {
	bus := spark.NewBus()
	svc := &userService{}
	if err := bus.Register(svc); err != nil {
		t.Fatalf("Register: %v", err)
	}

	raw, err := bus.Dispatch(context.Background(), CreateUser{Email: "a@b.c"})
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	got := raw.(*User)
	if got == nil || got.Email != "a@b.c" {
		t.Fatalf("unexpected result: %+v", got)
	}
	if len(svc.created) != 1 || svc.created[0] != "a@b.c" {
		t.Fatalf("handler not invoked: %v", svc.created)
	}
}

func TestDispatchEmptyResult(t *testing.T) {
	bus := spark.NewBus()
	svc := &userService{}
	if err := bus.Register(svc); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if _, err := bus.Dispatch(context.Background(), DeleteUser{ID: "u1"}); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if len(svc.deleted) != 1 || svc.deleted[0] != "u1" {
		t.Fatalf("handler not invoked: %v", svc.deleted)
	}
}

func TestRegisterScansMultipleMethods(t *testing.T) {
	bus := spark.NewBus()
	if err := bus.Register(&userService{}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, err := bus.Dispatch(context.Background(), CreateUser{Email: "x"}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if _, err := bus.Dispatch(context.Background(), DeleteUser{ID: "x"}); err != nil {
		t.Fatalf("DeleteUser: %v", err)
	}
}

func TestRegisterRejectsNoMatch(t *testing.T) {
	bus := spark.NewBus()
	if err := bus.Register(struct{ X int }{}); !errors.Is(err, spark.ErrInvalidHandler) {
		t.Fatalf("expected ErrInvalidHandler, got %v", err)
	}
}

func TestRegisterRejectsDuplicate(t *testing.T) {
	bus := spark.NewBus()
	if err := bus.Register(&userService{}); err != nil {
		t.Fatalf("first Register: %v", err)
	}
	if err := bus.Register(&userService{}); !errors.Is(err, spark.ErrAlreadyRegistered) {
		t.Fatalf("expected ErrAlreadyRegistered, got %v", err)
	}
}

func TestDispatchUnregistered(t *testing.T) {
	bus := spark.NewBus()
	_, err := bus.Dispatch(context.Background(), CreateUser{})
	if !errors.Is(err, spark.ErrHandlerNotFound) {
		t.Fatalf("expected ErrHandlerNotFound, got %v", err)
	}
}

func TestRegisterFunc(t *testing.T) {
	bus := spark.NewBus()
	calls := 0
	if err := bus.Register(func(_ context.Context, cmd CreateUser) (*User, error) {
		calls++
		return &User{Email: cmd.Email}, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	raw, err := bus.Dispatch(context.Background(), CreateUser{Email: "z"})
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	got := raw.(*User)
	if got.Email != "z" || calls != 1 {
		t.Fatalf("unexpected: got=%+v calls=%d", got, calls)
	}
}

func TestMiddlewareOrder(t *testing.T) {
	var calls []string
	mw := func(name string) spark.Middleware {
		return func(next spark.Next) spark.Next {
			return func(ctx context.Context, cmd spark.Command) (any, error) {
				calls = append(calls, "before:"+name)
				r, err := next(ctx, cmd)
				calls = append(calls, "after:"+name)
				return r, err
			}
		}
	}

	bus := spark.NewBus(spark.WithMiddleware(mw("outer"), mw("inner")))
	if err := bus.Register(&userService{}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, err := bus.Dispatch(context.Background(), CreateUser{Email: "e"}); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	want := []string{"before:outer", "before:inner", "after:inner", "after:outer"}
	if len(calls) != len(want) {
		t.Fatalf("got %v want %v", calls, want)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("got %v want %v", calls, want)
		}
	}
}

func TestIdempotencyCacheHit(t *testing.T) {
	store := newFakeStore()
	bus := spark.NewBus(spark.WithMiddleware(middleware.Idempotent(middleware.IdempotencyConfig{Store: store})))
	svc := &userService{}
	if err := bus.Register(svc); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ctx := spark.WithIdempotencyKey(context.Background(), "key-1")
	rawFirst, err := bus.Dispatch(ctx, CreateUser{Email: "a"})
	if err != nil {
		t.Fatalf("first dispatch: %v", err)
	}
	rawSecond, err := bus.Dispatch(ctx, CreateUser{Email: "b"})
	if err != nil {
		t.Fatalf("second dispatch: %v", err)
	}

	first, second := rawFirst.(*User), rawSecond.(*User)
	if first.Email != "a" || second.Email != "a" {
		t.Fatalf("expected cached first result, got first=%+v second=%+v", first, second)
	}
	if len(svc.created) != 1 {
		t.Fatalf("expected handler invoked once, got %d", len(svc.created))
	}
}

type flakyService struct {
	attempts int
}

type Flaky struct {
	spark.Result[spark.Empty]
}

func (Flaky) Type() string { return "flaky" }

func (s *flakyService) Run(_ context.Context, _ Flaky) error {
	s.attempts++
	if s.attempts < 3 {
		return errors.New("transient")
	}
	return nil
}

func TestRetry(t *testing.T) {
	bus := spark.NewBus(spark.WithMiddleware(middleware.Retry(5, time.Millisecond)))
	svc := &flakyService{}
	if err := bus.Register(svc); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, err := bus.Dispatch(context.Background(), Flaky{}); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if svc.attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", svc.attempts)
	}
}

type panicker struct{}

type Boom struct {
	spark.Result[spark.Empty]
}

func (Boom) Type() string { return "boom" }

func (panicker) Handle(_ context.Context, _ Boom) error {
	panic("oops")
}

func TestIdempotencyPayloadMismatch(t *testing.T) {
	store := newFakeStore()
	bus := spark.NewBus(spark.WithMiddleware(
		middleware.Idempotent(middleware.IdempotencyConfig{Store: store, Hasher: middleware.JSONHasher{}}),
	))
	svc := &userService{}
	if err := bus.Register(svc); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ctx := spark.WithIdempotencyKey(context.Background(), "key-1")
	if _, err := bus.Dispatch(ctx, CreateUser{Email: "a"}); err != nil {
		t.Fatalf("first dispatch: %v", err)
	}
	_, err := bus.Dispatch(ctx, CreateUser{Email: "different"})
	if !errors.Is(err, middleware.ErrPayloadMismatch) {
		t.Fatalf("expected ErrPayloadMismatch, got %v", err)
	}
}

func TestIdempotencySamePayloadStillHits(t *testing.T) {
	store := newFakeStore()
	bus := spark.NewBus(spark.WithMiddleware(
		middleware.Idempotent(middleware.IdempotencyConfig{Store: store, Hasher: middleware.JSONHasher{}}),
	))
	svc := &userService{}
	if err := bus.Register(svc); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ctx := spark.WithIdempotencyKey(context.Background(), "key-1")
	if _, err := bus.Dispatch(ctx, CreateUser{Email: "a"}); err != nil {
		t.Fatalf("first dispatch: %v", err)
	}
	if _, err := bus.Dispatch(ctx, CreateUser{Email: "a"}); err != nil {
		t.Fatalf("second dispatch: %v", err)
	}
	if len(svc.created) != 1 {
		t.Fatalf("expected handler invoked once, got %d", len(svc.created))
	}
}

func TestIdempotencyDetachedWrite(t *testing.T) {
	store := newFakeStore()
	bus := spark.NewBus(spark.WithMiddleware(middleware.Idempotent(middleware.IdempotencyConfig{Store: store})))
	svc := &userService{}
	if err := bus.Register(svc); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Cancel ctx mid-flight: the handler succeeds, then the cache write must
	// still happen (uses context.WithoutCancel under the hood).
	ctx, cancel := context.WithCancel(spark.WithIdempotencyKey(context.Background(), "key-1"))
	if _, err := bus.Dispatch(ctx, CreateUser{Email: "a"}); err != nil {
		t.Fatalf("first dispatch: %v", err)
	}
	cancel()

	// A fresh, uncancelled dispatch with the same key should hit the cache.
	ctx2 := spark.WithIdempotencyKey(context.Background(), "key-1")
	raw, err := bus.Dispatch(ctx2, CreateUser{Email: "ignored"})
	if err != nil {
		t.Fatalf("second dispatch: %v", err)
	}
	if raw.(*User).Email != "a" {
		t.Fatalf("expected cached value, got %+v", raw)
	}
	if len(svc.created) != 1 {
		t.Fatalf("expected handler invoked once, got %d", len(svc.created))
	}
}

// TestIdempotencyCacheHitWithCodec simulates a cross-process Store by
// asserting that prior.Result is []byte (post-codec wrap) and that the
// middleware unwraps it back to the original *User on the cache hit.
func TestIdempotencyCacheHitWithCodec(t *testing.T) {
	store := newFakeStore()
	cdc := middleware.NewJSONCodec(middleware.NewRegistry(&User{}))
	bus := spark.NewBus(spark.WithMiddleware(middleware.Idempotent(middleware.IdempotencyConfig{Store: store, Codec: cdc})))
	svc := &userService{}
	if err := bus.Register(svc); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ctx := spark.WithIdempotencyKey(context.Background(), "key-codec")
	rawFirst, err := bus.Dispatch(ctx, CreateUser{Email: "a"})
	if err != nil {
		t.Fatalf("first dispatch: %v", err)
	}
	if rawFirst.(*User).Email != "a" {
		t.Fatalf("first result: %+v", rawFirst)
	}

	store.mu.Lock()
	entry := store.entries["key-codec"]
	store.mu.Unlock()
	if _, ok := entry.Result.([]byte); !ok {
		t.Fatalf("stored Result is %T, want []byte (codec should have wrapped it)", entry.Result)
	}

	rawSecond, err := bus.Dispatch(ctx, CreateUser{Email: "ignored"})
	if err != nil {
		t.Fatalf("second dispatch: %v", err)
	}
	got, ok := rawSecond.(*User)
	if !ok {
		t.Fatalf("cache hit returned %T, want *User", rawSecond)
	}
	if got.Email != "a" {
		t.Fatalf("expected cached *User{Email: a}, got %+v", got)
	}
	if len(svc.created) != 1 {
		t.Fatalf("expected handler invoked once, got %d", len(svc.created))
	}
}

func TestIdempotencyReleasesClaimOnPanic(t *testing.T) {
	store := newFakeStore()
	bus := spark.NewBus(spark.WithMiddleware(middleware.Idempotent(middleware.IdempotencyConfig{Store: store})))
	if err := bus.Register(panicker{}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ctx := spark.WithIdempotencyKey(context.Background(), "panic-key")
	func() {
		defer func() { _ = recover() }()
		_, _ = bus.Dispatch(ctx, Boom{})
	}()

	if store.isInflight("panic-key") {
		t.Fatal("expected claim to be released after handler panic, still in flight")
	}
}

func TestRecover(t *testing.T) {
	bus := spark.NewBus(spark.WithMiddleware(middleware.Recover()))
	if err := bus.Register(panicker{}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, err := bus.Dispatch(context.Background(), Boom{}); err == nil {
		t.Fatal("expected error from panic")
	}
}

// UpdateUser exercises pointer-command flow: handler takes *UpdateUser, the
// caller dispatches with &UpdateUser{...}, and the bus keys by *UpdateUser.
type UpdateUser struct {
	spark.Result[*User]
	ID    string
	Email string
}

func (*UpdateUser) Type() string { return "user.update" }

type updateService struct{ seen []string }

func (s *updateService) Update(_ context.Context, cmd *UpdateUser) (*User, error) {
	s.seen = append(s.seen, cmd.ID)
	cmd.Email = "normalized:" + cmd.Email // mutation visible to caller
	return &User{ID: cmd.ID, Email: cmd.Email}, nil
}

func TestPointerCommand(t *testing.T) {
	bus := spark.NewBus()
	svc := &updateService{}
	if err := bus.Register(svc); err != nil {
		t.Fatalf("Register: %v", err)
	}

	cmd := &UpdateUser{ID: "u1", Email: "a@b.c"}
	raw, err := bus.Dispatch(context.Background(), cmd)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	got := raw.(*User)
	if got.ID != "u1" || got.Email != "normalized:a@b.c" {
		t.Fatalf("unexpected result: %+v", got)
	}
	if cmd.Email != "normalized:a@b.c" {
		t.Fatalf("handler did not mutate caller's command: %+v", cmd)
	}
	if len(svc.seen) != 1 || svc.seen[0] != "u1" {
		t.Fatalf("handler not invoked: %v", svc.seen)
	}
}

// TestPointerValueInterchangeable verifies that the pointer and value forms of
// a command resolve to the same handler regardless of which kind the handler
// declares or the caller dispatches.
func TestPointerValueInterchangeable(t *testing.T) {
	t.Run("value handler, pointer dispatch", func(t *testing.T) {
		bus := spark.NewBus()
		var got string
		err := bus.Register(func(_ context.Context, c CreateUser) (*User, error) {
			got = c.Email
			return &User{Email: c.Email}, nil
		})
		if err != nil {
			t.Fatalf("Register: %v", err)
		}
		if _, err := bus.Dispatch(context.Background(), &CreateUser{Email: "a@b.c"}); err != nil {
			t.Fatalf("Dispatch pointer: %v", err)
		}
		if got != "a@b.c" {
			t.Fatalf("handler not invoked, got %q", got)
		}
	})

	t.Run("pointer handler, value dispatch", func(t *testing.T) {
		bus := spark.NewBus()
		var got string
		err := bus.Register(func(_ context.Context, c *CreateUser) (*User, error) {
			got = c.Email
			return &User{Email: c.Email}, nil
		})
		if err != nil {
			t.Fatalf("Register: %v", err)
		}
		if _, err := bus.Dispatch(context.Background(), CreateUser{Email: "x@y.z"}); err != nil {
			t.Fatalf("Dispatch value: %v", err)
		}
		if got != "x@y.z" {
			t.Fatalf("handler not invoked, got %q", got)
		}
	})
}

// TestPointerValueConflict verifies that registering both forms of the same
// command is a conflict rather than two independent registrations.
func TestPointerValueConflict(t *testing.T) {
	bus := spark.NewBus()
	if err := bus.Register(func(_ context.Context, _ CreateUser) (*User, error) {
		return nil, nil
	}); err != nil {
		t.Fatalf("first Register: %v", err)
	}
	err := bus.Register(func(_ context.Context, _ *CreateUser) (*User, error) {
		return nil, nil
	})
	if !errors.Is(err, spark.ErrAlreadyRegistered) {
		t.Fatalf("expected ErrAlreadyRegistered, got %v", err)
	}
}

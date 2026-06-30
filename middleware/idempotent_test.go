package middleware_test

import (
	"context"
	"sync"
	"testing"

	"github.com/klemen-forstneric/spark"
	"github.com/klemen-forstneric/spark/middleware"
)

// memStore is a minimal in-memory IdempotencyStore for tests.
type memStore struct {
	mu       sync.Mutex
	entries  map[string]middleware.IdempotencyResult
	inflight map[string]bool
}

func newMemStore() *memStore {
	return &memStore{
		entries:  map[string]middleware.IdempotencyResult{},
		inflight: map[string]bool{},
	}
}

func (s *memStore) Claim(_ context.Context, key string) (*middleware.IdempotencyResult, error) {
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

func (s *memStore) Save(_ context.Context, key string, r middleware.IdempotencyResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[key] = r
	delete(s.inflight, key)
	return nil
}

func (s *memStore) Release(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, key)
	delete(s.inflight, key)
	return nil
}

// peek reports whether the given key is present in the store (entries or inflight).
func (s *memStore) peek(key string) (middleware.IdempotencyResult, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.entries[key]
	if ok {
		return r, true
	}
	if s.inflight[key] {
		return middleware.IdempotencyResult{}, true
	}
	return middleware.IdempotencyResult{}, false
}

// testCmd implements spark.Command with Type() == "test.cmd".
type testCmd struct{}

func (testCmd) Type() string { return "test.cmd" }

// otherTestCmd implements spark.Command with Type() == "test.other".
type otherTestCmd struct{}

func (otherTestCmd) Type() string { return "test.other" }

func TestIdempotent_QualifyByType_AppendsTypeToContextKey(t *testing.T) {
	store := newMemStore()
	calls := 0
	mw := middleware.Idempotent(middleware.IdempotencyConfig{
		Store:         store,
		QualifyByType: true,
	})
	next := mw(func(ctx context.Context, cmd spark.Command) (any, error) {
		calls++
		return "ok", nil
	})

	ctx := spark.WithIdempotencyKey(context.Background(), "evt-1")
	_, err := next(ctx, testCmd{})
	if err != nil {
		t.Fatalf("first dispatch: %v", err)
	}
	_, err = next(ctx, testCmd{})
	if err != nil {
		t.Fatalf("second dispatch: %v", err)
	}

	if calls != 1 {
		t.Errorf("calls = %d, want 1 (second dispatch with same key+type must hit cache)", calls)
	}
	_, claimed := store.peek("evt-1:test.cmd")
	if !claimed {
		t.Errorf("key %q not found in store; key must be qualified with cmd.Type()", "evt-1:test.cmd")
	}
}

func TestIdempotent_QualifyByType_DistinctTypesDoNotCollide(t *testing.T) {
	store := newMemStore()
	calls := 0
	mw := middleware.Idempotent(middleware.IdempotencyConfig{Store: store, QualifyByType: true})
	next := mw(func(ctx context.Context, cmd spark.Command) (any, error) { calls++; return nil, nil })

	ctx := spark.WithIdempotencyKey(context.Background(), "evt-1")
	_, _ = next(ctx, testCmd{})      // type "test.cmd"
	_, _ = next(ctx, otherTestCmd{}) // type "test.other"

	if calls != 2 {
		t.Errorf("calls = %d, want 2 (different command types under one key must both run)", calls)
	}
}

func TestIdempotent_QualifyByType_False_VerbatimKey(t *testing.T) {
	store := newMemStore()
	calls := 0
	mw := middleware.Idempotent(middleware.IdempotencyConfig{Store: store})
	next := mw(func(ctx context.Context, cmd spark.Command) (any, error) { calls++; return "ok", nil })

	ctx := spark.WithIdempotencyKey(context.Background(), "evt-2")
	_, _ = next(ctx, testCmd{})
	_, _ = next(ctx, testCmd{})

	if calls != 1 {
		t.Errorf("calls = %d, want 1 (verbatim key dedup must still work with QualifyByType=false)", calls)
	}
	_, ok := store.peek("evt-2")
	if !ok {
		t.Errorf("verbatim key %q not found in store", "evt-2")
	}
}

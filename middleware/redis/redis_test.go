package redis_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/klemen-forstneric/spark/middleware"
	sparkredis "github.com/klemen-forstneric/spark/middleware/redis"
)

func newStore(t *testing.T) (*sparkredis.Store, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := sparkredis.NewStore(client, sparkredis.Config{
		InflightTTL:  time.Second,
		CompletedTTL: time.Minute,
	})
	return store, mr
}

func TestClaimAcquires(t *testing.T) {
	store, _ := newStore(t)
	prior, err := store.Claim(context.Background(), "k")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if prior != nil {
		t.Fatalf("expected nil prior on fresh claim, got %+v", prior)
	}
}

func TestClaimInflight(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	if _, err := store.Claim(ctx, "k"); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	prior, err := store.Claim(ctx, "k")
	if !errors.Is(err, middleware.ErrCommandInFlight) {
		t.Fatalf("expected ErrCommandInFlight, got prior=%+v err=%v", prior, err)
	}
}

func TestSaveAndCacheHit(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	if _, err := store.Claim(ctx, "k"); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := store.Save(ctx, "k", middleware.IdempotencyResult{
		Result: []byte(`{"id":"u1"}`),
		Hash:   42,
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	prior, err := store.Claim(ctx, "k")
	if err != nil {
		t.Fatalf("second Claim: %v", err)
	}
	if prior == nil {
		t.Fatal("expected cached entry, got nil")
	}
	got, ok := prior.Result.([]byte)
	if !ok {
		t.Fatalf("Result type %T, want []byte", prior.Result)
	}
	if string(got) != `{"id":"u1"}` {
		t.Fatalf("Result: %s", got)
	}
	if prior.Hash != 42 {
		t.Fatalf("Hash: %d", prior.Hash)
	}
	if prior.Err != nil {
		t.Fatalf("Err: %v", prior.Err)
	}
}

func TestSaveRoundTripsErr(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	if _, err := store.Claim(ctx, "k"); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := store.Save(ctx, "k", middleware.IdempotencyResult{
		Err: errors.New("boom"),
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	prior, err := store.Claim(ctx, "k")
	if err != nil {
		t.Fatalf("second Claim: %v", err)
	}
	if prior == nil || prior.Err == nil || prior.Err.Error() != "boom" {
		t.Fatalf("expected err=boom, got %+v", prior)
	}
}

func TestRelease(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	if _, err := store.Claim(ctx, "k"); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := store.Release(ctx, "k"); err != nil {
		t.Fatalf("Release: %v", err)
	}
	prior, err := store.Claim(ctx, "k")
	if err != nil {
		t.Fatalf("Claim after release: %v", err)
	}
	if prior != nil {
		t.Fatalf("expected fresh claim after release, got %+v", prior)
	}
}

func TestInflightTTLExpires(t *testing.T) {
	store, mr := newStore(t)
	ctx := context.Background()
	if _, err := store.Claim(ctx, "k"); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	mr.FastForward(2 * time.Second)
	prior, err := store.Claim(ctx, "k")
	if err != nil {
		t.Fatalf("Claim after TTL: %v", err)
	}
	if prior != nil {
		t.Fatalf("expected reclaim after inflight TTL, got %+v", prior)
	}
}

func TestSaveRejectsNonByteResult(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	if err := store.Save(ctx, "k", middleware.IdempotencyResult{Result: "not bytes"}); err == nil {
		t.Fatal("expected error for non-[]byte result")
	}
}

package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/klemen-forstneric/spark/middleware"
)

// newStoreWithNonce builds a Store backed by miniredis where the nonce
// generator is replaced by a controllable sequence. Each Claim consumes
// the next value; reusing the same value across two Claims simulates a
// go-redis transparent retry of an already-applied SET NX GET.
func newStoreWithNonce(t *testing.T, nonces ...string) *Store {
	t.Helper()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	s := NewStore(client, Config{
		InflightTTL:  time.Second,
		CompletedTTL: time.Minute,
	})

	i := 0
	s.newNonce = func() string {
		if i >= len(nonces) {
			t.Fatalf("nonce generator exhausted after %d calls", i)
		}
		v := nonces[i]
		i++
		return v
	}
	return s
}

// TestClaimRetryWithSameNonceIsAcquired covers the go-redis transparent
// retry case (issue #2985): the SET NX GET landed server-side, the
// response was lost, and a second invocation reads back the marker we
// just wrote. The same nonce tells us it's our own write, not a conflict.
func TestClaimRetryWithSameNonceIsAcquired(t *testing.T) {
	store := newStoreWithNonce(t, "nonce-A", "nonce-A")
	ctx := context.Background()

	if _, err := store.Claim(ctx, "k"); err != nil {
		t.Fatalf("first Claim: %v", err)
	}
	prior, err := store.Claim(ctx, "k")
	if err != nil {
		t.Fatalf("retry Claim: %v", err)
	}
	if prior != nil {
		t.Fatalf("expected retry to be treated as acquired, got prior=%+v", prior)
	}
}

// TestClaimDifferentNonceIsConflict confirms the genuine in-flight path
// still works: two distinct dispatchers will pick different nonces, and
// the second must see ErrCommandInFlight.
func TestClaimDifferentNonceIsConflict(t *testing.T) {
	store := newStoreWithNonce(t, "nonce-A", "nonce-B")
	ctx := context.Background()

	if _, err := store.Claim(ctx, "k"); err != nil {
		t.Fatalf("first Claim: %v", err)
	}
	_, err := store.Claim(ctx, "k")
	if !errors.Is(err, middleware.ErrCommandInFlight) {
		t.Fatalf("expected ErrCommandInFlight on distinct nonce, got %v", err)
	}
}

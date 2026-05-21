// Package redis provides a Redis-backed implementation of
// middleware.IdempotencyStore for use with the Idempotent middleware.
//
// A Codec (configured via middleware.IdempotencyConfig.Codec) is required.
// Results round-trip across the Redis boundary as []byte, so the middleware
// must wrap them via a Codec before Save and unwrap them after Claim. Save
// returns an error if the result is not already []byte.
//
// Errors are serialized as their Error() string and reconstructed via
// errors.New on cache hit — this loses error type identity, so errors.Is
// and errors.As against the original sentinel will not match. If you need
// typed errors to survive the cache, encode them into the result value
// instead of relying on the second return value of the handler.
//
// Connection-retry safety: go-redis transparently retries commands when the
// connection drops mid-response. For a naive `SET NX GET` that means the
// first attempt could succeed server-side but the response is lost, and the
// retry would see the key we just wrote and report "already exists" — the
// caller would treat their own claim as a conflict (see
// https://github.com/redis/go-redis/issues/2985). To stay safe, every Claim
// stamps a random nonce into the inflight marker; if a retry reads back a
// marker carrying our nonce, we recognise it as our own write and treat the
// claim as acquired.
package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/klemen-forstneric/spark/middleware"
)

const (
	defaultPrefix       = "spark:idem:"
	defaultInflightTTL  = 30 * time.Second
	defaultCompletedTTL = 24 * time.Hour

	statusInflight = "inflight"
	statusDone     = "done"
)

// Config tunes a Store's behaviour. All fields are optional; the zero
// value is valid and uses the defaults documented below.
type Config struct {
	// Prefix is prepended to every key. Defaults to "spark:idem:".
	Prefix string

	// InflightTTL bounds how long an unfinished claim can block other
	// dispatchers. Make it longer than the p99 handler duration so
	// well-behaved dispatches never expire mid-flight, but short enough
	// that an orphaned claim from a crashed process is reclaimable in an
	// acceptable window. Defaults to 30 seconds.
	InflightTTL time.Duration

	// CompletedTTL is the lifetime of a cached outcome. Choose based on
	// the longest realistic client retry window. Defaults to 24 hours.
	CompletedTTL time.Duration
}

// Store implements middleware.IdempotencyStore on top of Redis.
type Store struct {
	client       goredis.UniversalClient
	prefix       string
	inflightTTL  time.Duration
	completedTTL time.Duration
	newNonce     func() string
}

// NewStore returns a Redis-backed Store wrapping the given go-redis client
// (any UniversalClient: Client, ClusterClient, Ring, sentinel Client).
// Panics if client is nil.
func NewStore(client goredis.UniversalClient, cfg Config) *Store {
	if client == nil {
		panic("spark/middleware/redis: client is required")
	}
	s := &Store{
		client:       client,
		prefix:       cfg.Prefix,
		inflightTTL:  cfg.InflightTTL,
		completedTTL: cfg.CompletedTTL,
		newNonce:     cryptoNonce,
	}
	if s.prefix == "" {
		s.prefix = defaultPrefix
	}
	if s.inflightTTL <= 0 {
		s.inflightTTL = defaultInflightTTL
	}
	if s.completedTTL <= 0 {
		s.completedTTL = defaultCompletedTTL
	}
	return s
}

// cryptoNonce returns 128 bits of randomness as a 32-char hex string.
// Wide enough that two concurrent claims will never collide on it.
func cryptoNonce() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// entry is the on-wire representation of a claim or completed outcome.
// Nonce is set only on inflight entries and is used to recognise our own
// writes if go-redis transparently retries the Claim command.
type entry struct {
	Status string `json:"s"`
	Nonce  string `json:"n,omitempty"`
	Result []byte `json:"r,omitempty"`
	ErrMsg string `json:"e,omitempty"`
	Hash   uint64 `json:"h,omitempty"`
}

// Claim implements middleware.IdempotencyStore. The atomic check-and-set
// uses `SET ... NX GET PX <ttl>`: if the key is absent we set the inflight
// marker and the command returns nil; if the key is present NX suppresses
// the write and GET returns the existing value. Requires Redis 7.0+.
//
// The inflight marker carries a fresh nonce. If go-redis transparently
// retries this command after the first attempt landed on the server but
// the response was lost, the retry's GET will return the marker we just
// wrote; we recognise our own nonce and treat the claim as acquired.
func (s *Store) Claim(ctx context.Context, key string) (*middleware.IdempotencyResult, error) {
	nonce := s.newNonce()
	inflight, err := json.Marshal(entry{Status: statusInflight, Nonce: nonce})
	if err != nil {
		return nil, fmt.Errorf("spark/middleware/redis: encode inflight marker: %w", err)
	}

	setArgs := goredis.SetArgs{
		Mode: "NX",
		Get:  true,
		TTL:  s.inflightTTL,
	}

	prior, err := s.client.SetArgs(ctx, s.prefix+key, inflight, setArgs).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil, nil
		}

		return nil, fmt.Errorf("spark/middleware/redis: claim: %w", err)
	}

	var e entry
	if err := json.Unmarshal([]byte(prior), &e); err != nil {
		return nil, fmt.Errorf("spark/middleware/redis: decode entry: %w", err)
	}

	if e.Status == statusInflight {
		if e.Nonce == nonce {
			return nil, nil
		}
		return nil, middleware.ErrCommandInFlight
	}

	out := &middleware.IdempotencyResult{Hash: e.Hash}
	if e.ErrMsg != "" {
		out.Err = errors.New(e.ErrMsg)
	}
	if len(e.Result) > 0 {
		out.Result = e.Result
	}
	return out, nil
}

// Save implements middleware.IdempotencyStore. The Result field must be
// []byte (configure middleware.IdempotencyConfig.Codec to satisfy this).
func (s *Store) Save(ctx context.Context, key string, r middleware.IdempotencyResult) error {
	e := entry{Status: statusDone, Hash: r.Hash}
	if r.Err != nil {
		e.ErrMsg = r.Err.Error()
	}
	if r.Result != nil {
		data, ok := r.Result.([]byte)
		if !ok {
			return fmt.Errorf("spark/middleware/redis: result must be []byte (configure middleware.IdempotencyConfig.Codec), got %T", r.Result)
		}
		e.Result = data
	}
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("spark/middleware/redis: encode entry: %w", err)
	}
	if err := s.client.Set(ctx, s.prefix+key, data, s.completedTTL).Err(); err != nil {
		return fmt.Errorf("spark/middleware/redis: save: %w", err)
	}
	return nil
}

// Release implements middleware.IdempotencyStore.
func (s *Store) Release(ctx context.Context, key string) error {
	if err := s.client.Del(ctx, s.prefix+key).Err(); err != nil {
		return fmt.Errorf("spark/middleware/redis: release: %w", err)
	}
	return nil
}

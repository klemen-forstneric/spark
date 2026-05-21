package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"

	"github.com/klemen-forstneric/spark"
)

// IdempotencyResult is the cached outcome of a previous dispatch keyed by an
// idempotency key. All fields are propagated back to the caller on a cache
// hit; Hash is set only when the middleware was configured with a Hasher.
type IdempotencyResult struct {
	Result any
	Err    error
	Hash   uint64
}

// IdempotencyStore persists idempotency claims and their outcomes. Backends
// must implement Claim atomically: a key is either claimed by the caller or
// already present.
type IdempotencyStore interface {
	// Claim atomically claims key for the caller. The returned pointer is
	// non-nil if a completed entry already exists, in which case the caller
	// must return the cached result without invoking the handler. A nil
	// pointer with a nil error means the caller has claimed the key and
	// should proceed. ErrCommandInFlight is returned when another
	// in-flight dispatch already holds the claim.
	Claim(ctx context.Context, key string) (prior *IdempotencyResult, err error)

	// Save records the outcome for a key previously claimed via Claim.
	Save(ctx context.Context, key string, result IdempotencyResult) error

	// Release drops a claim without recording an outcome.
	Release(ctx context.Context, key string) error
}

// Hasher computes a stable hash of a command's payload. When set on
// IdempotencyConfig, the hash is compared on cache hits to defend against
// an idempotency key being reused with a mutated payload.
type Hasher interface {
	Hash(cmd spark.Command) (uint64, error)
}

// JSONHasher hashes the JSON encoding of a command via FNV-1a. Stable for
// commands whose exported fields fully describe their identity. Commands
// with unexported state or non-marshalable fields need a custom Hasher.
type JSONHasher struct{}

// Hash implements Hasher.
func (JSONHasher) Hash(cmd spark.Command) (uint64, error) {
	data, err := json.Marshal(cmd)
	if err != nil {
		return 0, err
	}
	h := fnv.New64a()
	_, _ = h.Write(data)
	return h.Sum64(), nil
}

var (
	// ErrCommandInFlight is returned when a concurrent dispatch for the
	// same idempotency key is already running. Callers may retry after a
	// short backoff to read the cached result.
	ErrCommandInFlight = errors.New("spark/middleware: command with this idempotency key is already in flight")

	// ErrMissingIdempotencyKey is returned (only when IdempotencyConfig.RequireKey is set) if
	// no idempotency key is present on the context or command.
	ErrMissingIdempotencyKey = errors.New("spark/middleware: no idempotency key on context or command")

	// ErrPayloadMismatch is returned when an idempotency key is reused with
	// a different command payload than the original dispatch. Only emitted
	// when IdempotencyConfig.Hasher is set.
	ErrPayloadMismatch = errors.New("spark/middleware: idempotency key reused with different command payload")
)

// IdempotencyConfig configures the Idempotent middleware. Store is required;
// all other fields are optional and have sensible zero-value defaults.
type IdempotencyConfig struct {
	// Store is the backing IdempotencyStore. Required.
	Store IdempotencyStore

	// Logger reports store failures (failed Save/Release, orphan claim
	// cleanup after panic) at Warn level. Defaults to spark.NopLogger if
	// nil.
	Logger spark.LoggerCtx

	// Codec wraps result values for cross-process stores. Nil leaves
	// results as opaque Go values, which only works for in-memory stores —
	// any serializing store (Redis, Postgres, etc.) needs a codec to
	// round-trip the result's Go type.
	Codec Codec

	// Hasher enables payload binding: the command payload is hashed at
	// dispatch and compared to the cached payload hash on subsequent
	// dispatches with the same key. A mismatch returns ErrPayloadMismatch.
	// Nil disables the check.
	Hasher Hasher

	// RequireKey rejects dispatches that have no idempotency key (neither
	// on the context via spark.WithIdempotencyKey nor via the spark.Keyed
	// interface) with ErrMissingIdempotencyKey. When false, keyless
	// dispatches bypass the middleware.
	RequireKey bool

	// CacheSuccessOnly stores only successful outcomes. Failed dispatches
	// release their claim and can be retried with the same key. When
	// false (default), all outcomes are cached (Stripe-style semantics).
	CacheSuccessOnly bool
}

// Idempotent returns a middleware that deduplicates command dispatches by
// idempotency key. The key is taken from the context
// (spark.WithIdempotencyKey) or, if absent, from the command itself when it
// implements spark.Keyed.
//
// Order matters: Idempotent must sit outside Retry. Otherwise the second
// retry attempt hits the cache instead of re-executing the work.
//
// Panics if cfg.Store is nil.
func Idempotent(cfg IdempotencyConfig) spark.Middleware {
	if cfg.Store == nil {
		panic("spark/middleware: IdempotencyConfig.Store is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = spark.NopLogger
	}

	return func(next spark.Next) spark.Next {
		return func(ctx context.Context, cmd spark.Command) (any, error) {
			key, ok := extractKey(ctx, cmd)
			if !ok {
				if cfg.RequireKey {
					return nil, ErrMissingIdempotencyKey
				}
				return next(ctx, cmd)
			}

			var hash uint64
			if cfg.Hasher != nil {
				h, err := cfg.Hasher.Hash(cmd)
				if err != nil {
					return nil, err
				}
				hash = h
			}

			prior, err := cfg.Store.Claim(ctx, key)
			if err != nil {
				return nil, err
			}
			if prior != nil {
				if cfg.Hasher != nil && prior.Hash != hash {
					return nil, ErrPayloadMismatch
				}
				result, err := unwrap(cfg.Codec, prior.Result)
				if err != nil {
					return nil, fmt.Errorf("spark/middleware: decode cached result: %w", err)
				}
				return result, prior.Err
			}

			// We now own the claim. If next() panics before we record an
			// outcome, the deferred Release prevents the claim from being
			// orphaned until TTL. Once next() returns we own the cleanup
			// explicitly below, so the defer becomes a no-op.
			claimed := true
			defer func() {
				if !claimed {
					return
				}
				if relErr := cfg.Store.Release(context.WithoutCancel(ctx), key); relErr != nil {
					cfg.Logger.Warn(ctx, "spark/middleware: failed to release orphan idempotency claim",
						"key", key, "command", cmd.Type(), "error", relErr.Error())
				}
			}()

			result, err := next(ctx, cmd)
			claimed = false

			// The dispatch has already happened; the cache write is best
			// effort and must not be cancelled by an upstream context
			// expiring (a timeout on the caller, the HTTP request finishing,
			// etc.). Detach the context's cancellation while preserving its
			// values.
			writeCtx := context.WithoutCancel(ctx)

			if err != nil && cfg.CacheSuccessOnly {
				if relErr := cfg.Store.Release(writeCtx, key); relErr != nil {
					cfg.Logger.Warn(ctx, "spark/middleware: failed to release idempotency claim",
						"key", key, "command", cmd.Type(), "error", relErr.Error())
				}
				return result, err
			}

			stored, wrapErr := wrap(cfg.Codec, result)
			if wrapErr != nil {
				cfg.Logger.Warn(ctx, "spark/middleware: failed to encode result for caching",
					"key", key, "command", cmd.Type(), "error", wrapErr.Error())
				if relErr := cfg.Store.Release(writeCtx, key); relErr != nil {
					cfg.Logger.Warn(ctx, "spark/middleware: failed to release idempotency claim after encode failure",
						"key", key, "command", cmd.Type(), "error", relErr.Error())
				}
				return result, err
			}
			if saveErr := cfg.Store.Save(writeCtx, key, IdempotencyResult{Result: stored, Err: err, Hash: hash}); saveErr != nil {
				cfg.Logger.Warn(ctx, "spark/middleware: failed to save idempotency result",
					"key", key, "command", cmd.Type(), "error", saveErr.Error())
			}
			return result, err
		}
	}
}

// wrap encodes v via the codec if one is configured; otherwise returns v
// as-is. Stores keep opaque blobs and never need to know about types when
// a codec is wired in.
func wrap(c Codec, v any) (any, error) {
	if c == nil {
		return v, nil
	}
	return c.Marshal(v)
}

// unwrap decodes v via the codec if one is configured and v is a byte slice;
// otherwise returns v as-is.
func unwrap(c Codec, v any) (any, error) {
	if c == nil {
		return v, nil
	}
	data, ok := v.([]byte)
	if !ok {
		return v, nil
	}
	return c.Unmarshal(data)
}

func extractKey(ctx context.Context, cmd spark.Command) (string, bool) {
	if k, ok := spark.IdempotencyKeyFromContext(ctx); ok {
		return k, true
	}
	if keyed, ok := cmd.(spark.Keyed); ok {
		if k := keyed.IdempotencyKey(); k != "" {
			return k, true
		}
	}
	return "", false
}

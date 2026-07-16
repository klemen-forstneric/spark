package middleware_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/klemen-forstneric/spark"
	"github.com/klemen-forstneric/spark/middleware"
)

var (
	errRetrySentinel = errors.New("retry: sentinel non-retryable")
	errRetryOther    = errors.New("retry: other non-retryable")
	errRetryFlaky    = errors.New("retry: transient")
)

func TestRetry_SucceedsWithoutRetry(t *testing.T) {
	calls := 0
	mw := middleware.Retry(middleware.RetryConfig{Attempts: 3, Backoff: time.Millisecond})
	next := mw(func(ctx context.Context, cmd spark.Command) (any, error) {
		calls++
		return "ok", nil
	})

	result, err := next(context.Background(), testCmd{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if result != "ok" {
		t.Fatalf("result = %v, want %q", result, "ok")
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestRetry_SucceedsAfterRetries(t *testing.T) {
	calls := 0
	mw := middleware.Retry(middleware.RetryConfig{Attempts: 5, Backoff: time.Millisecond})
	next := mw(func(ctx context.Context, cmd spark.Command) (any, error) {
		calls++
		if calls < 3 {
			return nil, errRetryFlaky
		}
		return "ok", nil
	})

	result, err := next(context.Background(), testCmd{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if result != "ok" {
		t.Fatalf("result = %v, want %q", result, "ok")
	}
	if calls != 3 {
		t.Fatalf("calls = %d, want 3 (retry stops as soon as handler succeeds)", calls)
	}
}

func TestRetry_ExhaustsAttempts_ReturnsLastError(t *testing.T) {
	calls := 0
	mw := middleware.Retry(middleware.RetryConfig{Attempts: 4, Backoff: time.Millisecond})
	wantErr := errors.New("attempt failure")
	next := mw(func(ctx context.Context, cmd spark.Command) (any, error) {
		calls++
		return nil, fmt.Errorf("call %d: %w", calls, wantErr)
	})

	_, err := next(context.Background(), testCmd{})
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want wrapped %v", err, wantErr)
	}
	if calls != 4 {
		t.Fatalf("calls = %d, want 4 (Attempts)", calls)
	}
}

func TestRetry_AttemptsLessThanOneNormalizedToOne(t *testing.T) {
	calls := 0
	mw := middleware.Retry(middleware.RetryConfig{Attempts: 0, Backoff: time.Millisecond})
	next := mw(func(ctx context.Context, cmd spark.Command) (any, error) {
		calls++
		return nil, errRetryFlaky
	})

	_, err := next(context.Background(), testCmd{})
	if !errors.Is(err, errRetryFlaky) {
		t.Fatalf("err = %v, want %v", err, errRetryFlaky)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1 (Attempts < 1 normalised to 1)", calls)
	}
}

func TestRetry_ContextCancelledDuringBackoff_Aborts(t *testing.T) {
	calls := 0
	mw := middleware.Retry(middleware.RetryConfig{Attempts: 3, Backoff: 50 * time.Millisecond})
	next := mw(func(ctx context.Context, cmd spark.Command) (any, error) {
		calls++
		return nil, errRetryFlaky
	})

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(5*time.Millisecond, cancel)

	_, err := next(ctx, testCmd{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1 (cancellation must abort before the retried call)", calls)
	}
}

func TestRetry_NonRetryableError_StopsImmediately(t *testing.T) {
	calls := 0
	mw := middleware.Retry(middleware.RetryConfig{
		Attempts:     3,
		Backoff:      time.Millisecond,
		NonRetryable: []error{errRetrySentinel},
	})
	next := mw(func(ctx context.Context, cmd spark.Command) (any, error) {
		calls++
		return nil, errRetrySentinel
	})

	_, err := next(context.Background(), testCmd{})
	if !errors.Is(err, errRetrySentinel) {
		t.Fatalf("err = %v, want %v", err, errRetrySentinel)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1 (non-retryable error must not be retried)", calls)
	}
}

func TestRetry_RetryableError_RetriesUntilAttemptsExhausted(t *testing.T) {
	calls := 0
	mw := middleware.Retry(middleware.RetryConfig{
		Attempts:     3,
		Backoff:      time.Millisecond,
		NonRetryable: []error{errRetrySentinel},
	})
	next := mw(func(ctx context.Context, cmd spark.Command) (any, error) {
		calls++
		return nil, errRetryFlaky
	})

	_, err := next(context.Background(), testCmd{})
	if !errors.Is(err, errRetryFlaky) {
		t.Fatalf("err = %v, want %v", err, errRetryFlaky)
	}
	if calls != 3 {
		t.Fatalf("calls = %d, want 3 (errors not in NonRetryable are retried)", calls)
	}
}

func TestRetry_NonRetryableMatchesThroughWrappedError(t *testing.T) {
	calls := 0
	mw := middleware.Retry(middleware.RetryConfig{
		Attempts:     3,
		Backoff:      time.Millisecond,
		NonRetryable: []error{errRetrySentinel},
	})
	next := mw(func(ctx context.Context, cmd spark.Command) (any, error) {
		calls++
		return nil, fmt.Errorf("wrapped: %w", errRetrySentinel)
	})

	_, err := next(context.Background(), testCmd{})
	if !errors.Is(err, errRetrySentinel) {
		t.Fatalf("err = %v, want wrapped %v", err, errRetrySentinel)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1 (errors.Is must match through fmt.Errorf %%w wrapping)", calls)
	}
}

func TestRetry_NonRetryableList_OnlyMatchingEntryStops(t *testing.T) {
	calls := 0
	mw := middleware.Retry(middleware.RetryConfig{
		Attempts:     3,
		Backoff:      time.Millisecond,
		NonRetryable: []error{errRetryOther, errRetrySentinel},
	})
	next := mw(func(ctx context.Context, cmd spark.Command) (any, error) {
		calls++
		return nil, errRetrySentinel
	})

	_, err := next(context.Background(), testCmd{})
	if !errors.Is(err, errRetrySentinel) {
		t.Fatalf("err = %v, want %v", err, errRetrySentinel)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1 (any match in the NonRetryable list must stop retrying)", calls)
	}
}

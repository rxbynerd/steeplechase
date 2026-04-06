package sink

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetry_ImmediateSuccess(t *testing.T) {
	r := retryConfig{Initial: 10 * time.Millisecond, Max: 100 * time.Millisecond, MaxElapsed: time.Second}
	calls := 0
	attempts, err := r.Do(context.Background(), func(context.Context) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1", calls)
	}
	if attempts != 0 {
		t.Errorf("attempts = %d, want 0", attempts)
	}
}

func TestRetry_SucceedAfterN(t *testing.T) {
	r := retryConfig{
		Initial:    1 * time.Millisecond,
		Max:        5 * time.Millisecond,
		MaxElapsed: time.Second,
		Multiplier: 2.0,
	}
	calls := 0
	attempts, err := r.Do(context.Background(), func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}
	if attempts != 2 {
		t.Errorf("attempts = %d, want 2", attempts)
	}
}

func TestRetry_Permanent(t *testing.T) {
	r := retryConfig{Initial: 1 * time.Millisecond, Max: 5 * time.Millisecond, MaxElapsed: time.Second}
	calls := 0
	permanentErr := permanent(errors.New("bad request"))
	attempts, err := r.Do(context.Background(), func(context.Context) error {
		calls++
		return permanentErr
	})
	if !isPermanent(err) {
		t.Errorf("err = %v, want permanent", err)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1 (no retry on permanent)", calls)
	}
	if attempts != 0 {
		t.Errorf("attempts = %d, want 0", attempts)
	}
}

func TestRetry_MaxElapsed(t *testing.T) {
	r := retryConfig{
		Initial:    5 * time.Millisecond,
		Max:        5 * time.Millisecond,
		MaxElapsed: 40 * time.Millisecond,
	}
	calls := 0
	start := time.Now()
	_, err := r.Do(context.Background(), func(context.Context) error {
		calls++
		return errors.New("always failing")
	})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected error")
	}
	if calls < 2 {
		t.Errorf("calls = %d, want at least 2", calls)
	}
	if elapsed > 200*time.Millisecond {
		t.Errorf("elapsed = %v, max elapsed should have bounded this", elapsed)
	}
}

func TestRetry_ContextCancel(t *testing.T) {
	r := retryConfig{
		Initial:    50 * time.Millisecond,
		Max:        50 * time.Millisecond,
		MaxElapsed: time.Second,
	}
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after first attempt fails, during the first backoff sleep.
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	_, err := r.Do(ctx, func(context.Context) error {
		return errors.New("transient")
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

func TestRetry_ContextAlreadyCancelled(t *testing.T) {
	r := retryConfig{Initial: 1 * time.Millisecond, Max: 1 * time.Millisecond, MaxElapsed: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	calls := 0
	_, err := r.Do(ctx, func(context.Context) error {
		calls++
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	if calls != 0 {
		t.Errorf("calls = %d, want 0 (ctx already cancelled)", calls)
	}
}

func TestRetry_DeadlinePropagated(t *testing.T) {
	r := retryConfig{Initial: 1 * time.Millisecond, Max: 5 * time.Millisecond, MaxElapsed: time.Second}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := r.Do(ctx, func(context.Context) error {
		return context.DeadlineExceeded
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want context.DeadlineExceeded", err)
	}
}

func TestPermanentError_Wrap(t *testing.T) {
	inner := errors.New("underlying")
	p := permanent(inner)
	if !errors.Is(p, inner) {
		t.Error("permanent(err) should wrap err")
	}
	if !isPermanent(p) {
		t.Error("isPermanent should report true for permanent error")
	}
	if isPermanent(inner) {
		t.Error("isPermanent should report false for bare error")
	}
	if permanent(nil) != nil {
		t.Error("permanent(nil) should be nil")
	}
}

package sink

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

// retryConfig describes an exponential backoff with optional full jitter.
// The zero value is not a valid configuration; use defaultRetryConfig or set
// fields explicitly. MaxElapsed bounds the total wall-clock duration that
// Do() may spend retrying; once exceeded, the most recent error is returned.
type retryConfig struct {
	Initial    time.Duration // first backoff interval
	Max        time.Duration // cap on any single backoff interval
	MaxElapsed time.Duration // total budget across all attempts (0 = unlimited)
	Multiplier float64       // per-attempt multiplier (default 2.0)
	Jitter     float64       // fractional jitter, 0..1 (default 0.2)
}

// defaultRetryConfig returns a conservative default suitable for forwarding
// OTLP to a downstream backend: ~500ms initial, 10s cap, 30s total budget.
func defaultRetryConfig() retryConfig {
	return retryConfig{
		Initial:    500 * time.Millisecond,
		Max:        10 * time.Second,
		MaxElapsed: 30 * time.Second,
		Multiplier: 2.0,
		Jitter:     0.2,
	}
}

// permanentError wraps an error to signal that Do() should not retry it, even
// if the retry budget allows. Use it for non-retryable failures such as 4xx
// HTTP responses or protocol errors.
type permanentError struct{ err error }

func permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Reason returns the label used for steeplechase_sink_failure_total{reason}.
// Satisfies the classified interface in internal/metrics.
func (e *permanentError) Reason() string { return "permanent" }

// isPermanent reports whether err (or any error it wraps) is a permanentError.
func isPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

// Do executes op with exponential backoff. It returns the number of retry
// attempts performed (0 if op succeeded on the first call) and the final
// error (nil on success). Do returns early on:
//   - ctx cancellation / deadline
//   - a permanentError from op
//   - exhaustion of MaxElapsed
//
// The op is called at least once, regardless of MaxElapsed. Backoff sleeps
// are interruptible via ctx.
func (r retryConfig) Do(ctx context.Context, op func(context.Context) error) (attempts int, err error) {
	mult := r.Multiplier
	if mult <= 0 {
		mult = 2.0
	}
	jitter := r.Jitter
	if jitter < 0 {
		jitter = 0
	}
	if jitter > 1 {
		jitter = 1
	}

	start := time.Now()
	interval := r.Initial
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	maxInterval := r.Max
	if maxInterval <= 0 {
		maxInterval = interval
	}

	for {
		if cerr := ctx.Err(); cerr != nil {
			if err == nil {
				err = cerr
			}
			return attempts, err
		}

		err = op(ctx)
		if err == nil {
			return attempts, nil
		}
		if isPermanent(err) {
			return attempts, err
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return attempts, err
		}

		// Consult the elapsed budget before scheduling another attempt.
		if r.MaxElapsed > 0 && time.Since(start) >= r.MaxElapsed {
			return attempts, err
		}

		sleep := jitterInterval(interval, jitter)
		// Cap the sleep so we don't overshoot MaxElapsed by much.
		if r.MaxElapsed > 0 {
			remaining := r.MaxElapsed - time.Since(start)
			if remaining <= 0 {
				return attempts, err
			}
			if sleep > remaining {
				sleep = remaining
			}
		}

		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return attempts, ctx.Err()
		case <-timer.C:
		}

		attempts++
		// Grow the next interval before looping.
		next := time.Duration(float64(interval) * mult)
		if next > maxInterval {
			next = maxInterval
		}
		interval = next
	}
}

// jitterInterval returns a duration within [d*(1-j), d*(1+j)] using a uniform
// random sample. A jitter of 0 returns d unchanged. Uses math/rand/v2 which is
// fine for backoff (not cryptographic).
func jitterInterval(d time.Duration, j float64) time.Duration {
	if j == 0 {
		return d
	}
	low := float64(d) * (1 - j)
	high := float64(d) * (1 + j)
	return time.Duration(low + rand.Float64()*(high-low))
}

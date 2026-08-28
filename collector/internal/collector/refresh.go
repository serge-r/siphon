package collector

import (
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"
)

// minRefreshBackoff is the first retry delay after a failed refresh. It doubles
// on every consecutive failure, capped at the component's refresh interval.
const minRefreshBackoff = 5 * time.Second

// errHolder gives atomic.Value a single concrete type to store. Storing bare
// error values panics as soon as two different implementations reach the same
// Value — say *errors.errorString from an unexpected HTTP status and
// *fmt.wrapError from a network failure.
type errHolder struct{ err error }

func loadErr(value *atomic.Value) error {
	holder, ok := value.Load().(errHolder)
	if !ok {
		return nil
	}
	return holder.err
}

// runRefreshLoop refreshes immediately, then every interval. A failed refresh is
// retried with jittered exponential backoff instead of waiting out a whole
// interval, so a transient upstream error costs seconds rather than a full cache
// TTL. The jitter keeps replicas from retrying in lockstep.
func runRefreshLoop(stopCh <-chan struct{}, interval time.Duration, logger *slog.Logger, component string, refresh func() error) {
	backoff := minRefreshBackoff
	for {
		wait := interval
		if err := refresh(); err != nil {
			wait, backoff = nextRefreshDelay(interval, backoff)
			logger.Warn("retrying refresh after failure", "component", component, "retry_in", wait, "error", err)
		} else {
			backoff = minRefreshBackoff
		}

		timer := time.NewTimer(wait)
		select {
		case <-stopCh:
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// nextRefreshDelay returns the jittered delay before the next retry and the
// backoff to apply after it. The backoff is capped at the refresh interval, so a
// retry never waits longer than an ordinary refresh would.
func nextRefreshDelay(interval time.Duration, backoff time.Duration) (wait time.Duration, next time.Duration) {
	if backoff > interval {
		backoff = interval
	}
	return withJitter(backoff), 2 * backoff
}

// withJitter trims up to 20% off d so that replicas started together spread
// their retries out instead of hammering the upstream in lockstep.
func withJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	return d - time.Duration(rand.Int64N(int64(d)/5+1))
}

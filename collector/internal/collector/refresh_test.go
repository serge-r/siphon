package collector

import (
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestNextRefreshDelay(t *testing.T) {
	tests := map[string]struct {
		interval time.Duration
		backoff  time.Duration
		wantWait time.Duration
		wantNext time.Duration
	}{
		"first retry is far shorter than the refresh interval": {
			interval: time.Hour,
			backoff:  minRefreshBackoff,
			wantWait: minRefreshBackoff,
			wantNext: 2 * minRefreshBackoff,
		},
		"backoff is capped at the refresh interval": {
			interval: 30 * time.Second,
			backoff:  time.Hour,
			wantWait: 30 * time.Second,
			wantNext: time.Minute,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			wait, next := nextRefreshDelay(test.interval, test.backoff)
			if wait > test.wantWait || wait < test.wantWait*4/5 {
				t.Fatalf("wait = %s, want at most %s and no more than 20%% below it", wait, test.wantWait)
			}
			if next != test.wantNext {
				t.Fatalf("next = %s, want %s", next, test.wantNext)
			}
		})
	}
}

func TestRunRefreshLoopRetriesBeforeTheNextInterval(t *testing.T) {
	stopCh := make(chan struct{})
	defer close(stopCh)

	const failures = 2
	recovered := make(chan struct{})
	attempts := 0

	// The interval doubles as the backoff cap, so a short one keeps the test
	// fast while still exercising the retry path: without it the loop would only
	// try again after a full interval.
	go runRefreshLoop(stopCh, 50*time.Millisecond, discardLogger(), "test", func() error {
		attempts++
		switch {
		case attempts <= failures:
			return errors.New("upstream is down")
		case attempts == failures+1:
			close(recovered)
		}
		return nil
	})

	select {
	case <-recovered:
	case <-time.After(10 * time.Second):
		t.Fatal("refresh never recovered after transient failures")
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

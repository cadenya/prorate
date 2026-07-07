// Package limitertest is an exported conformance suite that any
// prorate.Limiter implementation must pass. Run it from your backend's
// tests:
//
//	func TestConformance(t *testing.T) {
//		limitertest.Run(t, limitertest.Config{
//			NewLimiter: func(t *testing.T) prorate.Limiter { return mybackend.New() },
//		})
//	}
//
// If your backend supports a controllable clock, set Advance to move it;
// otherwise the suite falls back to real sleeps (subtests use periods of
// one second, so wall-clock cost stays low).
package limitertest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cadenya/prorate"
)

// Config configures Run.
type Config struct {
	// NewLimiter returns a fresh limiter with no prior state. Required.
	NewLimiter func(t *testing.T) prorate.Limiter
	// Advance moves the limiter's clock forward by d. If nil, the suite
	// sleeps for real.
	Advance func(t *testing.T, d time.Duration)
}

func (c Config) advance(t *testing.T, d time.Duration) {
	t.Helper()
	if c.Advance != nil {
		c.Advance(t, d)
		return
	}
	time.Sleep(d)
}

// suiteLimit is the limit used by most subtests: 5 events per second,
// burst 3, so the emission interval is 200ms.
var suiteLimit = prorate.Limit{Rate: 5, Period: time.Second, Burst: 3}

const suiteEI = 200 * time.Millisecond

// Run executes the conformance suite against cfg.NewLimiter.
func Run(t *testing.T, cfg Config) {
	if cfg.NewLimiter == nil {
		t.Fatal("limitertest: Config.NewLimiter is required")
	}
	ctx := context.Background()

	t.Run("BurstExhaustion", func(t *testing.T) {
		l := cfg.NewLimiter(t)
		for i := 0; i < suiteLimit.Burst; i++ {
			d := mustAllow(t, l, ctx, "burst", suiteLimit)
			if want := suiteLimit.Burst - i - 1; d.Remaining != want {
				t.Errorf("request %d: Remaining = %d, want %d", i+1, d.Remaining, want)
			}
			if d.RetryAfter != 0 {
				t.Errorf("request %d: RetryAfter = %v on allow, want 0", i+1, d.RetryAfter)
			}
		}
		d := mustDeny(t, l, ctx, "burst", suiteLimit)
		if d.RetryAfter <= 0 || d.RetryAfter > suiteEI {
			t.Errorf("deny RetryAfter = %v, want in (0, %v]", d.RetryAfter, suiteEI)
		}
		if d.Remaining != 0 {
			t.Errorf("deny Remaining = %d, want 0", d.Remaining)
		}
	})

	t.Run("SteadyStateRefill", func(t *testing.T) {
		l := cfg.NewLimiter(t)
		for i := 0; i < suiteLimit.Burst; i++ {
			mustAllow(t, l, ctx, "steady", suiteLimit)
		}
		mustDeny(t, l, ctx, "steady", suiteLimit)
		// One emission interval refills exactly one token.
		cfg.advance(t, suiteEI)
		mustAllow(t, l, ctx, "steady", suiteLimit)
		mustDeny(t, l, ctx, "steady", suiteLimit)
	})

	t.Run("RetryAfterDecreasesAsTimePasses", func(t *testing.T) {
		l := cfg.NewLimiter(t)
		for i := 0; i < suiteLimit.Burst; i++ {
			mustAllow(t, l, ctx, "retry", suiteLimit)
		}
		d1 := mustDeny(t, l, ctx, "retry", suiteLimit)
		cfg.advance(t, suiteEI/2)
		d2 := mustDeny(t, l, ctx, "retry", suiteLimit)
		if d2.RetryAfter >= d1.RetryAfter {
			t.Errorf("RetryAfter did not decrease: first %v, after %v elapsed %v", d1.RetryAfter, suiteEI/2, d2.RetryAfter)
		}
	})

	t.Run("ResetAfterDrainsToIdle", func(t *testing.T) {
		l := cfg.NewLimiter(t)
		d := mustAllow(t, l, ctx, "reset", suiteLimit)
		if d.ResetAfter <= 0 || d.ResetAfter > suiteEI {
			t.Errorf("ResetAfter = %v after one request, want in (0, %v]", d.ResetAfter, suiteEI)
		}
		// After the bucket fully drains, the whole burst is available again.
		cfg.advance(t, suiteLimit.Period)
		for i := 0; i < suiteLimit.Burst; i++ {
			mustAllow(t, l, ctx, "reset", suiteLimit)
		}
	})

	t.Run("KeyIsolation", func(t *testing.T) {
		l := cfg.NewLimiter(t)
		for i := 0; i < suiteLimit.Burst; i++ {
			mustAllow(t, l, ctx, "subject-a", suiteLimit)
		}
		mustDeny(t, l, ctx, "subject-a", suiteLimit)
		mustAllow(t, l, ctx, "subject-b", suiteLimit)
	})

	t.Run("AllowN", func(t *testing.T) {
		l := cfg.NewLimiter(t)
		d, err := l.AllowN(ctx, "allown", suiteLimit, 2)
		if err != nil || !d.Allowed {
			t.Fatalf("AllowN(2) = (%+v, %v), want allowed", d, err)
		}
		if d.Remaining != 1 {
			t.Errorf("Remaining after AllowN(2) = %d, want 1", d.Remaining)
		}
		d, err = l.AllowN(ctx, "allown", suiteLimit, 2)
		if err != nil || d.Allowed {
			t.Fatalf("AllowN(2) with 1 token left = (%+v, %v), want denied", d, err)
		}
		mustAllow(t, l, ctx, "allown", suiteLimit)
	})

	t.Run("NGreaterThanBurstNeverAllowed", func(t *testing.T) {
		l := cfg.NewLimiter(t)
		d, err := l.AllowN(ctx, "toobig", suiteLimit, suiteLimit.Burst+1)
		if err != nil {
			t.Fatalf("AllowN(burst+1) error: %v", err)
		}
		if d.Allowed {
			t.Fatal("AllowN(burst+1) allowed, want denied")
		}
		if d.RetryAfter >= 0 {
			t.Errorf("AllowN(burst+1) RetryAfter = %v, want negative (never satisfiable)", d.RetryAfter)
		}
		// The oversized request must not have consumed anything.
		for i := 0; i < suiteLimit.Burst; i++ {
			mustAllow(t, l, ctx, "toobig", suiteLimit)
		}
	})

	t.Run("InvalidLimitRejected", func(t *testing.T) {
		l := cfg.NewLimiter(t)
		if _, err := l.Allow(ctx, "invalid", prorate.Limit{Rate: -1, Period: time.Second, Burst: 1}); err == nil {
			t.Error("Allow with negative Rate: want error, got nil")
		}
	})

	t.Run("ConcurrentCallers", func(t *testing.T) {
		l := cfg.NewLimiter(t)
		// A long period so no refill happens during the test.
		limit := prorate.Limit{Rate: 60, Period: time.Minute, Burst: 10}
		const callers = 50
		var wg sync.WaitGroup
		var mu sync.Mutex
		allowed := 0
		for i := 0; i < callers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				d, err := l.Allow(ctx, "concurrent", limit)
				if err != nil {
					t.Errorf("concurrent Allow error: %v", err)
					return
				}
				if d.Allowed {
					mu.Lock()
					allowed++
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		if allowed != limit.Burst {
			t.Errorf("concurrent callers: %d allowed, want exactly %d", allowed, limit.Burst)
		}
	})
}

func mustAllow(t *testing.T, l prorate.Limiter, ctx context.Context, key string, limit prorate.Limit) prorate.Decision {
	t.Helper()
	d, err := l.Allow(ctx, key, limit)
	if err != nil {
		t.Fatalf("Allow(%q) error: %v", key, err)
	}
	if !d.Allowed {
		t.Fatalf("Allow(%q) denied (RetryAfter %v), want allowed", key, d.RetryAfter)
	}
	if d.Limit != limit {
		t.Fatalf("Decision.Limit = %+v, want echo of %+v", d.Limit, limit)
	}
	return d
}

func mustDeny(t *testing.T, l prorate.Limiter, ctx context.Context, key string, limit prorate.Limit) prorate.Decision {
	t.Helper()
	d, err := l.Allow(ctx, key, limit)
	if err != nil {
		t.Fatalf("Allow(%q) error: %v", key, err)
	}
	if d.Allowed {
		t.Fatalf("Allow(%q) allowed, want denied", key)
	}
	return d
}

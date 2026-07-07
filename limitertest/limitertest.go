// Package limitertest is an exported conformance suite that any
// prorate.Limiter implementation must pass. Run it from your backend's
// tests:
//
//	func TestConformance(t *testing.T) {
//		clk := limitertest.NewClock()
//		limitertest.Run(t, limitertest.Config{
//			NewLimiter: func(t *testing.T) prorate.Limiter {
//				return mybackend.New(mybackend.WithNow(clk.Now))
//			},
//			Advance: clk.AdvanceFunc(),
//		})
//	}
//
// If your backend supports a controllable clock, set Advance to move it —
// this enables the exact hand-computed GCRA sequence checks. Otherwise
// leave Advance nil and the suite falls back to real sleeps with a
// coarser limit (long emission intervals, so scheduler noise cannot flip
// a decision) and skips the exact-sequence subtest.
package limitertest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cadenya/prorate"
)

// Clock is a mutex-guarded manual clock for driving limiters with an
// injectable time source.
type Clock struct {
	mu sync.Mutex
	t  time.Time
}

// NewClock returns a Clock starting at a fixed, arbitrary instant.
func NewClock() *Clock {
	return &Clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

// Now returns the current clock time; pass it as the backend's time
// source.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

// Advance moves the clock forward by d.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// AdvanceFunc adapts Advance to Config.Advance's signature.
func (c *Clock) AdvanceFunc() func(t *testing.T, d time.Duration) {
	return func(_ *testing.T, d time.Duration) { c.Advance(d) }
}

// Config configures Run.
type Config struct {
	// NewLimiter returns a fresh limiter with no prior state. Required.
	NewLimiter func(t *testing.T) prorate.Limiter
	// Advance moves the limiter's clock forward by d. If nil, the suite
	// sleeps for real and relaxes timing-sensitive assertions.
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

// suiteLimit returns the limit used by most subtests. With a controllable
// clock: 5 events per second, burst 3 (emission interval 200ms). With
// real sleeps: the same shape stretched to a 10s period (emission
// interval 2s), so a scheduler pause of hundreds of milliseconds cannot
// refill a token mid-assertion.
func (c Config) suiteLimit() prorate.Limit {
	if c.Advance != nil {
		return prorate.Limit{Rate: 5, Period: time.Second, Burst: 3}
	}
	return prorate.Limit{Rate: 5, Period: 10 * time.Second, Burst: 3}
}

// Run executes the conformance suite against cfg.NewLimiter.
func Run(t *testing.T, cfg Config) {
	if cfg.NewLimiter == nil {
		t.Fatal("limitertest: Config.NewLimiter is required")
	}
	ctx := context.Background()
	limit := cfg.suiteLimit()
	ei := limit.EmissionInterval()

	t.Run("BurstExhaustion", func(t *testing.T) {
		l := cfg.NewLimiter(t)
		for i := 0; i < limit.Burst; i++ {
			d := mustAllow(t, l, ctx, "burst", limit)
			if want := limit.Burst - i - 1; d.Remaining != want {
				t.Errorf("request %d: Remaining = %d, want %d", i+1, d.Remaining, want)
			}
			if d.RetryAfter != 0 {
				t.Errorf("request %d: RetryAfter = %v on allow, want 0", i+1, d.RetryAfter)
			}
		}
		d := mustDeny(t, l, ctx, "burst", limit)
		if d.RetryAfter <= 0 || d.RetryAfter > ei {
			t.Errorf("deny RetryAfter = %v, want in (0, %v]", d.RetryAfter, ei)
		}
		if d.Remaining != 0 {
			t.Errorf("deny Remaining = %d, want 0", d.Remaining)
		}
	})

	t.Run("SteadyStateRefill", func(t *testing.T) {
		l := cfg.NewLimiter(t)
		for i := 0; i < limit.Burst; i++ {
			mustAllow(t, l, ctx, "steady", limit)
		}
		mustDeny(t, l, ctx, "steady", limit)
		// One emission interval refills exactly one token.
		cfg.advance(t, ei)
		mustAllow(t, l, ctx, "steady", limit)
		mustDeny(t, l, ctx, "steady", limit)
	})

	t.Run("RetryAfterDecreasesAsTimePasses", func(t *testing.T) {
		l := cfg.NewLimiter(t)
		for i := 0; i < limit.Burst; i++ {
			mustAllow(t, l, ctx, "retry", limit)
		}
		d1 := mustDeny(t, l, ctx, "retry", limit)
		cfg.advance(t, ei/2)
		d2 := mustDeny(t, l, ctx, "retry", limit)
		if d2.RetryAfter >= d1.RetryAfter {
			t.Errorf("RetryAfter did not decrease: first %v, after %v elapsed %v", d1.RetryAfter, ei/2, d2.RetryAfter)
		}
	})

	t.Run("ResetAfterDrainsToIdle", func(t *testing.T) {
		l := cfg.NewLimiter(t)
		d := mustAllow(t, l, ctx, "reset", limit)
		if d.ResetAfter <= 0 || d.ResetAfter > ei {
			t.Errorf("ResetAfter = %v after one request, want in (0, %v]", d.ResetAfter, ei)
		}
		// After the bucket fully drains, the whole burst is available again.
		cfg.advance(t, limit.Period)
		for i := 0; i < limit.Burst; i++ {
			mustAllow(t, l, ctx, "reset", limit)
		}
	})

	t.Run("KeyIsolation", func(t *testing.T) {
		l := cfg.NewLimiter(t)
		for i := 0; i < limit.Burst; i++ {
			mustAllow(t, l, ctx, "subject-a", limit)
		}
		mustDeny(t, l, ctx, "subject-a", limit)
		mustAllow(t, l, ctx, "subject-b", limit)
	})

	t.Run("AllowN", func(t *testing.T) {
		l := cfg.NewLimiter(t)
		d, err := l.AllowN(ctx, "allown", limit, 2)
		if err != nil || !d.Allowed {
			t.Fatalf("AllowN(2) = (%+v, %v), want allowed", d, err)
		}
		if d.Remaining != 1 {
			t.Errorf("Remaining after AllowN(2) = %d, want 1", d.Remaining)
		}
		d, err = l.AllowN(ctx, "allown", limit, 2)
		if err != nil || d.Allowed {
			t.Fatalf("AllowN(2) with 1 token left = (%+v, %v), want denied", d, err)
		}
		mustAllow(t, l, ctx, "allown", limit)
	})

	t.Run("NGreaterThanBurstNeverAllowed", func(t *testing.T) {
		l := cfg.NewLimiter(t)
		d, err := l.AllowN(ctx, "toobig", limit, limit.Burst+1)
		if err != nil {
			t.Fatalf("AllowN(burst+1) error: %v", err)
		}
		if d.Allowed {
			t.Fatal("AllowN(burst+1) allowed, want denied")
		}
		if d.RetryAfter >= 0 {
			t.Errorf("AllowN(burst+1) RetryAfter = %v, want negative (never satisfiable)", d.RetryAfter)
		}
		// The oversized request must not have consumed anything, and the
		// decision must report the real (idle) bucket state.
		if d.Remaining != limit.Burst {
			t.Errorf("AllowN(burst+1) on idle bucket: Remaining = %d, want %d", d.Remaining, limit.Burst)
		}
		if d.ResetAfter != 0 {
			t.Errorf("AllowN(burst+1) on idle bucket: ResetAfter = %v, want 0", d.ResetAfter)
		}
		for i := 0; i < limit.Burst; i++ {
			mustAllow(t, l, ctx, "toobig", limit)
		}
	})

	t.Run("InvalidLimitRejected", func(t *testing.T) {
		l := cfg.NewLimiter(t)
		for name, bad := range map[string]prorate.Limit{
			"NegativeRate":       {Rate: -1, Period: time.Second, Burst: 1},
			"ZeroBurst":          {Rate: 1, Period: time.Second, Burst: 0},
			"SubNanosecondEI":    {Rate: 2_000_000_000, Period: time.Second, Burst: 1},
			"ZeroPeriodNonzeroR": {Rate: 1, Period: 0, Burst: 1},
		} {
			if _, err := l.Allow(ctx, "invalid", bad); err == nil {
				t.Errorf("%s: Allow(%+v): want error, got nil", name, bad)
			}
		}
	})

	t.Run("ZeroLimitUnlimited", func(t *testing.T) {
		l := cfg.NewLimiter(t)
		for i := 0; i < 2*limit.Burst; i++ {
			d, err := l.Allow(ctx, "unlimited", prorate.Limit{})
			if err != nil || !d.Allowed {
				t.Fatalf("zero limit call %d = (%+v, %v), want allowed", i, d, err)
			}
		}
	})

	t.Run("ConcurrentCallers", func(t *testing.T) {
		l := cfg.NewLimiter(t)
		// A long period so no refill happens during the test.
		climit := prorate.Limit{Rate: 60, Period: time.Minute, Burst: 10}
		const callers = 50
		var wg sync.WaitGroup
		var mu sync.Mutex
		allowed := 0
		for i := 0; i < callers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				d, err := l.Allow(ctx, "concurrent", climit)
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
		if allowed != climit.Burst {
			t.Errorf("concurrent callers: %d allowed, want exactly %d", allowed, climit.Burst)
		}
	})

	t.Run("GCRASequence", func(t *testing.T) {
		if cfg.Advance == nil {
			t.Skip("exact GCRA sequence requires a controllable clock (Config.Advance)")
		}
		runGCRASequence(t, cfg, ctx)
	})
}

// runGCRASequence drives a hand-computed table: rate 10/min (emission
// interval 6s), burst 5. Running it against every backend proves the
// implementations of the GCRA math agree exactly.
func runGCRASequence(t *testing.T, cfg Config, ctx context.Context) {
	l := cfg.NewLimiter(t)
	limit := prorate.Limit{Rate: 10, Period: time.Minute, Burst: 5}

	steps := []struct {
		advance    time.Duration
		allowed    bool
		remaining  int
		retryAfter time.Duration
		resetAfter time.Duration
	}{
		// Burst of 5 from cold: each allow pushes the TAT out by 6s.
		{0, true, 4, 0, 6 * time.Second},
		{0, true, 3, 0, 12 * time.Second},
		{0, true, 2, 0, 18 * time.Second},
		{0, true, 1, 0, 24 * time.Second},
		{0, true, 0, 0, 30 * time.Second},
		// Bucket full: deny, one token 6s away.
		{0, false, 0, 6 * time.Second, 30 * time.Second},
		// 3s later: still denied, 3s to go.
		{3 * time.Second, false, 0, 3 * time.Second, 27 * time.Second},
		// 3 more seconds: exactly one token refilled.
		{3 * time.Second, true, 0, 0, 30 * time.Second},
		{0, false, 0, 6 * time.Second, 30 * time.Second},
		// A full minute drains everything; the whole burst is available.
		{time.Minute, true, 4, 0, 6 * time.Second},
	}
	for i, step := range steps {
		cfg.Advance(t, step.advance)
		d, err := l.Allow(ctx, "seq", limit)
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		if d.Allowed != step.allowed || d.Remaining != step.remaining ||
			d.RetryAfter != step.retryAfter || d.ResetAfter != step.resetAfter {
			t.Errorf("step %d: got {allowed:%v remaining:%d retry:%v reset:%v}, want {allowed:%v remaining:%d retry:%v reset:%v}",
				i, d.Allowed, d.Remaining, d.RetryAfter, d.ResetAfter,
				step.allowed, step.remaining, step.retryAfter, step.resetAfter)
		}
	}
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

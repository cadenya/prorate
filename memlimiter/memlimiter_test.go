package memlimiter_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cadenya/prorate"
	"github.com/cadenya/prorate/limitertest"
	"github.com/cadenya/prorate/memlimiter"
)

// fakeClock is a mutex-guarded manual clock.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func TestConformance(t *testing.T) {
	clk := newFakeClock()
	limitertest.Run(t, limitertest.Config{
		NewLimiter: func(t *testing.T) prorate.Limiter {
			return memlimiter.New(memlimiter.WithNow(clk.Now))
		},
		Advance: func(t *testing.T, d time.Duration) { clk.Advance(d) },
	})
}

// TestGCRASequence is the hand-computed table from the TDD: rate 10/min
// (emission interval 6s), burst 5.
func TestGCRASequence(t *testing.T) {
	clk := newFakeClock()
	l := memlimiter.New(memlimiter.WithNow(clk.Now))
	limit := prorate.Limit{Rate: 10, Period: time.Minute, Burst: 5}
	ctx := context.Background()

	steps := []struct {
		advance    time.Duration
		allowed    bool
		remaining  int
		retryAfter time.Duration
		resetAfter time.Duration
	}{
		// Burst of 5 from cold: each allow pushes TAT out by 6s.
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
		// A full minute drains everything; whole burst available again.
		{time.Minute, true, 4, 0, 6 * time.Second},
	}
	for i, step := range steps {
		clk.Advance(step.advance)
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

func TestAllowNSequence(t *testing.T) {
	clk := newFakeClock()
	l := memlimiter.New(memlimiter.WithNow(clk.Now))
	limit := prorate.Limit{Rate: 10, Period: time.Minute, Burst: 5}
	ctx := context.Background()

	d, err := l.AllowN(ctx, "n", limit, 3)
	if err != nil || !d.Allowed || d.Remaining != 2 {
		t.Fatalf("AllowN(3) = (%+v, %v), want allowed with 2 remaining", d, err)
	}
	d, err = l.AllowN(ctx, "n", limit, 3)
	if err != nil || d.Allowed {
		t.Fatalf("AllowN(3) with 2 left = (%+v, %v), want denied", d, err)
	}
	// Cost 3 needs one more token: 6s away.
	if d.RetryAfter != 6*time.Second {
		t.Errorf("RetryAfter = %v, want 6s", d.RetryAfter)
	}
}

func TestInvalidN(t *testing.T) {
	l := memlimiter.New()
	if _, err := l.AllowN(context.Background(), "k", prorate.Limit{Rate: 1, Period: time.Second, Burst: 1}, 0); err == nil {
		t.Error("AllowN(0): want error")
	}
}

func TestZeroLimitUnlimited(t *testing.T) {
	l := memlimiter.New()
	for i := 0; i < 100; i++ {
		d, err := l.Allow(context.Background(), "k", prorate.Limit{})
		if err != nil || !d.Allowed {
			t.Fatalf("zero limit call %d = (%+v, %v), want allowed", i, d, err)
		}
	}
}

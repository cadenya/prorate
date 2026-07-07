package redislimiter_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/cadenya/prorate"
	"github.com/cadenya/prorate/limitertest"
	"github.com/cadenya/prorate/redislimiter"
)

// fakeClock is a mutex-guarded manual clock passed to the limiter via
// WithNow so miniredis tests are deterministic.
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

func newMiniredisClient(t *testing.T) *redis.Client {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestConformance(t *testing.T) {
	clk := newFakeClock()
	limitertest.Run(t, limitertest.Config{
		NewLimiter: func(t *testing.T) prorate.Limiter {
			return redislimiter.New(newMiniredisClient(t), redislimiter.WithNow(clk.Now))
		},
		Advance: func(t *testing.T, d time.Duration) { clk.Advance(d) },
	})
}

// TestGCRASequence mirrors the memlimiter hand-computed table so both
// implementations of the math agree: rate 10/min (emission interval 6s),
// burst 5.
func TestGCRASequence(t *testing.T) {
	clk := newFakeClock()
	l := redislimiter.New(newMiniredisClient(t), redislimiter.WithNow(clk.Now))
	limit := prorate.Limit{Rate: 10, Period: time.Minute, Burst: 5}
	ctx := context.Background()

	steps := []struct {
		advance    time.Duration
		allowed    bool
		remaining  int
		retryAfter time.Duration
		resetAfter time.Duration
	}{
		{0, true, 4, 0, 6 * time.Second},
		{0, true, 3, 0, 12 * time.Second},
		{0, true, 2, 0, 18 * time.Second},
		{0, true, 1, 0, 24 * time.Second},
		{0, true, 0, 0, 30 * time.Second},
		{0, false, 0, 6 * time.Second, 30 * time.Second},
		{3 * time.Second, false, 0, 3 * time.Second, 27 * time.Second},
		{3 * time.Second, true, 0, 0, 30 * time.Second},
		{0, false, 0, 6 * time.Second, 30 * time.Second},
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

// TestKeyLayout checks the prefix + hash tag key shape lands in Redis.
func TestKeyLayout(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	clk := newFakeClock()

	l := redislimiter.New(client, redislimiter.WithNow(clk.Now))
	if _, err := l.Allow(context.Background(), "acct-1/standard", prorate.Limit{Rate: 1, Period: time.Second, Burst: 1}); err != nil {
		t.Fatal(err)
	}
	if !mr.Exists("prorate:{acct-1/standard}") {
		t.Errorf("expected key prorate:{acct-1/standard}, have %v", mr.Keys())
	}

	custom := redislimiter.New(client, redislimiter.WithNow(clk.Now), redislimiter.WithKeyPrefix("rl:"))
	if _, err := custom.Allow(context.Background(), "acct-2/standard", prorate.Limit{Rate: 1, Period: time.Second, Burst: 1}); err != nil {
		t.Fatal(err)
	}
	if !mr.Exists("rl:{acct-2/standard}") {
		t.Errorf("expected key rl:{acct-2/standard}, have %v", mr.Keys())
	}
}

// TestIdleKeyExpiry checks PEXPIRE is set so idle buckets self-evict.
func TestIdleKeyExpiry(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	clk := newFakeClock()

	l := redislimiter.New(client, redislimiter.WithNow(clk.Now))
	limit := prorate.Limit{Rate: 10, Period: time.Minute, Burst: 5}
	if _, err := l.Allow(context.Background(), "idle", limit); err != nil {
		t.Fatal(err)
	}
	ttl := mr.TTL("prorate:{idle}")
	// One token consumed → bucket drains in one emission interval (6s).
	if ttl <= 0 || ttl > 6*time.Second {
		t.Errorf("TTL = %v, want in (0, 6s]", ttl)
	}
	mr.FastForward(7 * time.Second)
	if mr.Exists("prorate:{idle}") {
		t.Error("idle key did not expire")
	}
}

// TestBackendErrorSurfaces checks a dead backend returns an error, never a
// silent deny/allow.
func TestBackendErrorSurfaces(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	mr.Close()

	l := redislimiter.New(client)
	if _, err := l.Allow(context.Background(), "k", prorate.Limit{Rate: 1, Period: time.Second, Burst: 1}); err == nil {
		t.Fatal("want error from dead backend")
	}
}

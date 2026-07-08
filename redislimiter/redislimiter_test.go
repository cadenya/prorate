package redislimiter_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"go.cadenya.com/prorate"
	"go.cadenya.com/prorate/limitertest"
	"go.cadenya.com/prorate/redislimiter"
)

func newMiniredisClient(t *testing.T) *redis.Client {
	_, client := newMiniredis(t)
	return client
}

// newMiniredis returns a fresh miniredis and a connected client, both
// cleaned up with the test.
func newMiniredis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return mr, client
}

// TestConformance runs the full suite — including the exact GCRA sequence
// shared with memlimiter, proving both implementations of the math agree
// — against miniredis with the test-only client clock.
func TestConformance(t *testing.T) {
	clk := limitertest.NewClock()
	limitertest.Run(t, limitertest.Config{
		NewLimiter: func(t *testing.T) prorate.Limiter {
			return redislimiter.New(newMiniredisClient(t), redislimiter.WithNow(clk.Now))
		},
		Advance: clk.AdvanceFunc(),
	})
}

// TestKeyLayout checks the prefix + hash tag key shape lands in Redis.
func TestKeyLayout(t *testing.T) {
	mr, client := newMiniredis(t)
	clk := limitertest.NewClock()

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
	mr, client := newMiniredis(t)
	clk := limitertest.NewClock()

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

// TestOversizedRequestLeavesNoState checks that n > Burst never writes a
// bucket key (denied without consuming).
func TestOversizedRequestLeavesNoState(t *testing.T) {
	mr, client := newMiniredis(t)
	clk := limitertest.NewClock()

	l := redislimiter.New(client, redislimiter.WithNow(clk.Now))
	d, err := l.AllowN(context.Background(), "big", prorate.Limit{Rate: 10, Period: time.Minute, Burst: 5}, 6)
	if err != nil || d.Allowed {
		t.Fatalf("AllowN(6) = (%+v, %v), want denied without error", d, err)
	}
	if d.RetryAfter >= 0 {
		t.Errorf("RetryAfter = %v, want negative", d.RetryAfter)
	}
	if mr.Exists("prorate:{big}") {
		t.Error("oversized request wrote bucket state")
	}
}

// TestBackendErrorSurfaces checks a dead backend returns an error, never a
// silent deny/allow.
func TestBackendErrorSurfaces(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	defer func() { _ = client.Close() }()
	mr.Close()

	l := redislimiter.New(client)
	if _, err := l.Allow(context.Background(), "k", prorate.Limit{Rate: 1, Period: time.Second, Burst: 1}); err == nil {
		t.Fatal("want error from dead backend")
	}
}

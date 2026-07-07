package memlimiter_test

import (
	"context"
	"testing"
	"time"

	"github.com/cadenya/prorate"
	"github.com/cadenya/prorate/limitertest"
	"github.com/cadenya/prorate/memlimiter"
)

func TestConformance(t *testing.T) {
	clk := limitertest.NewClock()
	limitertest.Run(t, limitertest.Config{
		NewLimiter: func(t *testing.T) prorate.Limiter {
			return memlimiter.New(memlimiter.WithNow(clk.Now))
		},
		Advance: clk.AdvanceFunc(),
	})
}

func TestAllowNSequence(t *testing.T) {
	clk := limitertest.NewClock()
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

//go:build redistest

package redislimiter_test

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/cadenya/prorate"
	"github.com/cadenya/prorate/limitertest"
	"github.com/cadenya/prorate/redislimiter"
)

// TestConformanceRealRedis runs the conformance suite against a real
// Redis using server time and real sleeps:
//
//	docker run --rm -p 6379:6379 redis:7
//	go test -tags redistest ./redislimiter/...
func TestConformanceRealRedis(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis at %s unavailable: %v", addr, err)
	}
	// A run-unique, per-subtest prefix isolates state from other runs and
	// subtests without ever touching keys the test did not create; bucket
	// keys self-evict via PEXPIRE.
	runID := strconv.FormatInt(time.Now().UnixNano(), 36)
	limitertest.Run(t, limitertest.Config{
		NewLimiter: func(t *testing.T) prorate.Limiter {
			return redislimiter.New(client, redislimiter.WithKeyPrefix(
				"prorate-conformance:"+runID+":"+t.Name()+":"))
		},
		// Advance nil → real sleeps against server time; the suite uses a
		// coarse limit and skips the exact-sequence subtest.
	})
}

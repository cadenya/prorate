//go:build redistest

package redislimiter_test

import (
	"context"
	"os"
	"testing"

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
	// Start from a clean slate so reruns against a shared Redis are stable.
	if err := client.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	limitertest.Run(t, limitertest.Config{
		NewLimiter: func(t *testing.T) prorate.Limiter {
			// Distinct prefix per subtest so state never leaks between them.
			return redislimiter.New(client, redislimiter.WithKeyPrefix(
				"prorate-conformance:"+t.Name()+":"))
		},
		// Advance nil → real sleeps against server time.
	})
}

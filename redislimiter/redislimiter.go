// Package redislimiter is a Redis-backed GCRA rate limiter backend for
// prorate. Each (subject, tier) key is a single Redis string holding the
// GCRA theoretical arrival time, updated by one atomic Lua script, so
// memory is O(1) per key and idle keys self-evict via PEXPIRE.
//
// Decisions use Redis server time (TIME inside the script), so client
// clock skew across pods cannot corrupt them.
package redislimiter

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"go.cadenya.com/prorate"
)

// gcraScript implements GCRA in Redis. Time is in integer microseconds.
//
//	KEYS[1] = bucket key
//	ARGV[1] = emission interval (µs)
//	ARGV[2] = burst
//	ARGV[3] = n (tokens requested)
//	ARGV[4] = now override (µs) — empty in production, set only by tests;
//	          when empty the script uses Redis server TIME.
//
// Returns {allowed(0|1), remaining, retry_after µs, reset_after µs};
// retry_after is -1 when the request can never succeed (n > burst).
var gcraScript = redis.NewScript(`
local ei = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local n = tonumber(ARGV[3])
local now
if ARGV[4] ~= '' then
  now = tonumber(ARGV[4])
else
  local t = redis.call('TIME')
  now = t[1] * 1000000 + t[2]
end

local tat = tonumber(redis.call('GET', KEYS[1]))
if not tat or tat < now then
  tat = now
end

local tolerance = burst * ei
local allowed = 0
local retry_after = 0
if n > burst then
  -- Can never succeed at this limit; deny without touching state but
  -- still report the real bucket state below.
  retry_after = -1
else
  local new_tat = tat + n * ei
  local allow_at = new_tat - tolerance
  if allow_at <= now then
    allowed = 1
    tat = new_tat
    redis.call('SET', KEYS[1], tat, 'PX', math.ceil((tat - now) / 1000))
  else
    retry_after = allow_at - now
  end
end

local remaining = math.floor((now + tolerance - tat) / ei)
if remaining < 0 then remaining = 0 end
local reset_after = tat - now
if reset_after < 0 then reset_after = 0 end

return {allowed, remaining, retry_after, reset_after}
`)

// DefaultKeyPrefix is prepended to every bucket key.
const DefaultKeyPrefix = "prorate:"

// Limiter is a Redis-backed prorate.Limiter. Construct with New.
type Limiter struct {
	client redis.UniversalClient
	prefix string
	now    func() time.Time
}

// Option configures New.
type Option func(*Limiter)

// WithKeyPrefix overrides DefaultKeyPrefix.
func WithKeyPrefix(prefix string) Option {
	return func(l *Limiter) { l.prefix = prefix }
}

// withNow overrides the time source, passing the client clock into the
// script instead of using Redis server TIME. Deliberately unexported
// (exposed to this package's tests via export_test.go): running on client
// clocks would reintroduce exactly the cross-pod skew corruption that
// server time exists to prevent.
func withNow(now func() time.Time) Option {
	return func(l *Limiter) { l.now = now }
}

// New returns a limiter backed by client, which may be a redis.Client,
// redis.ClusterClient, or redis.Ring. Context deadlines are respected and
// the limiter itself never retries — but note that go-redis retries
// failed commands MaxRetries times (default 3, with backoff) before
// returning an error. For fail-fast behavior where the interceptor's
// fail-open/closed mode reacts promptly to a Redis outage, configure the
// client with MaxRetries: -1.
func New(client redis.UniversalClient, opts ...Option) *Limiter {
	l := &Limiter{client: client, prefix: DefaultKeyPrefix}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

var _ prorate.Limiter = (*Limiter)(nil)

// Allow consumes one token for key.
func (l *Limiter) Allow(ctx context.Context, key string, limit prorate.Limit) (prorate.Decision, error) {
	return l.AllowN(ctx, key, limit, 1)
}

// AllowN consumes n tokens for key.
func (l *Limiter) AllowN(ctx context.Context, key string, limit prorate.Limit, n int) (prorate.Decision, error) {
	if err := limit.Validate(); err != nil {
		return prorate.Decision{}, err
	}
	if limit.IsZero() {
		return prorate.Decision{Allowed: true, Limit: limit}, nil
	}
	if n <= 0 {
		return prorate.Decision{}, fmt.Errorf("redislimiter: n must be > 0, got %d", n)
	}
	ei := limit.EmissionInterval().Microseconds()
	if ei <= 0 {
		return prorate.Decision{}, fmt.Errorf("redislimiter: limit %+v has sub-microsecond emission interval", limit)
	}
	nowArg := ""
	if l.now != nil {
		nowArg = strconv.FormatInt(l.now().UnixMicro(), 10)
	}
	// Each bucket is a single key, so any slot placement is correct; the
	// hash tag makes the slot a pure function of the caller key alone,
	// independent of the configured prefix.
	redisKey := l.prefix + "{" + key + "}"
	res, err := gcraScript.Run(ctx, l.client, []string{redisKey}, ei, limit.Burst, n, nowArg).Int64Slice()
	if err != nil {
		return prorate.Decision{}, fmt.Errorf("redislimiter: %w", err)
	}
	if len(res) != 4 {
		return prorate.Decision{}, fmt.Errorf("redislimiter: script returned %d values, want 4", len(res))
	}
	return prorate.Decision{
		Allowed:    res[0] == 1,
		Limit:      limit,
		Remaining:  int(res[1]),
		RetryAfter: time.Duration(res[2]) * time.Microsecond,
		ResetAfter: time.Duration(res[3]) * time.Microsecond,
	}, nil
}

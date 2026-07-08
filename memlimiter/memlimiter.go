// Package memlimiter is an in-process GCRA rate limiter backend for
// prorate. It has zero dependencies beyond the standard library and is
// intended for tests and single-replica deployments: state is per-process
// and is not shared across replicas.
//
// All keys share one mutex, so throughput tops out around single-core
// map-operation speed regardless of GOMAXPROCS. That is far beyond what a
// single-replica gRPC server needs; use redislimiter (or a sharded custom
// Limiter) where the limiter itself must scale.
package memlimiter

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.cadenya.com/prorate"
)

// sweepInterval bounds how often the lazy eviction pass runs.
const sweepInterval = time.Minute

// Limiter is an in-memory prorate.Limiter. The zero value is not usable;
// call New.
type Limiter struct {
	now func() time.Time

	mu sync.Mutex
	// tats maps key -> GCRA theoretical arrival time. A key whose TAT is
	// in the past is indistinguishable from an absent key and is evicted
	// by the periodic sweep.
	tats      map[string]time.Time
	lastSweep time.Time
}

// Option configures New.
type Option func(*Limiter)

// WithNow overrides the clock, for tests.
func WithNow(now func() time.Time) Option {
	return func(l *Limiter) { l.now = now }
}

// New returns a ready-to-use in-memory limiter.
func New(opts ...Option) *Limiter {
	l := &Limiter{
		now:  time.Now,
		tats: make(map[string]time.Time),
	}
	for _, opt := range opts {
		opt(l)
	}
	l.lastSweep = l.now()
	return l
}

var _ prorate.Limiter = (*Limiter)(nil)

// Allow consumes one token for key.
func (l *Limiter) Allow(ctx context.Context, key string, limit prorate.Limit) (prorate.Decision, error) {
	return l.AllowN(ctx, key, limit, 1)
}

// AllowN consumes n tokens for key.
func (l *Limiter) AllowN(_ context.Context, key string, limit prorate.Limit, n int) (prorate.Decision, error) {
	if err := limit.Validate(); err != nil {
		return prorate.Decision{}, err
	}
	if limit.IsZero() {
		return prorate.Decision{Allowed: true, Limit: limit}, nil
	}
	if n <= 0 {
		return prorate.Decision{}, fmt.Errorf("memlimiter: n must be > 0, got %d", n)
	}
	now := l.now()
	ei := limit.EmissionInterval()

	l.mu.Lock()
	defer l.mu.Unlock()
	l.maybeSweep(now)

	tat := l.effectiveTAT(key, now)
	if n > limit.Burst {
		// Can never succeed at this limit; deny without touching state
		// but still report the real bucket state.
		return decision(false, limit, tat, now, -1), nil
	}
	newTAT := tat.Add(time.Duration(n) * ei)
	allowAt := newTAT.Add(-time.Duration(limit.Burst) * ei)
	if allowAt.After(now) {
		return decision(false, limit, tat, now, allowAt.Sub(now)), nil
	}
	l.tats[key] = newTAT
	return decision(true, limit, newTAT, now, 0), nil
}

// effectiveTAT returns the stored TAT clamped to now. Callers must hold mu.
func (l *Limiter) effectiveTAT(key string, now time.Time) time.Time {
	if tat, ok := l.tats[key]; ok && tat.After(now) {
		return tat
	}
	return now
}

// decision computes the bookkeeping fields shared by allow and deny.
func decision(allowed bool, limit prorate.Limit, tat, now time.Time, retryAfter time.Duration) prorate.Decision {
	ei := limit.EmissionInterval()
	tolerance := time.Duration(limit.Burst) * ei
	remaining := max(int((tolerance-tat.Sub(now))/ei), 0)
	resetAfter := max(tat.Sub(now), 0)
	return prorate.Decision{
		Allowed:    allowed,
		Limit:      limit,
		Remaining:  remaining,
		RetryAfter: retryAfter,
		ResetAfter: resetAfter,
	}
}

// maybeSweep evicts idle keys (TAT in the past) at most once per
// sweepInterval. Callers must hold mu.
func (l *Limiter) maybeSweep(now time.Time) {
	if now.Sub(l.lastSweep) < sweepInterval {
		return
	}
	l.lastSweep = now
	for key, tat := range l.tats {
		if !tat.After(now) {
			delete(l.tats, key)
		}
	}
}

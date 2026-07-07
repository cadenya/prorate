// Package prorate provides protobuf-annotated, pluggable-backend rate
// limiting for gRPC services.
//
// RPCs are annotated with named rate limit tiers in the proto contract
// (see buf.build/cadenya/prorate), a Registry reads those annotations via
// protoreflection at startup, and server interceptors enforce them against
// a pluggable Limiter backend. The library never decides who the rate
// limit subject is or what their rate is — the KeyFunc and LimitFunc
// callbacks on Config own that.
package prorate

import (
	"context"
	"fmt"
	"time"
)

// Limit is a rate: Rate events per Period with Burst capacity.
//
// The zero value means "unlimited": a LimitFunc that returns the zero
// Limit exempts that subject from limiting for the request.
type Limit struct {
	// Rate is the number of events allowed per Period.
	Rate int
	// Period is the window over which Rate applies, e.g. time.Minute.
	Period time.Duration
	// Burst is the maximum number of events allowed instantaneously from
	// an idle state (>= 1); the GCRA burst tolerance.
	Burst int
}

// IsZero reports whether l is the zero value, which limiters and
// interceptors interpret as "unlimited".
func (l Limit) IsZero() bool {
	return l == Limit{}
}

// Validate returns an error if the limit is neither the zero value nor a
// well-formed rate.
func (l Limit) Validate() error {
	if l.IsZero() {
		return nil
	}
	if l.Rate <= 0 {
		return fmt.Errorf("prorate: Limit.Rate must be > 0, got %d", l.Rate)
	}
	if l.Period <= 0 {
		return fmt.Errorf("prorate: Limit.Period must be > 0, got %v", l.Period)
	}
	if l.Burst < 1 {
		return fmt.Errorf("prorate: Limit.Burst must be >= 1, got %d", l.Burst)
	}
	return nil
}

// emissionInterval is the GCRA emission interval: the steady-state spacing
// between events at exactly Rate per Period.
func (l Limit) emissionInterval() time.Duration {
	return l.Period / time.Duration(l.Rate)
}

// EmissionInterval returns the steady-state spacing between events
// (Period / Rate). Exposed for Limiter implementations.
func (l Limit) EmissionInterval() time.Duration {
	return l.emissionInterval()
}

// Decision is the outcome of a Limiter call.
type Decision struct {
	// Allowed reports whether the request may proceed.
	Allowed bool
	// Limit echoes the limit that was applied, for response headers.
	Limit Limit
	// Remaining is the best-effort number of additional single-token
	// requests that would be allowed right now.
	Remaining int
	// RetryAfter is how long the caller must wait before a request of the
	// same cost can succeed. Zero when allowed. A negative value means the
	// request can never succeed at this limit (its cost exceeds Burst).
	RetryAfter time.Duration
	// ResetAfter is the time until the bucket fully drains back to idle.
	ResetAfter time.Duration
}

// Limiter is the pluggable rate limit backend. Key is an opaque
// subject+scope string; implementations must treat it as the sharding
// unit.
//
// Errors mean "backend unavailable or broken", never "denied": a denied
// request is Decision{Allowed: false} with a nil error.
type Limiter interface {
	// Allow consumes exactly one token for key under limit.
	Allow(ctx context.Context, key string, limit Limit) (Decision, error)
	// AllowN consumes n tokens for key under limit. A request with
	// n > limit.Burst can never succeed and is denied with
	// RetryAfter < 0 without consuming anything.
	AllowN(ctx context.Context, key string, limit Limit, n int) (Decision, error)
}

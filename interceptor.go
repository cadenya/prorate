package prorate

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// FailureMode selects behavior when the Limiter backend errors.
type FailureMode int

const (
	// FailOpen allows the request through on backend errors (default).
	FailOpen FailureMode = iota
	// FailClosed rejects the request with codes.Unavailable on backend
	// errors. Unavailable, not ResourceExhausted — the caller was not
	// rate limited, the limiter was broken.
	FailClosed
)

// DenyInfo describes a rejected request, passed to Config.OnDeny.
type DenyInfo struct {
	Key        string
	FullMethod string
	Tier       string
	Decision   Decision
}

// Config configures the server interceptors.
type Config struct {
	// Registry holds the per-method policies. Required.
	Registry *Registry
	// Limiter is the backend. Required.
	Limiter Limiter

	// KeyFunc extracts the rate-limit subject (e.g. account ID) from the
	// request context. skip=true bypasses limiting for this request
	// (e.g. unauthenticated methods handled elsewhere, internal callers).
	// Return stable server-side identifiers, not raw client input: the
	// subject becomes part of the limiter bucket key, and subjects
	// containing '/', '{', or '}' can collide with or mis-shard other
	// buckets. Required.
	KeyFunc func(ctx context.Context, fullMethod string) (key string, skip bool)

	// LimitFunc maps (subject, tier) to an effective Limit. This is where
	// consumers implement plan-based multipliers, per-subject overrides,
	// etc. Called on every request — implementations should be cached and
	// cheap. Returning the zero Limit means "unlimited" for this request.
	// Required.
	LimitFunc func(ctx context.Context, key string, tier string) Limit

	// KnownTiers is the complete tier vocabulary LimitFunc understands.
	// Every tier referenced by an annotation (and DefaultTier) must appear
	// here or the interceptor constructor fails — a typo'd tier must fail
	// boot, never run unlimited. Required.
	KnownTiers []string

	// DefaultTier is applied when neither the method nor its service set
	// a tier, and to any method missing from the Registry entirely (e.g.
	// a service registered after the registry was built). Required and
	// must appear in KnownTiers: without it, an unregistered method would
	// resolve to a tier LimitFunc does not understand and run unlimited.
	DefaultTier string

	// FailureMode selects FailOpen (zero value) or FailClosed behavior on
	// Limiter errors.
	FailureMode FailureMode

	// OnDeny fires inline after a request is rejected, before the error
	// is returned. It must be fast; offload anything slow. For metrics
	// and notification pipelines.
	OnDeny func(ctx context.Context, info DenyInfo)
	// OnError fires inline when the Limiter errors, after the
	// fail-open/closed policy has been applied.
	OnError func(ctx context.Context, fullMethod string, err error)

	// DisableHeaders turns off the RateLimit-* / Retry-After response
	// metadata. Headers are emitted by default.
	DisableHeaders bool
}

// denyMessage is the stable status message returned on rate limited
// requests. Documented because clients will match on it even though they
// should use the status code and RetryInfo detail instead.
const denyMessage = "rate limit exceeded"

// validate checks required fields and the tier vocabulary. Any tier
// referenced by an annotation but absent from KnownTiers is a startup
// error.
func (c *Config) validate() error {
	if c.Registry == nil {
		return fmt.Errorf("prorate: Config.Registry is required")
	}
	if c.Limiter == nil {
		return fmt.Errorf("prorate: Config.Limiter is required")
	}
	if c.KeyFunc == nil {
		return fmt.Errorf("prorate: Config.KeyFunc is required")
	}
	if c.LimitFunc == nil {
		return fmt.Errorf("prorate: Config.LimitFunc is required")
	}
	if len(c.KnownTiers) == 0 {
		return fmt.Errorf("prorate: Config.KnownTiers is required so annotated tiers can be validated at startup")
	}
	known := make(map[string]bool, len(c.KnownTiers))
	for _, t := range c.KnownTiers {
		// The tier becomes part of the limiter bucket key (key + "/" +
		// tier); these characters would make distinct (subject, tier)
		// pairs collide or corrupt Redis Cluster hash tags.
		if strings.ContainsAny(t, "/{}") || t == "" {
			return fmt.Errorf("prorate: tier name %q is invalid: tiers must be non-empty and must not contain '/', '{', or '}'", t)
		}
		known[t] = true
	}
	// DefaultTier is required unconditionally: it also covers methods
	// missing from the Registry entirely, which validate() cannot see.
	if c.DefaultTier == "" {
		return fmt.Errorf("prorate: Config.DefaultTier is required (it applies to methods without a tier annotation and to methods not present in the Registry)")
	}
	if !known[c.DefaultTier] {
		return fmt.Errorf("prorate: DefaultTier %q is not in KnownTiers %v", c.DefaultTier, slices.Sorted(slices.Values(c.KnownTiers)))
	}
	var unknown []string
	for method, p := range c.Registry.Policies() {
		if p.Exempt || p.Tier == "" {
			continue
		}
		if !known[p.Tier] {
			unknown = append(unknown, fmt.Sprintf("%s -> %q", method, p.Tier))
		}
	}
	if len(unknown) > 0 {
		slices.Sort(unknown)
		return fmt.Errorf("prorate: annotations reference tiers not in KnownTiers %v: %v", slices.Sorted(slices.Values(c.KnownTiers)), unknown)
	}
	return nil
}

// UnaryServerInterceptor returns a unary interceptor enforcing the
// registry's policies. Install it after auth interceptors so KeyFunc can
// read authenticated identity from the context.
func UnaryServerInterceptor(cfg Config) (grpc.UnaryServerInterceptor, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := cfg.check(ctx, info.FullMethod, func(md metadata.MD) error {
			return grpc.SetHeader(ctx, md)
		}); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}, nil
}

// StreamServerInterceptor returns a stream interceptor enforcing the
// registry's policies. The limit is checked once at stream open;
// per-message limiting is out of scope in v1.
func StreamServerInterceptor(cfg Config) (grpc.StreamServerInterceptor, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := cfg.check(ss.Context(), info.FullMethod, ss.SetHeader); err != nil {
			return err
		}
		return handler(srv, ss)
	}, nil
}

// check runs the full decision path for one request: policy lookup,
// KeyFunc, LimitFunc, limiter call, headers, and error mapping. A nil
// return means the request may proceed.
func (c *Config) check(ctx context.Context, fullMethod string, setHeader func(metadata.MD) error) error {
	// Methods absent from the registry (e.g. services registered after
	// the registry was built) get the zero Policy and fall through to
	// DefaultTier below — safe by default, never silently unlimited.
	// validate() guarantees DefaultTier is set and known.
	policy, _ := c.Registry.Policy(fullMethod)
	if policy.Exempt {
		return nil
	}
	key, skip := c.KeyFunc(ctx, fullMethod)
	if skip {
		return nil
	}
	tier := policy.Tier
	if tier == "" {
		tier = c.DefaultTier
	}
	limit := c.LimitFunc(ctx, key, tier)
	if limit.IsZero() {
		return nil
	}
	dec, err := c.Limiter.Allow(ctx, key+"/"+tier, limit)
	if err != nil {
		if c.OnError != nil {
			c.OnError(ctx, fullMethod, err)
		}
		if c.FailureMode == FailClosed {
			return status.Error(codes.Unavailable, "rate limiter unavailable")
		}
		return nil
	}
	if !c.DisableHeaders {
		md := metadata.Pairs(
			"ratelimit-limit", strconv.Itoa(dec.Limit.Rate),
			"ratelimit-remaining", strconv.Itoa(dec.Remaining),
			"ratelimit-reset", strconv.FormatInt(ceilSeconds(dec.ResetAfter), 10),
		)
		if !dec.Allowed {
			md.Set("retry-after", strconv.FormatInt(ceilSeconds(dec.RetryAfter), 10))
		}
		// Header errors (e.g. headers already sent) are not a reason to
		// change the rate limit decision.
		_ = setHeader(md)
	}
	if dec.Allowed {
		return nil
	}
	if c.OnDeny != nil {
		c.OnDeny(ctx, DenyInfo{Key: key, FullMethod: fullMethod, Tier: tier, Decision: dec})
	}
	st := status.New(codes.ResourceExhausted, denyMessage)
	if dec.RetryAfter >= 0 {
		if withDetail, err := st.WithDetails(&errdetails.RetryInfo{
			RetryDelay: durationpb.New(dec.RetryAfter),
		}); err == nil {
			st = withDetail
		}
	}
	return st.Err()
}

// ceilSeconds rounds d up to whole seconds, clamping negatives to 0.
func ceilSeconds(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64(math.Ceil(d.Seconds()))
}

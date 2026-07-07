package prorate_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/cadenya/prorate"
	"github.com/cadenya/prorate/memlimiter"
)

// testRegistry builds a registry over the annotated + plain test services.
func testRegistry(t *testing.T, services ...string) *prorate.Registry {
	t.Helper()
	if services == nil {
		services = []string{"test.v1.AnnotatedService", "test.v1.PlainService"}
	}
	reg, err := prorate.FromFiles(testFiles(t), services)
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

// stubLimiter returns a canned decision or error.
type stubLimiter struct {
	dec     prorate.Decision
	err     error
	lastKey string
	calls   int
}

func (s *stubLimiter) Allow(ctx context.Context, key string, limit prorate.Limit) (prorate.Decision, error) {
	return s.AllowN(ctx, key, limit, 1)
}

func (s *stubLimiter) AllowN(_ context.Context, key string, limit prorate.Limit, _ int) (prorate.Decision, error) {
	s.calls++
	s.lastKey = key
	d := s.dec
	d.Limit = limit
	return d, s.err
}

// fakeServerTransportStream captures headers set via grpc.SetHeader.
type fakeServerTransportStream struct {
	method string
	md     metadata.MD
}

func (f *fakeServerTransportStream) Method() string { return f.method }
func (f *fakeServerTransportStream) SetHeader(md metadata.MD) error {
	f.md = metadata.Join(f.md, md)
	return nil
}
func (f *fakeServerTransportStream) SendHeader(md metadata.MD) error { return f.SetHeader(md) }
func (f *fakeServerTransportStream) SetTrailer(metadata.MD) error    { return nil }

func baseConfig(l prorate.Limiter) prorate.Config {
	return prorate.Config{
		Limiter: l,
		KeyFunc: func(ctx context.Context, fullMethod string) (string, bool) { return "acct-1", false },
		LimitFunc: func(ctx context.Context, key, tier string) prorate.Limit {
			return prorate.Limit{Rate: 10, Period: time.Minute, Burst: 5}
		},
		KnownTiers:  []string{"standard", "intensive"},
		DefaultTier: "standard",
	}
}

// invoke runs the unary interceptor for fullMethod and returns the handler
// error plus captured headers.
func invoke(t *testing.T, cfg prorate.Config, fullMethod string) (error, metadata.MD, bool) {
	t.Helper()
	interceptor, err := prorate.UnaryServerInterceptor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	sts := &fakeServerTransportStream{method: fullMethod}
	ctx := grpc.NewContextWithServerTransportStream(context.Background(), sts)
	handlerCalled := false
	_, err = interceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: fullMethod}, func(ctx context.Context, req any) (any, error) {
		handlerCalled = true
		return "ok", nil
	})
	return err, sts.md, handlerCalled
}

func TestUnaryAllowSetsHeaders(t *testing.T) {
	lim := &stubLimiter{dec: prorate.Decision{Allowed: true, Remaining: 4, ResetAfter: 5500 * time.Millisecond}}
	cfg := baseConfig(lim)
	cfg.Registry = testRegistry(t)

	err, md, handlerCalled := invoke(t, cfg, "/test.v1.AnnotatedService/Inherit")
	if err != nil || !handlerCalled {
		t.Fatalf("allow: err=%v handlerCalled=%v", err, handlerCalled)
	}
	for header, want := range map[string]string{
		"ratelimit-limit":     "10",
		"ratelimit-remaining": "4",
		"ratelimit-reset":     "6", // 5.5s rounded up
	} {
		if got := md.Get(header); len(got) != 1 || got[0] != want {
			t.Errorf("header %s = %v, want [%s]", header, got, want)
		}
	}
	if got := md.Get("retry-after"); len(got) != 0 {
		t.Errorf("retry-after present on allow: %v", got)
	}
	if lim.lastKey != "acct-1/standard" {
		t.Errorf("limiter key = %q, want acct-1/standard (subject + resolved tier)", lim.lastKey)
	}
}

func TestUnaryDeny(t *testing.T) {
	lim := &stubLimiter{dec: prorate.Decision{Allowed: false, Remaining: 0, RetryAfter: 1200 * time.Millisecond, ResetAfter: 30 * time.Second}}
	cfg := baseConfig(lim)
	cfg.Registry = testRegistry(t)
	var denied *prorate.DenyInfo
	cfg.OnDeny = func(ctx context.Context, info prorate.DenyInfo) { denied = &info }

	err, md, handlerCalled := invoke(t, cfg, "/test.v1.AnnotatedService/Intensive")
	if handlerCalled {
		t.Fatal("handler called on deny")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.ResourceExhausted {
		t.Fatalf("deny status = %v, want ResourceExhausted", err)
	}
	if st.Message() != "rate limit exceeded" {
		t.Errorf("deny message = %q, want the documented stable text", st.Message())
	}
	var retryInfo *errdetails.RetryInfo
	for _, d := range st.Details() {
		if ri, ok := d.(*errdetails.RetryInfo); ok {
			retryInfo = ri
		}
	}
	if retryInfo == nil {
		t.Fatal("RetryInfo detail missing")
	}
	if got := retryInfo.GetRetryDelay().AsDuration(); got != 1200*time.Millisecond {
		t.Errorf("RetryInfo delay = %v, want 1.2s", got)
	}
	if got := md.Get("retry-after"); len(got) != 1 || got[0] != "2" {
		t.Errorf("retry-after = %v, want [2] (1.2s rounded up)", got)
	}
	if denied == nil {
		t.Fatal("OnDeny not called")
	}
	if denied.Key != "acct-1" || denied.Tier != "intensive" || denied.FullMethod != "/test.v1.AnnotatedService/Intensive" {
		t.Errorf("DenyInfo = %+v", denied)
	}
}

func TestUnaryExemptAndSkipPassthrough(t *testing.T) {
	lim := &stubLimiter{dec: prorate.Decision{Allowed: false}}
	cfg := baseConfig(lim)
	cfg.Registry = testRegistry(t)

	// Exempt method: no limiter call, no headers.
	err, md, handlerCalled := invoke(t, cfg, "/test.v1.AnnotatedService/Health")
	if err != nil || !handlerCalled {
		t.Fatalf("exempt: err=%v handlerCalled=%v", err, handlerCalled)
	}
	if len(md) != 0 || lim.calls != 0 {
		t.Errorf("exempt: headers=%v limiterCalls=%d, want none", md, lim.calls)
	}

	// KeyFunc skip: same passthrough.
	cfg.KeyFunc = func(ctx context.Context, fullMethod string) (string, bool) { return "", true }
	err, md, handlerCalled = invoke(t, cfg, "/test.v1.AnnotatedService/Inherit")
	if err != nil || !handlerCalled || len(md) != 0 || lim.calls != 0 {
		t.Errorf("skip: err=%v handlerCalled=%v headers=%v calls=%d", err, handlerCalled, md, lim.calls)
	}
}

func TestUnaryZeroLimitIsUnlimited(t *testing.T) {
	lim := &stubLimiter{dec: prorate.Decision{Allowed: false}}
	cfg := baseConfig(lim)
	cfg.Registry = testRegistry(t)
	cfg.LimitFunc = func(ctx context.Context, key, tier string) prorate.Limit { return prorate.Limit{} }

	err, _, handlerCalled := invoke(t, cfg, "/test.v1.AnnotatedService/Inherit")
	if err != nil || !handlerCalled || lim.calls != 0 {
		t.Errorf("zero limit: err=%v handlerCalled=%v calls=%d, want passthrough", err, handlerCalled, lim.calls)
	}
}

func TestUnaryFailOpenAndClosed(t *testing.T) {
	lim := &stubLimiter{err: errors.New("redis down")}
	cfg := baseConfig(lim)
	cfg.Registry = testRegistry(t)
	var gotErr error
	cfg.OnError = func(ctx context.Context, fullMethod string, err error) { gotErr = err }

	// FailOpen (default): request goes through.
	err, md, handlerCalled := invoke(t, cfg, "/test.v1.AnnotatedService/Inherit")
	if err != nil || !handlerCalled {
		t.Fatalf("fail-open: err=%v handlerCalled=%v", err, handlerCalled)
	}
	if len(md) != 0 {
		t.Errorf("fail-open: headers %v emitted without a decision", md)
	}
	if gotErr == nil || !strings.Contains(gotErr.Error(), "redis down") {
		t.Errorf("OnError got %v", gotErr)
	}

	// FailClosed: Unavailable, not ResourceExhausted.
	cfg.FailureMode = prorate.FailClosed
	err, _, handlerCalled = invoke(t, cfg, "/test.v1.AnnotatedService/Inherit")
	if handlerCalled {
		t.Fatal("fail-closed: handler called")
	}
	if st, _ := status.FromError(err); st.Code() != codes.Unavailable {
		t.Errorf("fail-closed status = %v, want Unavailable", err)
	}
}

func TestUnaryUnregisteredMethodGetsDefaultTier(t *testing.T) {
	lim := &stubLimiter{dec: prorate.Decision{Allowed: true}}
	cfg := baseConfig(lim)
	cfg.Registry = testRegistry(t)

	err, _, handlerCalled := invoke(t, cfg, "/some.other.Service/Method")
	if err != nil || !handlerCalled {
		t.Fatalf("err=%v handlerCalled=%v", err, handlerCalled)
	}
	if lim.lastKey != "acct-1/standard" {
		t.Errorf("unregistered method limiter key = %q, want default tier applied", lim.lastKey)
	}
}

func TestUnaryDisableHeaders(t *testing.T) {
	lim := &stubLimiter{dec: prorate.Decision{Allowed: true, Remaining: 3}}
	cfg := baseConfig(lim)
	cfg.Registry = testRegistry(t)
	cfg.DisableHeaders = true

	_, md, _ := invoke(t, cfg, "/test.v1.AnnotatedService/Inherit")
	if len(md) != 0 {
		t.Errorf("headers emitted with DisableHeaders: %v", md)
	}
}

func TestConstructorValidation(t *testing.T) {
	lim := memlimiter.New()
	valid := baseConfig(lim)
	valid.Registry = testRegistry(t)
	if _, err := prorate.UnaryServerInterceptor(valid); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}

	t.Run("UnknownAnnotatedTier", func(t *testing.T) {
		cfg := baseConfig(lim)
		cfg.Registry = testRegistry(t, "test.v1.TypoService")
		_, err := prorate.UnaryServerInterceptor(cfg)
		if err == nil || !strings.Contains(err.Error(), `"intensiv"`) {
			t.Fatalf("want unknown-tier startup error naming the typo, got %v", err)
		}
	})
	t.Run("UnknownDefaultTier", func(t *testing.T) {
		cfg := baseConfig(lim)
		cfg.Registry = testRegistry(t)
		cfg.DefaultTier = "nope"
		if _, err := prorate.UnaryServerInterceptor(cfg); err == nil {
			t.Fatal("want error for DefaultTier not in KnownTiers")
		}
	})
	t.Run("DefaultTierAlwaysRequired", func(t *testing.T) {
		// Even when every registry method carries an explicit tier,
		// DefaultTier is required: it covers methods missing from the
		// registry entirely, which would otherwise run unlimited.
		cfg := baseConfig(lim)
		cfg.Registry = testRegistry(t, "test.v1.AnnotatedService")
		cfg.DefaultTier = ""
		if _, err := prorate.UnaryServerInterceptor(cfg); err == nil {
			t.Fatal("want error when DefaultTier is empty")
		}
	})
	t.Run("InvalidTierCharacters", func(t *testing.T) {
		for _, bad := range []string{"eu/standard", "st{andard", "tier}", ""} {
			cfg := baseConfig(lim)
			cfg.Registry = testRegistry(t)
			cfg.KnownTiers = append(cfg.KnownTiers, bad)
			if _, err := prorate.UnaryServerInterceptor(cfg); err == nil {
				t.Errorf("tier %q accepted, want constructor error", bad)
			}
		}
	})
	t.Run("MissingRequiredFields", func(t *testing.T) {
		for name, mutate := range map[string]func(*prorate.Config){
			"Registry":    func(c *prorate.Config) { c.Registry = nil },
			"Limiter":     func(c *prorate.Config) { c.Limiter = nil },
			"KeyFunc":     func(c *prorate.Config) { c.KeyFunc = nil },
			"LimitFunc":   func(c *prorate.Config) { c.LimitFunc = nil },
			"KnownTiers":  func(c *prorate.Config) { c.KnownTiers = nil },
			"DefaultTier": func(c *prorate.Config) { c.DefaultTier = "" },
		} {
			cfg := baseConfig(lim)
			cfg.Registry = testRegistry(t)
			mutate(&cfg)
			if _, err := prorate.UnaryServerInterceptor(cfg); err == nil {
				t.Errorf("missing %s: want constructor error", name)
			}
			if _, err := prorate.StreamServerInterceptor(cfg); err == nil {
				t.Errorf("missing %s: want stream constructor error", name)
			}
		}
	})
}

// fakeServerStream implements grpc.ServerStream for stream interceptor tests.
type fakeServerStream struct {
	grpc.ServerStream
	ctx context.Context
	md  metadata.MD
}

func (f *fakeServerStream) Context() context.Context { return f.ctx }
func (f *fakeServerStream) SetHeader(md metadata.MD) error {
	f.md = metadata.Join(f.md, md)
	return nil
}

func TestStreamCheckAtOpen(t *testing.T) {
	lim := &stubLimiter{dec: prorate.Decision{Allowed: true, Remaining: 2}}
	cfg := baseConfig(lim)
	cfg.Registry = testRegistry(t)
	interceptor, err := prorate.StreamServerInterceptor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ss := &fakeServerStream{ctx: context.Background()}
	info := &grpc.StreamServerInfo{FullMethod: "/test.v1.AnnotatedService/Watch", IsServerStream: true}
	handlerCalls := 0
	handler := func(srv any, stream grpc.ServerStream) error {
		handlerCalls++
		return nil
	}
	if err := interceptor(nil, ss, info, handler); err != nil || handlerCalls != 1 {
		t.Fatalf("stream allow: err=%v handlerCalls=%d", err, handlerCalls)
	}
	if lim.calls != 1 {
		t.Errorf("limiter called %d times, want once at stream open", lim.calls)
	}
	if lim.lastKey != "acct-1/intensive" {
		t.Errorf("stream limiter key = %q, want acct-1/intensive", lim.lastKey)
	}
	if got := ss.md.Get("ratelimit-remaining"); len(got) != 1 || got[0] != "2" {
		t.Errorf("stream headers = %v", ss.md)
	}

	// Deny at open.
	lim.dec = prorate.Decision{Allowed: false, RetryAfter: time.Second}
	if err := interceptor(nil, ss, info, handler); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("stream deny = %v, want ResourceExhausted", err)
	}
	if handlerCalls != 1 {
		t.Errorf("handler called on denied stream")
	}
}

// TestEndToEndWithMemLimiter drives the real memlimiter through the
// interceptor: burst then deny.
func TestEndToEndWithMemLimiter(t *testing.T) {
	cfg := baseConfig(memlimiter.New())
	cfg.Registry = testRegistry(t)
	cfg.LimitFunc = func(ctx context.Context, key, tier string) prorate.Limit {
		return prorate.Limit{Rate: 10, Period: time.Minute, Burst: 2}
	}
	for i := 0; i < 2; i++ {
		if err, _, _ := invoke(t, cfg, "/test.v1.AnnotatedService/Inherit"); err != nil {
			t.Fatalf("request %d: %v", i+1, err)
		}
	}
	err, md, _ := invoke(t, cfg, "/test.v1.AnnotatedService/Inherit")
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("third request = %v, want ResourceExhausted", err)
	}
	if got := md.Get("retry-after"); len(got) != 1 {
		t.Errorf("retry-after missing on deny: %v", md)
	}
}

# prorate

Protobuf-annotated, pluggable-backend rate limiting for gRPC services.

Annotate your RPCs with named rate limit tiers in the proto contract, and
prorate enforces them in a server interceptor — no codegen, no config files
that drift from the API. Modeled on how [protovalidate] works: the
annotations are a published buf module, and a Go runtime library reads them
via protoreflection at startup.

```protobuf
import "prorate/v1/prorate.proto";

service AgentService {
  option (prorate.v1.service_policy) = { default_tier: "standard" };

  rpc RunAgent(RunAgentRequest) returns (RunAgentResponse) {
    option (prorate.v1.method_policy) = { tier: "intensive" };
  }
  rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse) {
    option (prorate.v1.method_policy) = { exempt: true };
  }
}
```

- **Contract-first.** Rate limit tiers live next to the RPCs they protect,
  reviewed in the same PR as the API change.
- **Safe by default.** Unannotated methods get a default tier. A typo'd
  tier name is a **startup error**, never a silently unlimited endpoint.
- **Policy is yours.** The library never decides who the subject is or
  what their rate is — your `KeyFunc` and `LimitFunc` callbacks own that
  (plan-based multipliers, per-subject overrides, …).
- **Pluggable backends.** Redis ([GCRA], one atomic Lua script, O(1)
  memory per key) and in-memory built in; implement `Limiter` for anything
  else and validate it with the exported conformance suite.
- **Standard client signals.** `RESOURCE_EXHAUSTED` with a `RetryInfo`
  detail, plus `ratelimit-*` / `retry-after` metadata headers, so
  gRPC-JSON transcoders like Envoy surface proper HTTP 429 semantics.

## Quickstart

Install the Go module:

```
go get go.cadenya.com/prorate
```

Add the annotations to your proto build (`buf.yaml`):

```yaml
deps:
  - buf.build/cadenya-agents/prorate
```

Annotate your services (see above), regenerate, then wire up the
interceptor:

```go
import (
    "go.cadenya.com/prorate"
    "go.cadenya.com/prorate/redislimiter"
)

// 1. Register your services, then build the registry via reflection.
//    Every annotated tier is resolved here; unknown tiers fail startup.
registry, err := prorate.FromServer(srv) // srv: *grpc.Server with services registered

// 2. Configure enforcement. KeyFunc and LimitFunc are your policy.
rates := map[string]prorate.Limit{
    "standard":  {Rate: 600, Period: time.Minute, Burst: 60},
    "intensive": {Rate: 60,  Period: time.Minute, Burst: 10},
}
cfg := prorate.Config{
    Registry: registry,
    Limiter:  redislimiter.New(redisClient),
    KeyFunc: func(ctx context.Context, fullMethod string) (string, bool) {
        acct, ok := auth.AccountFromContext(ctx) // your auth
        return acct, !ok // skip=true bypasses limiting for this request
    },
    LimitFunc: func(ctx context.Context, key, tier string) prorate.Limit {
        return rates[tier] // apply plan multipliers etc. here; must be cheap
    },
    KnownTiers:  []string{"standard", "intensive"},
    DefaultTier: "standard",
}
unary, err := prorate.UnaryServerInterceptor(cfg)
stream, err := prorate.StreamServerInterceptor(cfg)
```

Install the interceptors **after** your auth interceptor so `KeyFunc` can
read the authenticated identity from the context. A runnable end-to-end
example lives in [`examples/basicserver`](examples/basicserver).

## Annotations

Module: `buf.build/cadenya-agents/prorate`, package `prorate.v1`. Both extensions
use field number **51000** on `google.protobuf.MethodOptions` /
`ServiceOptions` (no collision with protovalidate's 1159 or
grpc-gateway's numbers).

| Option | On | Fields |
|---|---|---|
| `(prorate.v1.method_policy)` | method | `tier` (string), `exempt` (bool) |
| `(prorate.v1.service_policy)` | service | `default_tier` (string) |

Resolution precedence per method: **method `tier` → service
`default_tier` → `Config.DefaultTier`**. `exempt: true` wins over
everything. Methods missing from the registry entirely (e.g. a service
registered after the registry was built, or the reflection service) get
`Config.DefaultTier`, which is why it is always required. Tier names are
free-form strings — you define the vocabulary — except that `/`, `{`, and
`}` are rejected (tiers become part of the bucket key), and every
referenced tier must appear in `Config.KnownTiers` or the interceptor
constructor errors.

## What clients see

On every limited request (allow or deny) the response carries metadata in
the style of [draft-ietf-httpapi-ratelimit-headers], which Envoy's
gRPC-JSON transcoder passes through as HTTP response headers:

| Header | Value |
|---|---|
| `ratelimit-limit` | the tier's `Rate` |
| `ratelimit-remaining` | best-effort tokens left right now |
| `ratelimit-reset` | seconds (rounded up) until the bucket is idle |
| `retry-after` | deny only: seconds (rounded up) until retry can succeed |

Denied requests get `codes.ResourceExhausted` with message
`rate limit exceeded` (stable, documented — but match on the code and the
`errdetails.RetryInfo` detail, not the text). Backend failures under
`FailClosed` get `codes.Unavailable` instead — the caller was not rate
limited, and prorate won't lie about the cause.

Set `Config.DisableHeaders` to turn the metadata off.

## GCRA in one paragraph

Both built-in backends implement the [generic cell rate algorithm][GCRA]:
the bucket state is a single timestamp (the *theoretical arrival time*),
advanced by `Period/Rate` per accepted request and compared against
`Burst × Period/Rate` of tolerance. That means O(1) memory per key, smooth
burst handling instead of window-edge spikes, and exact `RetryAfter`
values. A `Limit{Rate: 10, Period: time.Minute, Burst: 5}` allows 5
requests instantly from idle, then one every 6 seconds. The Redis backend
runs the whole decision in one atomic Lua script using **Redis server
time**, so client clock skew across pods cannot corrupt decisions, and
sets `PEXPIRE` so idle keys evict themselves.

## Failure modes

`Limiter` errors mean "backend broken", never "denied". Choose per
deployment:

- `FailOpen` (default): allow the request, fire `Config.OnError`. Right
  for abuse-protection limits where availability beats strictness.
- `FailClosed`: reject with `codes.Unavailable`, fire `OnError`. Right
  when over-admission is worse than downtime.

`OnDeny` / `OnError` hooks run **inline** — keep them fast and offload
anything slow (metrics pipelines, notification emails).

## Writing your own Limiter

Implement the two-method interface:

```go
type Limiter interface {
    Allow(ctx context.Context, key string, limit Limit) (Decision, error)
    AllowN(ctx context.Context, key string, limit Limit, n int) (Decision, error)
}
```

and prove it correct with the exported conformance suite:

```go
func TestConformance(t *testing.T) {
    clk := limitertest.NewClock()
    limitertest.Run(t, limitertest.Config{
        NewLimiter: func(t *testing.T) prorate.Limiter {
            return mybackend.New(mybackend.WithNow(clk.Now))
        },
        Advance: clk.AdvanceFunc(), // nil → real sleeps, coarser assertions
    })
}
```

The suite covers burst exhaustion, steady-state refill, `RetryAfter`
monotonicity, key isolation, `AllowN` semantics, concurrent callers, and —
when the clock is controllable — an exact hand-computed GCRA sequence that
both built-in backends also run, so every backend provably implements the
same math.

## Semantics worth knowing

- **Zero `Limit` = unlimited.** A `LimitFunc` that returns the zero value
  exempts that subject for the request — use it for subject-level
  exemptions.
- **Streams are checked once at open.** Per-message limiting is out of
  scope in v1.
- **`AllowN` costs above `Burst` never succeed**: denied with
  `RetryAfter < 0`, nothing consumed; `Remaining` and `ResetAfter` still
  report the real bucket state (the conformance suite enforces this).
- **Rates are fixed at interceptor construction.** There is no dynamic
  policy reload in v1; `LimitFunc` is the place for runtime variation
  per subject.
- **Offline tooling:** `FromFiles` / `FromFileDescriptorSet` build a
  registry from compiled descriptors (e.g. an Envoy `proto_descriptor.pb`)
  for docs generation without a running server.
- **In-memory backend is per-process.** `memlimiter` is for tests and
  single-replica deployments only.
- **Redis client retries are yours to configure.** The limiter never
  retries, but a default-configured go-redis client retries failed
  commands 3 times with backoff before surfacing an error. Set
  `MaxRetries: -1` on the client if you want the fail-open/closed policy
  to react to a Redis outage immediately.
- **KeyFunc should return stable server-side identifiers** (account IDs,
  API key IDs) — the subject becomes part of the bucket key, and raw
  client-controlled strings containing `/`, `{`, or `}` can collide with
  or mis-shard other buckets.

## Development

```
just gen         # regenerate proto code
just test        # unit tests (miniredis, no services needed)
just test-redis  # conformance against a real Redis 7
```

## License

[Apache-2.0](LICENSE).

[protovalidate]: https://github.com/bufbuild/protovalidate
[GCRA]: https://en.wikipedia.org/wiki/Generic_cell_rate_algorithm
[draft-ietf-httpapi-ratelimit-headers]: https://datatracker.ietf.org/doc/draft-ietf-httpapi-ratelimit-headers/

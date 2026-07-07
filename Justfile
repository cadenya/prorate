# Regenerate Go code from protos (options module + test fixtures).
gen:
    buf generate
    buf generate --template internal/testdata/buf.gen.yaml

# Lint protos and Go.
lint:
    buf lint
    go vet ./...

# Unit tests (miniredis-backed; no external services needed).
test:
    go test -race ./...

# Conformance suite against a real Redis (docker run --rm -p 6379:6379 redis:7).
test-redis:
    go test -race -tags redistest ./redislimiter/...

# Check proto backwards compatibility against main.
breaking:
    buf breaking --against '.git#branch=main'

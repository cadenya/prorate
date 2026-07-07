package redislimiter

// WithNow exposes the client-clock override to this package's tests only.
// It is not part of the public API: production decisions must use Redis
// server time.
var WithNow = withNow

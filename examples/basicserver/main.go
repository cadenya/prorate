// Command basicserver is a runnable example: a gRPC server whose rate
// limits come entirely from proto annotations, enforced by prorate with
// the in-memory backend.
//
//	go run ./examples/basicserver
//
// The server registers the gRPC reflection service, so grpcurl works out
// of the box (the rate limit subject is read from the x-account-id
// metadata header):
//
//	grpcurl -plaintext -H 'x-account-id: acct-1' localhost:50051 test.v1.AnnotatedService/Intensive
//
// Repeat it a few times quickly to see RESOURCE_EXHAUSTED with a
// retry-after header.
package main

import (
	"context"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"

	"go.cadenya.com/prorate"
	testv1 "go.cadenya.com/prorate/internal/testdata/gen/test/v1"
	"go.cadenya.com/prorate/memlimiter"
)

// rates is the consumer-owned tier table. In a real deployment LimitFunc
// would consult plans/entitlements; here it is a static map.
var rates = map[string]prorate.Limit{
	"standard":  {Rate: 60, Period: time.Minute, Burst: 10},
	"intensive": {Rate: 10, Period: time.Minute, Burst: 2},
}

type server struct {
	testv1.UnimplementedAnnotatedServiceServer
}

func (server) Intensive(context.Context, *testv1.IntensiveRequest) (*testv1.IntensiveResponse, error) {
	return &testv1.IntensiveResponse{}, nil
}

func (server) Inherit(context.Context, *testv1.InheritRequest) (*testv1.InheritResponse, error) {
	return &testv1.InheritResponse{}, nil
}

func (server) Health(context.Context, *testv1.HealthRequest) (*testv1.HealthResponse, error) {
	return &testv1.HealthResponse{}, nil
}

func (server) Watch(_ *testv1.WatchRequest, stream grpc.ServerStreamingServer[testv1.WatchResponse]) error {
	for i := 0; i < 3; i++ {
		if err := stream.Send(&testv1.WatchResponse{Tick: time.Now().Format(time.RFC3339)}); err != nil {
			return err
		}
		time.Sleep(200 * time.Millisecond)
	}
	return nil
}

// registerServices is the single place services are registered. Both the
// throwaway registry-building server and the real server use it, so the
// two can never drift: a service added here is automatically covered by
// the registry and validated at startup.
func registerServices(r grpc.ServiceRegistrar) {
	testv1.RegisterAnnotatedServiceServer(r, server{})
}

func main() {
	// 1. Build the policy registry by reflecting over the registered
	// services. gRPC servers take interceptors at construction, so use a
	// throwaway server for registration order; registerServices keeps it
	// in lockstep with the real one.
	preflight := grpc.NewServer()
	registerServices(preflight)
	registry, err := prorate.FromServer(preflight)
	if err != nil {
		log.Fatalf("building registry: %v", err)
	}

	// 2. Build the interceptors. A typo'd tier in any annotation fails
	// right here, at startup.
	cfg := prorate.Config{
		Registry: registry,
		Limiter:  memlimiter.New(),
		KeyFunc: func(ctx context.Context, fullMethod string) (string, bool) {
			md, _ := metadata.FromIncomingContext(ctx)
			if ids := md.Get("x-account-id"); len(ids) > 0 {
				return ids[0], false
			}
			return "anonymous", false
		},
		LimitFunc: func(ctx context.Context, key, tier string) prorate.Limit {
			return rates[tier]
		},
		KnownTiers:  []string{"standard", "intensive"},
		DefaultTier: "standard",
	}
	unary, err := prorate.UnaryServerInterceptor(cfg)
	if err != nil {
		log.Fatalf("building unary interceptor: %v", err)
	}
	stream, err := prorate.StreamServerInterceptor(cfg)
	if err != nil {
		log.Fatalf("building stream interceptor: %v", err)
	}

	// 3. The real server, with limiting installed. The reflection service
	// is not in the registry, so its methods fall through to DefaultTier —
	// safe by default.
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(unary),
		grpc.ChainStreamInterceptor(stream),
	)
	registerServices(srv)
	reflection.Register(srv)

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("basicserver listening on %s", lis.Addr())
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

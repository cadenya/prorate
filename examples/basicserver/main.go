// Command basicserver is a runnable example: a gRPC server whose rate
// limits come entirely from proto annotations, enforced by prorate with
// the in-memory backend.
//
//	go run ./examples/basicserver
//
// Then hammer it with grpcurl (the subject is read from the x-account-id
// metadata header):
//
//	grpcurl -plaintext -H 'x-account-id: acct-1' localhost:50051 test.v1.AnnotatedService/Intensive
package main

import (
	"context"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/cadenya/prorate"
	testv1 "github.com/cadenya/prorate/internal/testdata/gen/test/v1"
	"github.com/cadenya/prorate/memlimiter"
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

func main() {
	// 1. Register services first — the registry reflects over them.
	preflight := grpc.NewServer()
	testv1.RegisterAnnotatedServiceServer(preflight, server{})
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

	// 3. The real server, with limiting installed.
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(unary),
		grpc.ChainStreamInterceptor(stream),
	)
	testv1.RegisterAnnotatedServiceServer(srv, server{})

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("basicserver listening on %s", lis.Addr())
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

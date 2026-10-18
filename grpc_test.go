package cf_grpc

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	cf "github.com/caerus-framework/caerus-framework"
	cf_logs "github.com/caerus-framework/caerus-framework-logs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestClientDefaultName(t *testing.T) {
	c := NewClient(WithTarget("127.0.0.1:1"))
	if c.Name() != ClientComponentName {
		t.Fatalf("Name() = %q, want %q", c.Name(), ClientComponentName)
	}
}

func TestServerDefaultName(t *testing.T) {
	s := NewServer()
	if s.Name() != ServerComponentName {
		t.Fatalf("Name() = %q, want %q", s.Name(), ServerComponentName)
	}
}

func TestServerDefaultBind(t *testing.T) {
	s := NewServer()
	if got := s.Addr(); got != DefaultServerBind {
		t.Fatalf("Addr() = %q, want %q (must not collide with observability :9090)", got, DefaultServerBind)
	}
}

func TestNamedInstances(t *testing.T) {
	c := NewClient(WithClientName("auth"), WithTarget("127.0.0.1:1"))
	s := NewServer(WithServerName("grpc-public"))
	if c.Name() != "auth" {
		t.Fatalf("client Name() = %q", c.Name())
	}
	if s.Name() != "grpc-public" {
		t.Fatalf("server Name() = %q", s.Name())
	}
}

func TestDependenciesIncludeLogs(t *testing.T) {
	c := NewClient(WithTarget("127.0.0.1:1"))
	s := NewServer()
	for _, deps := range [][]string{c.GetDependencies(), s.GetDependencies()} {
		found := false
		for _, d := range deps {
			if d == cf_logs.ComponentName {
				found = true
			}
		}
		if !found {
			t.Fatalf("GetDependencies missing logs: %v", deps)
		}
	}
}

func TestStubHelper(t *testing.T) {
	c := NewClient(WithTarget("127.0.0.1:1"))
	got := Stub(c, func(conn grpc.ClientConnInterface) string {
		if conn == nil {
			t.Fatal("nil conn")
		}
		return "ok"
	})
	if got != "ok" {
		t.Fatalf("Stub = %q", got)
	}
}

func TestServerClientRoundTrip(t *testing.T) {
	ctx := context.Background()
	fw := cf.New()
	srv := NewServer(WithBind("127.0.0.1:0"), WithServerLogger(slog.Default()))
	if err := srv.Init(ctx, fw); err != nil {
		t.Fatalf("server Init: %v", err)
	}
	t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })

	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	srv.Register(func(g *grpc.Server) {
		healthpb.RegisterHealthServer(g, hs)
	})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Run(runCtx) }()

	deadline := time.Now().Add(2 * time.Second)
	var addr string
	for time.Now().Before(deadline) {
		addr = srv.Addr()
		if addr != "" && addr != "127.0.0.1:0" && srv.listening.Load() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if addr == "" || addr == "127.0.0.1:0" {
		t.Fatal("server did not bind")
	}

	cli := NewClient(
		WithTarget(addr),
		WithConnectTimeout(2*time.Second),
		WithClientLogger(slog.Default()),
	)
	if err := cli.Init(ctx, fw); err != nil {
		t.Fatalf("client Init: %v", err)
	}
	t.Cleanup(func() { _ = cli.Shutdown(context.Background()) })

	hc := healthpb.NewHealthClient(cli.Conn())
	resp, err := hc.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("status = %v", resp.GetStatus())
	}
	got := metricWithLabels(t, cli.Metrics(), "grpc_client_calls_total", map[string]string{
		"grpc_service": "Health",
		"method":       "Check",
		"code":         "OK",
	})
	if got != 1 {
		t.Fatalf("Check calls = %v, want 1", got)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return")
	}
}

func TestClientDegradedMode(t *testing.T) {
	cli := NewClient(
		WithTarget("127.0.0.1:1"),
		WithConnectTimeout(200*time.Millisecond),
		WithClientDegradedMode(true),
		WithClientLogger(slog.Default()),
	)
	if err := cli.Init(context.Background(), cf.New()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = cli.Shutdown(context.Background()) })
	if err := cli.Health(context.Background()); err == nil {
		t.Fatal("Health should fail when not connected and health_when_degraded=not_ready")
	}
	if cli.UnderlyingConn() != nil {
		t.Fatal("DegradedMode Init should leave no connection")
	}
}

func TestClientDegradedModeReconnects(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	cli := NewClient(
		WithTarget(addr),
		WithConnectTimeout(200*time.Millisecond),
		WithReconnectInterval(50*time.Millisecond),
		WithClientDegradedMode(true),
		WithClientLogger(slog.Default()),
	)
	if err := cli.Init(context.Background(), cf.New()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = cli.Shutdown(context.Background()) })
	if cli.UnderlyingConn() != nil {
		t.Fatal("Init reached Ready before the server was serving")
	}

	gs := grpc.NewServer()
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(gs, hs)
	go func() { _ = gs.Serve(ln) }()
	t.Cleanup(gs.Stop)

	runCtx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- cli.Run(runCtx) }()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cli.UnderlyingConn() != nil && cli.Health(context.Background()) == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if cli.UnderlyingConn() == nil {
		t.Fatal("Run did not store a connection")
	}
	if err := cli.Health(context.Background()); err != nil {
		t.Fatalf("Health after reconnect: %v", err)
	}
	hc := healthpb.NewHealthClient(cli.Conn())
	resp, err := hc.Check(context.Background(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("status = %v", resp.GetStatus())
	}
	if got := cli.reconnects.Load(); got < 1 {
		t.Fatalf("reconnects = %d, want at least 1", got)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return")
	}
}

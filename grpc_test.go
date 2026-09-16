package cf_grpc

import (
	"context"
	"log/slog"
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
}

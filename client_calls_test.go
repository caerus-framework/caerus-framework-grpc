package cf_grpc

import (
	"context"
	"log/slog"
	"testing"
	"time"

	cf "github.com/caerus-framework/caerus-framework"
	cf_observability "github.com/caerus-framework/caerus-framework-observability"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSplitGRPCMethod(t *testing.T) {
	svc, meth := splitGRPCMethod("/caerus.scans.v1.ScansCatcherService/ReportUnknownURLs")
	if svc != "ScansCatcherService" || meth != "ReportUnknownURLs" {
		t.Fatalf("split = %s %s", svc, meth)
	}
}

func TestClientInvokeNotConnectedCountsUnavailable(t *testing.T) {
	cli := NewClient(
		WithClientName("grpc-scans-catcher"),
		WithTarget("127.0.0.1:1"),
		WithConnectTimeout(200*time.Millisecond),
		WithClientDegradedMode(true),
		WithClientLogger(slog.Default()),
	)
	if err := cli.Init(context.Background(), cf.New()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = cli.Shutdown(context.Background()) })

	err := cli.Conn().Invoke(context.Background(), "/caerus.scans.v1.ScansCatcherService/ReportUnknownURLs", nil, nil)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("code = %s, err = %v", status.Code(err), err)
	}
	got := metricWithLabels(t, cli.Metrics(), "grpc_client_calls_total", map[string]string{
		"component":    "grpc-scans-catcher",
		"grpc_service": "ScansCatcherService",
		"method":       "ReportUnknownURLs",
		"code":         "Unavailable",
	})
	if got != 1 {
		t.Fatalf("calls = %v, want 1", got)
	}
	n := metricWithLabels(t, cli.Metrics(), "grpc_client_call_duration_seconds_count", map[string]string{
		"component":    "grpc-scans-catcher",
		"grpc_service": "ScansCatcherService",
		"method":       "ReportUnknownURLs",
	})
	if n != 1 {
		t.Fatalf("duration count = %v, want 1", n)
	}
}

func metricWithLabels(t *testing.T, ms []cf_observability.Metric, name string, want map[string]string) float64 {
	t.Helper()
	for _, m := range ms {
		if m.Name != name {
			continue
		}
		match := true
		for k, v := range want {
			if m.Labels[k] != v {
				match = false
				break
			}
		}
		if match {
			if m.Type != cf_observability.MetricTypeCounter {
				t.Fatalf("%s type = %v, want counter", name, m.Type)
			}
			return m.Value
		}
	}
	t.Fatalf("missing %s with labels %v", name, want)
	return 0
}

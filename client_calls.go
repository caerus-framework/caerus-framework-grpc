package cf_grpc

import (
	"strings"
	"sync"
	"time"

	cf_observability "github.com/caerus-framework/caerus-framework-observability"
	"google.golang.org/grpc/codes"
)

// callStats counts RPCs this client has finished. Unary calls also add their
// duration so a scrape can show a mean. Streams are counted when opened and
// do not add duration (the stream's later error is not a single finish time).
type callStats struct {
	mu    sync.Mutex
	calls map[callKey]uint64
	sum   map[latKey]float64
	n     map[latKey]uint64
}

type callKey struct {
	service string
	method  string
	code    string
}

type latKey struct {
	service string
	method  string
}

func newCallStats() *callStats {
	return &callStats{
		calls: map[callKey]uint64{},
		sum:   map[latKey]float64{},
		n:     map[latKey]uint64{},
	}
}

// observe records one finished attempt. seconds is ignored when withDuration
// is false. fullMethod is the gRPC path, /package.Service/Method.
func (s *callStats) observe(fullMethod string, code codes.Code, elapsed time.Duration, withDuration bool) {
	if s == nil {
		return
	}
	service, method := splitGRPCMethod(fullMethod)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls[callKey{service: service, method: method, code: code.String()}]++
	if !withDuration {
		return
	}
	key := latKey{service: service, method: method}
	s.sum[key] += elapsed.Seconds()
	s.n[key]++
}

func (s *callStats) metrics(component string) []cf_observability.Metric {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]cf_observability.Metric, 0, len(s.calls)+len(s.n)*2)
	for key, n := range s.calls {
		out = append(out, cf_observability.Metric{
			Name:  "grpc_client_calls_total",
			Help:  "Finished RPCs from this process, by peer service, method, and gRPC status code.",
			Type:  cf_observability.MetricTypeCounter,
			Value: float64(n),
			Labels: map[string]string{
				"component":    component,
				"grpc_service": key.service,
				"method":       key.method,
				"code":         key.code,
			},
		})
	}
	for key, n := range s.n {
		labels := map[string]string{
			"component":    component,
			"grpc_service": key.service,
			"method":       key.method,
		}
		out = append(out,
			cf_observability.Metric{
				Name:   "grpc_client_call_duration_seconds_sum",
				Help:   "Seconds spent in finished unary RPCs. Divide by grpc_client_call_duration_seconds_count for the mean. Streams are not included.",
				Type:   cf_observability.MetricTypeCounter,
				Value:  s.sum[key],
				Labels: labels,
			},
			cf_observability.Metric{
				Name:   "grpc_client_call_duration_seconds_count",
				Help:   "Finished unary RPCs included in grpc_client_call_duration_seconds_sum.",
				Type:   cf_observability.MetricTypeCounter,
				Value:  float64(n),
				Labels: labels,
			},
		)
	}
	return out
}

// splitGRPCMethod turns /caerus.scans.v1.ScansCatcherService/ReportUnknownURLs
// into service ScansCatcherService and method ReportUnknownURLs.
func splitGRPCMethod(full string) (service, method string) {
	full = strings.TrimPrefix(full, "/")
	svc, meth, ok := strings.Cut(full, "/")
	if !ok {
		if full == "" {
			return "unknown", "unknown"
		}
		return "unknown", full
	}
	if i := strings.LastIndex(svc, "."); i >= 0 {
		svc = svc[i+1:]
	}
	if svc == "" {
		svc = "unknown"
	}
	if meth == "" {
		meth = "unknown"
	}
	return svc, meth
}

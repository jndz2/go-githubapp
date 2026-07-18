package githubapp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gregjones/httpcache"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestClientMetrics_RateLimit(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")

	rt := ClientMetrics(meter)(roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		rec := httptest.NewRecorder()
		rec.Header().Set("X-Ratelimit-Limit", "5000")
		rec.Header().Set("X-Ratelimit-Remaining", "4999")
		rec.Header().Set("X-Ratelimit-Used", "1")
		rec.Header().Set("X-Ratelimit-Reset", "1700000000")
		rec.WriteHeader(http.StatusOK)
		return rec.Result(), nil
	}))

	req, err := http.NewRequest(http.MethodGet, "https://api.github.com/repos/jndz2/go-githubapp", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req = req.WithContext(context.WithValue(req.Context(), installationKey, int64(42)))

	if _, err := rt.RoundTrip(req); err != nil {
		t.Fatalf("unexpected error from RoundTrip: %v", err)
	}

	rm := collectMetrics(t, reader)
	attrs := attribute.NewSet(attribute.Int64("installation", 42))

	assertGauge(t, rm, MetricsKeyRateLimit, attrs, 5000)
	assertGauge(t, rm, MetricsKeyRateLimitRemaining, attrs, 4999)
	assertGauge(t, rm, MetricsKeyRateLimitUsed, attrs, 1)
	assertGauge(t, rm, MetricsKeyRateLimitReset, attrs, 1700000000)
}

func TestClientMetrics_CacheHit(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")

	rt := ClientMetrics(meter)(roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		rec := httptest.NewRecorder()
		rec.Header().Set(httpcache.XFromCache, "1")
		rec.WriteHeader(http.StatusOK)
		return rec.Result(), nil
	}))

	req, err := http.NewRequest(http.MethodGet, "https://api.github.com/repos/jndz2/go-githubapp", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}

	if _, err := rt.RoundTrip(req); err != nil {
		t.Fatalf("unexpected error from RoundTrip: %v", err)
	}

	rm := collectMetrics(t, reader)
	attrs := attribute.NewSet(attribute.Int64("installation", 0))

	assertCounter(t, rm, MetricsKeyRequestsCached, attrs, 1)
}

func collectMetrics(t *testing.T, reader sdkmetric.Reader) metricdata.ResourceMetrics {
	t.Helper()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("failed to collect metrics: %v", err)
	}
	return rm
}

func findMetric(rm metricdata.ResourceMetrics, name string) (metricdata.Metrics, bool) {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m, true
			}
		}
	}
	return metricdata.Metrics{}, false
}

func assertGauge(t *testing.T, rm metricdata.ResourceMetrics, name string, attrs attribute.Set, want int64) {
	t.Helper()

	m, ok := findMetric(rm, name)
	if !ok {
		t.Fatalf("metric %q was not recorded", name)
	}
	gauge, ok := m.Data.(metricdata.Gauge[int64])
	if !ok {
		t.Fatalf("metric %q is not an int64 gauge: %T", name, m.Data)
	}
	for _, dp := range gauge.DataPoints {
		if dp.Attributes.Equals(&attrs) {
			if dp.Value != want {
				t.Errorf("metric %q = %d, want %d", name, dp.Value, want)
			}
			return
		}
	}
	t.Fatalf("metric %q has no data point with attributes %v", name, attrs)
}

func assertCounter(t *testing.T, rm metricdata.ResourceMetrics, name string, attrs attribute.Set, want int64) {
	t.Helper()

	m, ok := findMetric(rm, name)
	if !ok {
		t.Fatalf("metric %q was not recorded", name)
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("metric %q is not an int64 sum: %T", name, m.Data)
	}
	for _, dp := range sum.DataPoints {
		if dp.Attributes.Equals(&attrs) {
			if dp.Value != want {
				t.Errorf("metric %q = %d, want %d", name, dp.Value, want)
			}
			return
		}
	}
	t.Fatalf("metric %q has no data point with attributes %v", name, attrs)
}

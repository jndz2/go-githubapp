// Copyright 2018 Palantir Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package githubapp

import (
	"context"
	"net/http"
	"strconv"

	"github.com/gregjones/httpcache"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

const (
	MetricsKeyRequestsCached = "github.requests.cached"

	MetricsKeyRateLimit          = "github.rate.limit"
	MetricsKeyRateLimitRemaining = "github.rate.remaining"
	MetricsKeyRateLimitUsed      = "github.rate.used"
	MetricsKeyRateLimitReset     = "github.rate.reset"
)

// ClientMetrics creates client middleware that records metrics about all
// requests. It also defines the metrics in the provided registry.
func ClientMetrics(meter metric.Meter) ClientMiddleware {
	if meter == nil {
		meter = noop.Meter{}
	}

	cachedCounter, _ := meter.Int64Counter(MetricsKeyRequestsCached)
	limitGauge, _ := meter.Int64Gauge(MetricsKeyRateLimit)
	remainingGauge, _ := meter.Int64Gauge(MetricsKeyRateLimitRemaining)
	usedGauge, _ := meter.Int64Gauge(MetricsKeyRateLimitUsed)
	resetGauge, _ := meter.Int64Gauge(MetricsKeyRateLimitReset)

	return func(next http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			installationID, _ := r.Context().Value(installationKey).(int64)

			res, err := next.RoundTrip(r)
			if res != nil {
				ctx := r.Context()
				attrs := metric.WithAttributes(attribute.Int64("installation", installationID))

				if res.Header.Get(httpcache.XFromCache) != "" {
					cachedCounter.Add(ctx, 1, attrs)
				}

				// Headers from https://developer.github.com/v3/#rate-limiting
				updateGaugeForHeader(ctx, res.Header, httpHeaderRateLimit, limitGauge, attrs)
				updateGaugeForHeader(ctx, res.Header, httpHeaderRateRemaining, remainingGauge, attrs)
				updateGaugeForHeader(ctx, res.Header, httpHeaderRateUsed, usedGauge, attrs)
				updateGaugeForHeader(ctx, res.Header, httpHeaderRateReset, resetGauge, attrs)
			}

			return res, err
		})
	}
}

func updateGaugeForHeader(ctx context.Context, headers http.Header, header string, gauge metric.Int64Gauge, attrs metric.RecordOption) {
	headerString := headers.Get(header)
	if headerString == "" {
		return
	}
	if headerVal, err := strconv.ParseInt(headerString, 10, 64); err == nil {
		gauge.Record(ctx, headerVal, attrs)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (fn roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return fn(r)
}

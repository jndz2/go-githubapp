// Copyright 2020 Palantir Technologies, Inc.
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
	"errors"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

const (
	MetricsKeyQueueLength   = "github.event.queued"
	MetricsKeyActiveWorkers = "github.event.workers"
	MetricsKeyEventAge      = "github.event.age"
	MetricsKeyDroppedEvents = "github.event.dropped"
)

var (
	ErrCapacityExceeded = errors.New("scheduler: capacity exceeded")
)

// Dispatch is a webhook payload and the handler that handles it.
type Dispatch struct {
	Handler EventHandler

	EventType  string
	DeliveryID string
	Payload    []byte
}

// Execute calls the Dispatch's handler with the stored arguments.
func (d Dispatch) Execute(ctx context.Context) error {
	return d.Handler.Handle(ctx, d.EventType, d.DeliveryID, d.Payload)
}

// AsyncErrorCallback is called by an asynchronous scheduler when an event
// handler returns an error or panics. The error from the handler is passed
// directly as the final argument.
//
// If the handler panics, err will be a HandlerPanicError.
type AsyncErrorCallback func(ctx context.Context, d Dispatch, err error)

// DefaultAsyncErrorCallback logs errors.
func DefaultAsyncErrorCallback(ctx context.Context, d Dispatch, err error) {
	defaultAsyncErrorCallback(ctx, d, err)
}

var defaultAsyncErrorCallback = MetricsAsyncErrorCallback(nil)

// MetricsAsyncErrorCallback logs errors and increments an error counter.
func MetricsAsyncErrorCallback(meter metric.Meter) AsyncErrorCallback {
	counter := errorCounter(meter)

	return func(ctx context.Context, d Dispatch, err error) {
		zerolog.Ctx(ctx).Error().Err(err).Msg("Unexpected error handling webhook")
		counter.Add(ctx, 1, metric.WithAttributes(attribute.String("event", d.EventType)))
	}
}

// Scheduler is a strategy for executing event handlers.
//
// The Schedule method takes a Dispatch and executes it by calling the handler
// for the payload. The execution may be asynchronous, but the scheduler must
// create a new context in this case. The dispatcher waits for Schedule to
// return before responding to GitHub, so asynchronous schedulers should only
// return errors that happen during scheduling, not during execution.
//
// Schedule may return ErrCapacityExceeded if it cannot schedule or queue new
// events at the time of the call.
type Scheduler interface {
	Schedule(ctx context.Context, d Dispatch) error
}

// SchedulerOption configures properties of a scheduler.
type SchedulerOption func(*scheduler)

// WithAsyncErrorCallback sets the error callback for an asynchronous
// scheduler. If not set, the scheduler uses DefaultAsyncErrorCallback.
func WithAsyncErrorCallback(onError AsyncErrorCallback) SchedulerOption {
	return func(s *scheduler) {
		if onError != nil {
			s.onError = onError
		}
	}
}

// WithSchedulingMetrics enables metrics reporting for schedulers.
func WithSchedulingMetrics(meter metric.Meter) SchedulerOption {
	return func(s *scheduler) {
		if meter == nil {
			meter = noop.Meter{}
		}

		_, _ = meter.Int64ObservableGauge(MetricsKeyQueueLength,
			metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
				o.Observe(int64(len(s.queue)))
				return nil
			}),
		)
		_, _ = meter.Int64ObservableGauge(MetricsKeyActiveWorkers,
			metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
				o.Observe(atomic.LoadInt64(&s.activeWorkers))
				return nil
			}),
		)

		s.eventAge, _ = meter.Int64Histogram(MetricsKeyEventAge, metric.WithUnit("ms"))
		s.dropped, _ = meter.Int64Counter(MetricsKeyDroppedEvents)
	}
}

type queueDispatch struct {
	ctx context.Context
	t   time.Time
	d   Dispatch
}

// core functionality and options for (async) schedulers
type scheduler struct {
	onError AsyncErrorCallback

	activeWorkers int64
	queue         chan queueDispatch

	eventAge metric.Int64Histogram
	dropped  metric.Int64Counter
}

func (s *scheduler) safeExecute(ctx context.Context, d Dispatch) {
	var err error
	defer func() {
		atomic.AddInt64(&s.activeWorkers, -1)
		if r := recover(); r != nil {
			err = HandlerPanicError{
				value: r,
				stack: getStack(1),
			}
		}
		if err != nil && s.onError != nil {
			s.onError(ctx, d, err)
		}
	}()

	atomic.AddInt64(&s.activeWorkers, 1)
	err = d.Execute(ctx)
}

// DefaultScheduler returns a scheduler that executes handlers in the go
// routine of the caller and returns any error.
func DefaultScheduler() Scheduler {
	return &defaultScheduler{}
}

type defaultScheduler struct{}

func (s *defaultScheduler) Schedule(ctx context.Context, d Dispatch) error {
	return d.Execute(ctx)
}

// AsyncScheduler returns a scheduler that executes handlers in new goroutines.
// Goroutines are not reused and there is no limit on the number created.
func AsyncScheduler(opts ...SchedulerOption) Scheduler {
	s := &asyncScheduler{
		scheduler: scheduler{
			onError: DefaultAsyncErrorCallback,
		},
	}
	for _, opt := range opts {
		opt(&s.scheduler)
	}
	return s
}

type asyncScheduler struct {
	scheduler
}

func (s *asyncScheduler) Schedule(ctx context.Context, d Dispatch) error {
	go s.safeExecute(context.WithoutCancel(ctx), d)
	return nil
}

// QueueAsyncScheduler returns a scheduler that executes handlers in a fixed
// number of worker goroutines. If no workers are available, events queue until
// the queue is full.
func QueueAsyncScheduler(queueSize int, workers int, opts ...SchedulerOption) Scheduler {
	if queueSize < 0 {
		panic("QueueAsyncScheduler: queue size must be non-negative")
	}
	if workers < 1 {
		panic("QueueAsyncScheduler: worker count must be positive")
	}

	s := &queueScheduler{
		scheduler: scheduler{
			onError: DefaultAsyncErrorCallback,
			queue:   make(chan queueDispatch, queueSize),
		},
	}
	for _, opt := range opts {
		opt(&s.scheduler)
	}

	for range workers {
		go func() {
			for d := range s.queue {
				if s.eventAge != nil {
					s.eventAge.Record(d.ctx, time.Since(d.t).Milliseconds())
				}
				s.safeExecute(d.ctx, d.d)
			}
		}()
	}

	return s
}

type queueScheduler struct {
	scheduler
}

func (s *queueScheduler) Schedule(ctx context.Context, d Dispatch) error {
	select {
	case s.queue <- queueDispatch{ctx: context.WithoutCancel(ctx), t: time.Now(), d: d}:
	default:
		if s.dropped != nil {
			s.dropped.Add(ctx, 1)
		}
		return ErrCapacityExceeded
	}
	return nil
}

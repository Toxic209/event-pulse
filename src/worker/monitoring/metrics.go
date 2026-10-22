package monitoring

import (
	"time"
	
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	eventsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "eventpulse_events_processed_total",
		Help: "Total number of successfully processed events.",
	})

	eventsFailed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "eventpulse_events_failed_total",
		Help: "Total number of failed event processing attempts.",
	})

	eventProcessingDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "eventpulse_event_processing_duration_seconds",
		Help:    "Time taken to process an event, in seconds.",
		Buckets: prometheus.DefBuckets,
	})

	eventsRetried = promauto.NewCounter(prometheus.CounterOpts{
		Name: "eventpulse_events_retried_total",
		Help: "Total number of event retry attempts.",
	})

	eventsInFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "eventpulse_events_in_flight",
		Help: "Number of events currently being processed.",
	})
)

func RecordProcessedEvent() {
	eventsProcessed.Inc()
}

func RecordFailedEvents() {
	eventsFailed.Inc()
}

func RecordRetriedEvents() {
	eventsRetried.Inc()
}

func RecordProcessingDuration(start time.Time) {
	eventProcessingDuration.Observe(time.Since(start).Seconds())
}

func IncEventsInFlight() {
	eventsInFlight.Inc()
}

func DecEventsInFlight() {
	eventsInFlight.Dec()
}
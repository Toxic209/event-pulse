package monitoring

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	EventsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "eventpulse_events_processed_total",
		Help: "Total number of successfully processed events.",
	})

	EventsFailed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "eventpulse_events_failed_total",
		Help: "Total number of failed event processing attempts.",
	})

	EventProcessingDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "eventpulse_event_processing_duration_seconds",
		Help:    "Time taken to process an event, in seconds.",
		Buckets: prometheus.DefBuckets,
	})

	EventsRetried = promauto.NewCounter(prometheus.CounterOpts{
		Name: "eventpulse_events_retried_total",
		Help: "Total number of event retry attempts.",
	})
)

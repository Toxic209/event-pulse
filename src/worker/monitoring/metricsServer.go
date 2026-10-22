package monitoring

import (
	"net/http"
	"log"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func StartMetricServer() {
	http.Handle("/metrics", promhttp.Handler());

	log.Println("Metrics Server running on :9091");

	err := http.ListenAndServe(":9091", nil);

	log.Printf("Metrics server stopped: %v", err)

	if err != nil {
		log.Fatal(err);
	}
}
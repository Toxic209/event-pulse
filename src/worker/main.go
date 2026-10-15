package main

import (
	"log/slog"
	"os"

	"github.com/Toxic209/event-pulse/src/worker/internals/redis"
	"github.com/Toxic209/event-pulse/src/worker/internals/streams"
	"github.com/Toxic209/event-pulse/src/worker/internals/worker"
	"github.com/Toxic209/event-pulse/src/worker/monitoring"
)

func main() {

	logger := slog.New(
		slog.NewJSONHandler(os.Stdout, nil),
	)

		redis := redis.NewRedisClient();

		streams.EnsureGroupCreation(redis);

		err := worker.StartWorker(redis, logger);

		go monitoring.StartMetricServer();
		
		if err != nil {
			logger.Error("Unable to Start Worker", "error", err);
			os.Exit(1);
		}
	
}

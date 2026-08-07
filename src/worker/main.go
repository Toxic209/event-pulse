package main

import (
	"fmt"
	"github.com/Toxic209/event-pulse/src/worker/internals/redis"
	"github.com/Toxic209/event-pulse/src/worker/internals/streams"
	"github.com/Toxic209/event-pulse/src/worker/internals/worker"
)

func main() {
	fmt.Println("Go worker running...")

		redis := redis.NewRedisClient()

		streams.EnsureGroupCreation(redis)

		worker.StartWorker((redis))
	
}

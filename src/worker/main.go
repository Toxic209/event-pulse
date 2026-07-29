package main

import (
	"fmt"
	"log"
	"os"

	"github.com/Toxic209/event-pulse/src/worker/internals/redis"
	"github.com/Toxic209/event-pulse/src/worker/internals/streams"
)

func main() {
	fmt.Println("Go worker running...");

	for {

		redis := redis.NewRedisClient();

		streams.EnsureGroupCreation(redis);

		consumerName := "worker-1"
		if len(os.Args) >= 3 {
			consumerName = os.Args[2];
		}

		err := streams.FetchEvent(redis, "event-processors", consumerName);

		if err != nil {
			log.Println(err)
		}
	}
}

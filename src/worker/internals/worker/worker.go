package worker

import (
	"os"
	"log"
	"github.com/redis/go-redis/v9"
	"github.com/Toxic209/event-pulse/src/worker/internals/streams"
)

func StartWorker(redis *redis.Client) {
	consumerName := "worker-1"
	if len(os.Args) >= 2 {
		consumerName = os.Args[1]
	}

	err := streams.FetchEvent(redis, "event-processors", consumerName)

	if err != nil {
		log.Println(err)
	}
}

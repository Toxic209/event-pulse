package worker

import (
	"fmt"
	"log"
	"os"

	"github.com/Toxic209/event-pulse/src/worker/internals/postgres"
	"github.com/Toxic209/event-pulse/src/worker/internals/streams"
	"github.com/redis/go-redis/v9"
)

func StartWorker(redis *redis.Client) {

	db, err := postgres.Connectpg()
	repo := postgres.NewEventRepo(db)
	
	for {
		consumerName := "worker-1"
		if len(os.Args) >= 2 {
			consumerName = os.Args[1]
		}


		if err != nil {
			fmt.Println("Postgres connection failed!")
		}

		err = streams.FetchEvent(redis, "event-processors", consumerName, &repo)

		if err != nil {
			log.Println(err)
		}
	}
}

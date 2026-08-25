package worker

import (
	"fmt"
	"log"
	"os"

	"github.com/Toxic209/event-pulse/src/worker/internals/postgres"
	"github.com/Toxic209/event-pulse/src/worker/internals/streams"
	"github.com/redis/go-redis/v9"
)

func StartWorker(client *redis.Client) error {

	consumerGroup := "event-processors"

	db, err := postgres.Connectpg()

	if err != nil {
		fmt.Println("Postgres connection failed!")
		return err
	}

	repo := postgres.NewEventRepo(db)

	jobs := make(chan redis.XMessage)

	processorBound := 10

	cursorStartForAutoClaim := "0-0"

	pendingEvents, err := repo.RecoverPending()
	if err != nil {
		log.Println(err)
		return err
	}
	fmt.Println("Pending events recovered:", len(pendingEvents))

	err = streams.AddPendingEvents(pendingEvents, client)
	if err != nil {
		log.Println(err)
		return err
	}

	err = streams.AddDeadEventsToDLQ(&repo, client)
	if err != nil {
		log.Println(err)
		return err
	}

	for i := 0; i < processorBound; i++ {
		go func() {
			for msg := range jobs {
				streams.ProcessEvent(client, &repo, msg, consumerGroup)
			}
		}()
	}

	for {
		consumerName := "worker-1"
		if len(os.Args) >= 2 {
			consumerName = os.Args[1]
		}

		fetchedStreams, err := streams.FetchEvent(client, consumerGroup, consumerName, &repo)

		if err != nil {
			log.Println(err)
			return err
		}

		for _, stream := range fetchedStreams {
			for _, msg := range stream.Messages {
				jobs <- msg
			}
		}

		//====Auto Claim pending events from the PEL====
		for {
			pendingMessages, nextStart, err := streams.AutoClaimPendingEvents(client, consumerName, consumerGroup, cursorStartForAutoClaim)

			if err != nil {
				log.Println(err)
				return err
			}

			for _, msg := range pendingMessages {
				jobs <- msg
			}

			cursorStartForAutoClaim = nextStart

			if cursorStartForAutoClaim == "0-0" {
				break
			}
		}
	}
}

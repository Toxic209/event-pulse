package streams

import (
	"context"
	"fmt"
	"log"

	"github.com/Toxic209/event-pulse/src/worker/internals/eventHandlers"
	"github.com/Toxic209/event-pulse/src/worker/internals/postgres"
	"github.com/redis/go-redis/v9"
)

func EnsureGroupCreation(client *redis.Client) error {
	return client.XGroupCreateMkStream(
		context.Background(),
		"event", "event-processors", "$",
	).Err()
}

func FetchEvent(client *redis.Client, processorGroup string, consumerName string, repo *postgres.EventRepo) error {
	streams, error := client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group:    processorGroup,
		Consumer: consumerName,
		Streams:  []string{"event", ">"},
		Count:    1,
		Block:    0,
		NoAck:    false,
		Claim:    0,
	}).Result()

	if error != nil {
		return error
	}

	for _, stream := range streams {
		for _, msg := range stream.Messages {

			payload, ok := msg.Values["payload"].(string)

			if !ok {
				return fmt.Errorf("invalid payload")
			}

			eventId, ok := msg.Values["eventId"].(string)
			if !ok {
				return fmt.Errorf("eventId is missing or not a string")
			}

			switch msg.Values["eventType"] {

			case "email":
				err := eventhandlers.EmailHandler(payload, eventId)

				if err == nil {

					if err := repo.MarkComplete(eventId); err != nil {
						log.Println(err)
						continue
					}

				} else {
					log.Println(err)

					if err := repo.MarkFailed(eventId); err != nil {
						log.Println(err)
						continue
					}

				}
				_, err = client.XAck(context.Background(), "event", "event-processors", msg.ID).Result()
				if err != nil {
					fmt.Println(err)
				}

			case "payment":
				err := eventhandlers.PaymentHandler(payload)
				if err == nil {
					if err := repo.MarkComplete(eventId); err != nil {
						log.Println(err)
						continue
					}
				} else {
					log.Println(err);

					if err := repo.MarkFailed(eventId); err != nil {
						log.Println(err)
						continue
					}
				}
				_, err = client.XAck(context.Background(), "event", "event-processors", msg.ID).Result()
				if err != nil {
					fmt.Println(err)
				}
			}
		}
	}

	return nil
}

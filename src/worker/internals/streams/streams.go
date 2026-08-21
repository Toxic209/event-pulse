package streams

import (
	"context"
	"encoding/json"
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

func AddPendingEvents(pendingEvents []postgres.Events, client *redis.Client) error {

	for _, event := range pendingEvents {
		payloadJson, err := json.Marshal(event.Payload)

		if err != nil {
			fmt.Println("Error: ", err)
		}

		_, err = client.XAdd(context.Background(), &redis.XAddArgs{
			Stream: "event",
			Values: map[string]any{
				"eventId":   event.ID,
				"eventType": event.EventType,
				"payload":   payloadJson,
				"status":    event.Status,
			},
		}).Result()

		if err != nil {
			fmt.Println("error: ", err)
			return err
		}
	}
	return nil
}

func FetchEvent(
	client *redis.Client,
	processorGroup string,
	consumerName string,
	repo *postgres.EventRepo,
) ([]redis.XStream, error) {
	streams, err := client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group:    processorGroup,
		Consumer: consumerName,
		Streams:  []string{"event", ">"},
		Count:    10,
		Block:    0,
		NoAck:    false,
		Claim:    0,
	}).Result()

	if err != nil {
		log.Println(err)
		return []redis.XStream{}, err
	}

	return streams, nil
}

func ProcessEvent(client *redis.Client, repo *postgres.EventRepo, msg redis.XMessage, processorGroup string) error {

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
				return err
			}

		} else {
			failedEvent := msg.Values["status"] == "FAILED"
			if failedEvent {
				err := repo.IncrementRetryCount(eventId)

				if err != nil {
					fmt.Println(err)
					return err
				}
			}

			log.Println(err)

			if err := repo.MarkFailed(eventId); err != nil {
				log.Println(err)
				return err
			}

		}

	case "payment":
		err := eventhandlers.PaymentHandler(payload)
		if err == nil {
			if err := repo.MarkComplete(eventId); err != nil {
				log.Println(err)
				return err
			}
		} else {
			failedEvent := msg.Values["status"] == "FAILED"
			if failedEvent {
				err := repo.IncrementRetryCount(eventId)

				if err != nil {
					fmt.Println(err)
					return err
				}
			}

			log.Println(err)

			if err := repo.MarkFailed(eventId); err != nil {
				log.Println(err)
				return err
			}
		}
	default:
		return fmt.Errorf("unknown event type: %v", msg.Values["eventType"])

	}
	
	_, err := client.XAck(context.Background(), "event", processorGroup, msg.ID).Result()
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func AddDeadEventsToDLQ(repo *postgres.EventRepo, client *redis.Client) error {
	deadEvents, err := repo.FetchDeadEvents()
	if err != nil {
		fmt.Println(err)
		return err
	}

	for _, deadEvent := range deadEvents {
		payloadjson, err := json.Marshal(deadEvent.Payload)
		if err != nil {
			fmt.Println("JSON Marshall Error: ", err)
		}

		_, err = client.XAdd(context.Background(), &redis.XAddArgs{
			Stream: "event-dlq",
			Values: map[string]any{
				"eventId":   deadEvent.ID,
				"eventType": deadEvent.EventType,
				"payload":   payloadjson,
				"status":    deadEvent.Status,
			},
		}).Result()
		if err != nil {
			fmt.Println(err)
			return err
		}
	}

	return nil
}

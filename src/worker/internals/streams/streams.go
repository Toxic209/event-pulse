package streams

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/Toxic209/event-pulse/src/worker/internals/eventHandlers"
	"github.com/Toxic209/event-pulse/src/worker/internals/postgres"
)


func EnsureGroupCreation(client *redis.Client) error {
	return client.XGroupCreateMkStream(
		context.Background(), 
		"event", "event-processors", "$",
	).Err();
}

func FetchEvent(client *redis.Client, processorGroup string, consumerName string, repo *postgres.EventRepo) error {
	streams, error := client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group: processorGroup,
		Consumer: consumerName,
		Streams: []string{"event", ">"},
		Count: 1,
		Block: 0,
		NoAck: false,
		Claim: 0,
	}).Result();

	if error != nil {
		return error;
	}

	for _, stream := range streams {
		for _, msg := range stream.Messages {
			// fmt.Println(msg.ID);
			// fmt.Printf("eventType: %s\n", msg.Values["eventType"]);
			// fmt.Printf("Payload: %s\n", msg.Values["payload"]);
			payload, ok := msg.Values["payload"].(string)
			// fmt.Println(ok);
			if !ok {
				return fmt.Errorf("invalid payload");
			}

			eventId, ok := msg.Values["eventId"].(string)
				if !ok {
					return fmt.Errorf("eventId is missing or not a string");
				}

			switch msg.Values["eventType"]{

			case "email":	
				err := eventhandlers.EmailHandler(payload, eventId);
				if err == nil {
					err = repo.MarkComplete(eventId);
				}
				client.XAck(context.Background(), "event", "event-processors", msg.ID).Result();

			case "payment":
				err := eventhandlers.PaymentHandler(payload);
				if err == nil {
					err = repo.MarkComplete(eventId);
				}
				client.XAck(context.Background(), "event", "event-processors", msg.ID).Result();	
			}
			
		}
	}

	return nil;
}
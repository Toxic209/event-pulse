package streams

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/Toxic209/event-pulse/src/worker/internals/eventHandlers"
)


func EnsureGroupCreation(client *redis.Client) error {
	return client.XGroupCreateMkStream(
		context.Background(), 
		"event", "event-processors", "$",
	).Err();
}

func FetchEvent(client *redis.Client, processorGroup string, consumerName string, ) error {
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
			fmt.Println(ok);
			if !ok {
				return fmt.Errorf("invalid payload");
			}

			switch msg.Values["eventType"]{
			case "email":
				eventhandlers.EmailHandler(payload);
			case "payment":
				eventhandlers.PaymentHandler(payload);
			}
			
			client.XAck(context.Background(), "event", "event-processors", msg.ID).Result();
		}
	}

	return nil;
}
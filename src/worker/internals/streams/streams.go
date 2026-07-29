package streams

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
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
			fmt.Println(msg.ID);
			fmt.Printf("eventType: %s\n", msg.Values["eventType"]);
			fmt.Printf("Payload: %s\n", msg.Values["payload"]);
			client.XAck(context.Background(), "event", "event-processors", "$");
		}
	}

	return nil;
}
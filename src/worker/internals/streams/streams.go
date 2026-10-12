package streams

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"
	"errors"

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

func AddPendingEvents(pendingEvents []postgres.Events, client *redis.Client, logger *slog.Logger) error {

	for _, event := range pendingEvents {
		payloadJson, err := json.Marshal(event.Payload)

		if err != nil {
			return fmt.Errorf("Error: %w", err)
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
			logger.Error(
				"Failed to add pending events back to the stream.",
				"eventId", event.ID,
				"eventType", event.EventType,
				"error", err,
			)
			return err
		}
	}
	return nil
}

func FetchEvent(
	ctx context.Context,
	client *redis.Client,
	processorGroup string,
	consumerName string,
	repo *postgres.EventRepo,
	logger *slog.Logger,
) ([]redis.XStream, error) {
	streams, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    processorGroup,
		Consumer: consumerName,
		Streams:  []string{"event", ">"},
		Count:    10,
		Block:    5 * time.Second,
		NoAck:    false,
		Claim:    0,
	}).Result()

	if err != nil {

		if ctx.Err() != nil {
			return nil, ctx.Err();
		}

		if errors.Is(err, redis.Nil){
			return nil, nil;
		}

		logger.Error(
			"Failed to fetch event from stream",
			"processor-group", processorGroup,
			"consumer-name", consumerName,
			"error", err,
		)

		return []redis.XStream{}, err
	}

	return streams, nil
}

func ProcessEvent(
	client *redis.Client,
	repo *postgres.EventRepo,
	msg redis.XMessage,
	processorGroup string,
	logger *slog.Logger,
	processorName string,
) error {

	payload, ok := msg.Values["payload"].(string)

	if !ok {
		return fmt.Errorf("invalid payload")
	}

	eventId, ok := msg.Values["eventId"].(string)
	if !ok {
		return fmt.Errorf("eventId is missing or not a string")
	}

	eventType := msg.Values["eventType"].(string)

	eventLogger := logger.With(
		"event_id", eventId,
		"event_type", eventType,
		"processor", processorName,
	)

	switch eventType {

	case "email":
		err := eventhandlers.EmailHandler(payload, eventId)

		if err == nil {

			eventLogger.Info("event processed")

			if err := repo.MarkComplete(eventId); err != nil {
				eventLogger.Error("Unable to mark complete after event processing,", "error", err)
				return err
			}

		} else {
			eventLogger.Error("event processing failed", "error", err)

			failedEvent := msg.Values["status"] == "FAILED"

			if failedEvent {
				err := repo.IncrementRetryCount(eventId)

				if err != nil {
					eventLogger.Error("failed to increment retry count.", "error", err)
					return err
				}
			}

			if err := repo.MarkFailed(eventId); err != nil {
				eventLogger.Error("unable to mark event as failed after failure", "error", err)
				return err
			}
		}

	case "payment":
		err := eventhandlers.PaymentHandler(payload)

		if err == nil {

			eventLogger.Info("event processed")

			if err := repo.MarkComplete(eventId); err != nil {
				eventLogger.Error("Unable to mark complete after event processing,", "error", err)
				return err
			}

		} else {

			eventLogger.Error("event processing failed", "error", err)

			failedEvent := msg.Values["status"] == "FAILED"

			if failedEvent {
				err := repo.IncrementRetryCount(eventId)

				if err != nil {
					eventLogger.Error("failed to increment retry count.", "error", err)
					return err
				}
			}

			if err := repo.MarkFailed(eventId); err != nil {
				eventLogger.Error("unable to mark event as failed after failure", "error", err)
				return err
			}
		}
	default:
		eventLogger.Error("unknown event type", "event_type", eventType)
		return fmt.Errorf("unknow event type: %s", eventType)
	}

	_, err := client.XAck(context.Background(), "event", processorGroup, msg.ID).Result()
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func AddDeadEventsToDLQ(repo *postgres.EventRepo, client *redis.Client, logger *slog.Logger) error {

	deadEvents, err := repo.FetchDeadEvents()

	if err != nil {
		return fmt.Errorf("Error fetching dead events: %w", err)
	}

	for _, deadEvent := range deadEvents {

		payloadjson, err := json.Marshal(deadEvent.Payload)

		if err != nil {
			return fmt.Errorf("JSON Marshall Error: %w", err)
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
			logger.Error(
				"Failed to add dead events to the DLQ.",
				"eventId", deadEvent.ID,
				"eventType", deadEvent.EventType,
				"error", err,
			)
			return err
		}
	}

	return nil
}

func AutoClaimPendingEvents(
	client *redis.Client,
	consumerName string,
	consumerGroup string,
	start string,
	logger *slog.Logger) ([]redis.XMessage, string, error) {
	pendingMessages, nextStart, err := client.XAutoClaim(context.Background(), &redis.XAutoClaimArgs{
		Stream:   "event",
		Group:    consumerGroup,
		Consumer: consumerName,
		MinIdle:  30 * time.Second,
		Start:    start,
		Count:    10,
	},
	).Result()

	if err != nil {
		logger.Error(
			"Error Fetching Pending Events from PEL.",
			"consumer-group", consumerGroup,
			"consumer-name", consumerName,
			"error", err,
		)

		return pendingMessages, nextStart, nil
	}

	return pendingMessages, nextStart, nil
}

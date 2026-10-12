package worker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Toxic209/event-pulse/src/worker/internals/postgres"
	"github.com/Toxic209/event-pulse/src/worker/internals/streams"
	"github.com/redis/go-redis/v9"
)

func StartWorker(client *redis.Client, logger *slog.Logger) error {

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)

	defer stop();

	consumerGroup := "event-processors"

	db, err := postgres.Connectpg()

	if err != nil {
		logger.Error("Postgres connection failed!", "error", err)
		return err
	}

	repo := postgres.NewEventRepo(db)

	jobs := make(chan redis.XMessage)

	processorBound := 10

	cursorStartForAutoClaim := "0-0"

	pendingEvents, err := repo.RecoverPending()
	if err != nil {
		logger.Error("Unable to Fetch Pending Events from the PEL!", "error", err)
		return err
	}

	logger.Info("Go worker running...")

	logger.Info("Pending events recovered: ", slog.Int("count", len(pendingEvents)))

	err = streams.AddPendingEvents(pendingEvents, client, logger)

	if err != nil {
		logger.Error("Failed to add recovered pending events to the stream.", "error", err)
		return err
	}

	err = streams.AddDeadEventsToDLQ(&repo, client, logger)

	if err != nil {
		logger.Error("Failed to add dead events to the DLQ.", "error", err)
		return err
	}

	var wg sync.WaitGroup

	for i := 0; i < processorBound; i++ {
		processorName := fmt.Sprintf("processor-%d", i)
		wg.Add(1)

		go func(processorName string) {
			defer wg.Done()
			for msg := range jobs {
				streams.ProcessEvent(client, &repo, msg, consumerGroup, logger, processorName)
			}
		}(processorName)
	}

	consumerName := "worker-1"
	if len(os.Args) >= 2 {
		consumerName = os.Args[1]
	}

workerLoop:
	for {

		fetchedStreams, err := streams.FetchEvent(ctx, client, consumerGroup, consumerName, &repo, logger)

		if err != nil {
			if ctx.Err() != nil {
				break
			}

			logger.Error("Failed to fetch Events from the Stream", "error", err)
			continue
		}

		for _, stream := range fetchedStreams {
			for _, msg := range stream.Messages {
				select {
				case jobs <- msg:

				case <-ctx.Done():
					break workerLoop
				}
			}
		}

		//	====Auto Claim pending events from the PEL====
	autoClaim:
		for {
			pendingMessages, nextStart, err := streams.AutoClaimPendingEvents(client, consumerName, consumerGroup, cursorStartForAutoClaim, logger)

			if err != nil {
				logger.Error("Failure in Auto Claiming Pending Events fromt the PEL.", "error", err)
				break
			}

			for _, msg := range pendingMessages {
				select {
				case jobs <- msg:
				case <-ctx.Done():
					break autoClaim
				}
			}

			cursorStartForAutoClaim = nextStart

			if cursorStartForAutoClaim == "0-0" {
				break
			}
		}
	}

	close(jobs)
	wg.Wait()
	db.Close()
	client.Close()

	return nil
}

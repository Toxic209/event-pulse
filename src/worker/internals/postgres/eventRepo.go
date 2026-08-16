package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type EventRepo struct {
	db *pgx.Conn
}

type Events struct {
	ID        string
	EventType string
	Payload   any
}

func NewEventRepo(db *pgx.Conn) EventRepo {
	return EventRepo{
		db: db,
	}
}

func (repo *EventRepo) MarkComplete(eventId string) error {
	_, err := repo.db.Exec(
		context.Background(),
		`
		UPDATE event
		SET status = $1
		WHERE id =$2
		`, "PROCESSED", eventId,
	)

	if err != nil {
		return err
	}

	return nil
}

func (repo *EventRepo) MarkFailed(eventId string) error {
	_, err := repo.db.Exec(
		context.Background(),
		`
		UPDATE event
		SET status = $1
		WHERE id =$2
		`, "FAILED", eventId,
	)

	if err != nil {
		return err
	}

	return nil
}

func (repo *EventRepo) RecoverPending() ([]Events, error) {

	rows, err := repo.db.Query(
		context.Background(),
		`SELECT id, "eventType", payload FROM event
		WHERE status IN ('PENDING', 'FAILED')`,
	)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var events []Events

	for rows.Next() {
		var event Events

		err := rows.Scan(
			&event.ID,
			&event.EventType,
			&event.Payload,
		)

		if err != nil {
			fmt.Println("Scan error:", err)
			return nil, err
		}

		events = append(events, event)

	}

	return events, nil
}

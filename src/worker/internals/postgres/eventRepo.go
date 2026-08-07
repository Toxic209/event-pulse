package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
)

type EventRepo struct {
	db *pgx.Conn
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

	return nil;
}
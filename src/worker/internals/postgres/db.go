package postgres

import (
	"context"
	"os"
	"log"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func Connectpg() (*pgxpool.Pool, error) {
	err := godotenv.Load("../../.env");
	if err != nil {
		log.Fatal(err);
	}

	pool, err := pgxpool.New(context.Background(), os.Getenv("DATABASE_URL"));
	if err != nil {
		panic(err);
	}

	fmt.Println("Connected to PostgreSQL")

	return pool, nil;
}
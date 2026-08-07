package postgres

import (
	"context"
	"os"
	"log"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
)

func Connectpg() (*pgx.Conn, error) {
	err := godotenv.Load("../../.env");
	if err != nil {
		log.Fatal(err);
	}

	conn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"));
	if err != nil {
		panic(err);
	}

	fmt.Println("Connected to PostgreSQL")

	return conn, nil;
}
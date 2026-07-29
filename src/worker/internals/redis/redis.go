package redis

import (
	"github.com/redis/go-redis/v9"
	"github.com/joho/godotenv"
	"os"
	"log"
)

func NewRedisClient() *redis.Client {

	err := godotenv.Load("../../.env");
	if err != nil {
		log.Fatal(err);
	}

	opts, err := redis.ParseURL(os.Getenv("REDIS_URL"))
	if err != nil {
		panic(err)
	}

	return redis.NewClient(opts)
}

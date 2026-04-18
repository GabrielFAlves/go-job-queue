package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/seu-usuario/go-job-queue/internal/producer"
	"github.com/seu-usuario/go-job-queue/internal/queue"
)

func main() {
	redisAddr := flag.String("redis-addr", "localhost:6379", "Redis address")
	redisPassword := flag.String("redis-password", "", "Redis password")
	redisDB := flag.Int("redis-db", 0, "Redis DB")
	stream := flag.String("stream", "jobs", "Redis stream name")
	jobType := flag.String("type", "", "Job type, e.g. email.send")
	payload := flag.String("payload", "{}", "Raw JSON payload")
	maxAttempts := flag.Int("max-attempts", 3, "Max retry attempts before DLQ")
	flag.Parse()

	if *jobType == "" {
		log.Fatal("-type is required")
	}

	if !json.Valid([]byte(*payload)) {
		log.Fatal("-payload must be a valid JSON string")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	redisClient := redis.NewClient(&redis.Options{
		Addr:     *redisAddr,
		Password: *redisPassword,
		DB:       *redisDB,
	})
	defer func() {
		_ = redisClient.Close()
	}()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis ping: %v", err)
	}

	queueClient := queue.NewRedisStreamQueue(redisClient, *stream)
	producerService := producer.NewService(queueClient)

	job, streamID, err := producerService.Enqueue(ctx, producer.EnqueueInput{
		Type:        *jobType,
		Payload:     []byte(*payload),
		MaxAttempts: *maxAttempts,
	})
	if err != nil {
		log.Fatalf("enqueue: %v", err)
	}

	fmt.Printf("job enqueued successfully\n")
	fmt.Printf("job_id=%s stream=%s stream_id=%s\n", job.ID, *stream, streamID)
}

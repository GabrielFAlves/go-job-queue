package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/GabrielFAlves/go-job-queue/internal/domain"
	"github.com/GabrielFAlves/go-job-queue/internal/metrics"
	"github.com/GabrielFAlves/go-job-queue/internal/queue"
	"github.com/GabrielFAlves/go-job-queue/internal/worker"
)

func main() {
	redisAddr := flag.String("redis-addr", "localhost:6379", "Redis address")
	redisPassword := flag.String("redis-password", "", "Redis password")
	redisDB := flag.Int("redis-db", 0, "Redis DB")
	stream := flag.String("stream", "jobs", "Redis stream name")
	dlqStream := flag.String("dlq-stream", "jobs.dlq", "Dead letter stream name")
	group := flag.String("group", "workers", "Redis consumer group")
	consumerName := flag.String("consumer", fmt.Sprintf("consumer-%d", time.Now().UnixNano()), "Consumer name")
	concurrency := flag.Int("concurrency", 2, "Number of worker goroutines")
	retryBase := flag.Duration("retry-base", time.Second, "Base retry backoff duration")
	retryMax := flag.Duration("retry-max", 30*time.Second, "Maximum retry backoff duration")
	shutdownTimeout := flag.Duration("shutdown-timeout", 10*time.Second, "Graceful shutdown timeout for in-flight jobs")
	metricsAddr := flag.String("metrics-addr", ":2112", "HTTP address for /metrics endpoint")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

	redisQueue := queue.NewRedisStreamQueue(redisClient, *stream)
	redisQueue.SetDLQStream(*dlqStream)
	metricsStore := metrics.NewStore()

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", metricsStore)
	metricsServer := &http.Server{
		Addr:    *metricsAddr,
		Handler: metricsMux,
	}
	go func() {
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("metrics server stopped with error: %v", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = metricsServer.Shutdown(shutdownCtx)
	}()

	registry := worker.NewRegistry()
	mustRegister(registry, "email.send", func(ctx context.Context, job domain.Job) error {
		var payload map[string]any
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			return fmt.Errorf("decode payload: %w", err)
		}

		if failValue, exists := payload["fail"]; exists {
			if failBool, ok := failValue.(bool); ok && failBool {
				return errors.New("simulated failure requested by payload.fail=true")
			}
		}

		to, _ := payload["to"].(string)
		if to == "" {
			to = "unknown"
		}

		log.Printf("[email.send] sending email to=%s job_id=%s attempt=%d/%d", to, job.ID, job.Attempts+1, job.MaxAttempts)
		select {
		case <-time.After(400 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	mustRegister(registry, "image.resize", func(ctx context.Context, job domain.Job) error {
		log.Printf("[image.resize] processing job_id=%s attempt=%d/%d", job.ID, job.Attempts+1, job.MaxAttempts)
		select {
		case <-time.After(200 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	processor := worker.NewProcessor(
		redisQueue,
		registry,
		metricsStore,
		*group,
		*consumerName,
		*concurrency,
		*retryBase,
		*retryMax,
	)

	log.Printf("worker started stream=%s dlq=%s group=%s consumer=%s concurrency=%d metrics=%s", *stream, *dlqStream, *group, *consumerName, *concurrency, *metricsAddr)
	if err := processor.RunWithShutdown(ctx, *shutdownTimeout); err != nil {
		log.Fatalf("processor stopped with error: %v", err)
	}
	log.Printf("worker stopped")
}

func mustRegister(registry *worker.Registry, jobType string, handler worker.Handler) {
	if err := registry.Register(jobType, handler); err != nil {
		log.Fatalf("register handler type=%s: %v", jobType, err)
	}
}

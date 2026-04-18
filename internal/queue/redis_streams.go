package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/seu-usuario/go-job-queue/internal/domain"
)

var ErrNoMessages = errors.New("no messages available")

type StreamJobMessage struct {
	RedisID string
	Job     domain.Job
}

type RedisStreamQueue struct {
	client    *redis.Client
	stream    string
	dlqStream string
}

func NewRedisStreamQueue(client *redis.Client, stream string) *RedisStreamQueue {
	return &RedisStreamQueue{
		client:    client,
		stream:    stream,
		dlqStream: stream + ".dlq",
	}
}

func (q *RedisStreamQueue) SetDLQStream(stream string) {
	if stream == "" {
		return
	}
	q.dlqStream = stream
}

func (q *RedisStreamQueue) Enqueue(ctx context.Context, job domain.Job) (string, error) {
	return q.enqueueToStream(ctx, q.stream, job)
}

func (q *RedisStreamQueue) Requeue(ctx context.Context, job domain.Job) (string, error) {
	return q.enqueueToStream(ctx, q.stream, job)
}

func (q *RedisStreamQueue) SendToDLQ(ctx context.Context, job domain.Job) (string, error) {
	return q.enqueueToStream(ctx, q.dlqStream, job)
}

func (q *RedisStreamQueue) enqueueToStream(ctx context.Context, stream string, job domain.Job) (string, error) {
	jobBytes, err := json.Marshal(job)
	if err != nil {
		return "", fmt.Errorf("marshal job: %w", err)
	}

	id, err := q.client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{
			"job": string(jobBytes),
		},
	}).Result()
	if err != nil {
		return "", fmt.Errorf("xadd to stream %q: %w", stream, err)
	}

	return id, nil
}

func (q *RedisStreamQueue) EnsureConsumerGroup(ctx context.Context, group string) error {
	err := q.client.XGroupCreateMkStream(ctx, q.stream, group, "0").Err()
	if err == nil {
		return nil
	}

	if strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}

	return fmt.Errorf("create consumer group %q for stream %q: %w", group, q.stream, err)
}

func (q *RedisStreamQueue) ReadGroup(ctx context.Context, group, consumer string, count int64, block time.Duration) ([]StreamJobMessage, error) {
	streams, err := q.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{q.stream, ">"},
		Count:    count,
		Block:    block,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrNoMessages
		}
		return nil, fmt.Errorf("xreadgroup stream %q: %w", q.stream, err)
	}

	messages := make([]StreamJobMessage, 0)
	for _, stream := range streams {
		for _, msg := range stream.Messages {
			raw, ok := msg.Values["job"]
			if !ok {
				continue
			}

			rawString, ok := raw.(string)
			if !ok {
				continue
			}

			var job domain.Job
			if err := json.Unmarshal([]byte(rawString), &job); err != nil {
				return nil, fmt.Errorf("unmarshal job from stream message %s: %w", msg.ID, err)
			}

			messages = append(messages, StreamJobMessage{
				RedisID: msg.ID,
				Job:     job,
			})
		}
	}

	if len(messages) == 0 {
		return nil, ErrNoMessages
	}

	return messages, nil
}

func (q *RedisStreamQueue) Ack(ctx context.Context, group string, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}

	if err := q.client.XAck(ctx, q.stream, group, ids...).Err(); err != nil {
		return fmt.Errorf("xack stream %q: %w", q.stream, err)
	}

	return nil
}

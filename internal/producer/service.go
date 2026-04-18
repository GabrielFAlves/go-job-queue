package producer

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/GabrielFAlves/go-job-queue/internal/domain"
)

type QueuePublisher interface {
	Enqueue(ctx context.Context, job domain.Job) (string, error)
}

type Service struct {
	publisher QueuePublisher
}

func NewService(publisher QueuePublisher) *Service {
	return &Service{publisher: publisher}
}

type EnqueueInput struct {
	Type        string
	Payload     []byte
	MaxAttempts int
	Metadata    map[string]string
}

func (s *Service) Enqueue(ctx context.Context, in EnqueueInput) (domain.Job, string, error) {
	if in.Type == "" {
		return domain.Job{}, "", fmt.Errorf("type is required")
	}
	if in.MaxAttempts <= 0 {
		in.MaxAttempts = 3
	}

	now := time.Now().UTC()
	job := domain.Job{
		ID:          uuid.NewString(),
		Type:        in.Type,
		Payload:     in.Payload,
		Attempts:    0,
		MaxAttempts: in.MaxAttempts,
		CreatedAt:   now,
		AvailableAt: now,
		Status:      domain.JobStatusPending,
		Metadata:    in.Metadata,
	}

	streamID, err := s.publisher.Enqueue(ctx, job)
	if err != nil {
		return domain.Job{}, "", fmt.Errorf("enqueue job %s: %w", job.ID, err)
	}

	return job, streamID, nil
}

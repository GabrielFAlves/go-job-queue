package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"github.com/GabrielFAlves/go-job-queue/internal/domain"
	"github.com/GabrielFAlves/go-job-queue/internal/metrics"
	"github.com/GabrielFAlves/go-job-queue/internal/queue"
)

type Consumer interface {
	EnsureConsumerGroup(ctx context.Context, group string) error
	ReadGroup(ctx context.Context, group, consumer string, count int64, block time.Duration) ([]queue.StreamJobMessage, error)
	Ack(ctx context.Context, group string, ids ...string) error
	Requeue(ctx context.Context, job domain.Job) (string, error)
	SendToDLQ(ctx context.Context, job domain.Job) (string, error)
}

type Processor struct {
	consumer     Consumer
	registry     *Registry
	metrics      *metrics.Store
	group        string
	consumerName string
	workers      int
	pollCount    int64
	pollBlock    time.Duration
	baseBackoff  time.Duration
	maxBackoff   time.Duration
}

func NewProcessor(consumer Consumer, registry *Registry, metricsStore *metrics.Store, group, consumerName string, workers int, baseBackoff, maxBackoff time.Duration) *Processor {
	if workers <= 0 {
		workers = 1
	}
	if baseBackoff <= 0 {
		baseBackoff = time.Second
	}
	if maxBackoff < baseBackoff {
		maxBackoff = 30 * time.Second
	}

	return &Processor{
		consumer:     consumer,
		registry:     registry,
		metrics:      metricsStore,
		group:        group,
		consumerName: consumerName,
		workers:      workers,
		pollCount:    int64(workers),
		pollBlock:    2 * time.Second,
		baseBackoff:  baseBackoff,
		maxBackoff:   maxBackoff,
	}
}

func (p *Processor) Run(ctx context.Context) error {
	if err := p.consumer.EnsureConsumerGroup(ctx, p.group); err != nil {
		return err
	}

	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()

	jobsCh := make(chan queue.StreamJobMessage, p.workers*2)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	for i := 0; i < p.workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for msg := range jobsCh {
				p.addPending(-1)
				p.addProcessing(1)

				failedAttempt, err := p.handleOne(workerCtx, msg)
				p.addProcessing(-1)
				if failedAttempt {
					p.incFailed()
				}
				if err == nil && !failedAttempt {
					p.incCompleted()
				}

				if err != nil {
					log.Printf("worker=%d job_id=%s redis_id=%s error=%v", workerID, msg.Job.ID, msg.RedisID, err)
				}
			}
		}(i + 1)
	}

	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		defer close(jobsCh)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			messages, err := p.consumer.ReadGroup(ctx, p.group, p.consumerName, p.pollCount, p.pollBlock)
			if err != nil {
				if errors.Is(err, queue.ErrNoMessages) {
					continue
				}

				select {
				case errCh <- err:
				default:
				}
				return
			}

			for _, msg := range messages {
				select {
				case jobsCh <- msg:
					p.addPending(1)
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	select {
	case <-ctx.Done():
		<-pollDone
		wg.Wait()
		return nil
	case err := <-errCh:
		<-pollDone
		workerCancel()
		wg.Wait()
		return fmt.Errorf("worker poll failed: %w", err)
	}
}

func (p *Processor) RunWithShutdown(ctx context.Context, shutdownTimeout time.Duration) error {
	if shutdownTimeout <= 0 {
		return p.Run(ctx)
	}

	if err := p.consumer.EnsureConsumerGroup(ctx, p.group); err != nil {
		return err
	}

	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()

	jobsCh := make(chan queue.StreamJobMessage, p.workers*2)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	for i := 0; i < p.workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for msg := range jobsCh {
				p.addPending(-1)
				p.addProcessing(1)

				failedAttempt, err := p.handleOne(workerCtx, msg)
				p.addProcessing(-1)
				if failedAttempt {
					p.incFailed()
				}
				if err == nil && !failedAttempt {
					p.incCompleted()
				}

				if err != nil {
					log.Printf("worker=%d job_id=%s redis_id=%s error=%v", workerID, msg.Job.ID, msg.RedisID, err)
				}
			}
		}(i + 1)
	}

	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		defer close(jobsCh)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			messages, err := p.consumer.ReadGroup(ctx, p.group, p.consumerName, p.pollCount, p.pollBlock)
			if err != nil {
				if errors.Is(err, queue.ErrNoMessages) {
					continue
				}

				select {
				case errCh <- err:
				default:
				}
				return
			}

			for _, msg := range messages {
				select {
				case jobsCh <- msg:
					p.addPending(1)
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	waitWorkers := func(timeout time.Duration) bool {
		done := make(chan struct{})
		go func() {
			defer close(done)
			wg.Wait()
		}()

		select {
		case <-done:
			return true
		case <-time.After(timeout):
			return false
		}
	}

	select {
	case <-ctx.Done():
		<-pollDone
		if waitWorkers(shutdownTimeout) {
			return nil
		}

		log.Printf("shutdown timeout reached (%s), canceling in-flight jobs", shutdownTimeout)
		workerCancel()
		wg.Wait()
		return nil
	case err := <-errCh:
		<-pollDone
		workerCancel()
		wg.Wait()
		return fmt.Errorf("worker poll failed: %w", err)
	}
}

func (p *Processor) handleOne(ctx context.Context, msg queue.StreamJobMessage) (bool, error) {
	handler, ok := p.registry.Get(msg.Job.Type)
	if !ok {
		if err := p.retryOrDeadLetter(ctx, msg, fmt.Errorf("no handler registered for job type %q", msg.Job.Type)); err != nil {
			return true, err
		}
		return true, nil
	}

	if err := handler(ctx, msg.Job); err != nil {
		if retryErr := p.retryOrDeadLetter(ctx, msg, err); retryErr != nil {
			return true, retryErr
		}
		return true, nil
	}

	if err := p.consumer.Ack(ctx, p.group, msg.RedisID); err != nil {
		return true, fmt.Errorf("ack success path: %w", err)
	}

	log.Printf("processed job_id=%s type=%s redis_id=%s", msg.Job.ID, msg.Job.Type, msg.RedisID)
	return false, nil
}

func (p *Processor) retryOrDeadLetter(ctx context.Context, msg queue.StreamJobMessage, processingErr error) error {
	job := msg.Job
	job.Attempts++
	job.LastError = processingErr.Error()
	job.Status = domain.JobStatusFailed

	if job.MaxAttempts <= 0 {
		job.MaxAttempts = 3
	}

	if job.Attempts >= job.MaxAttempts {
		job.Status = domain.JobStatusDead
		job.AvailableAt = time.Now().UTC()

		dlqID, err := p.consumer.SendToDLQ(ctx, job)
		if err != nil {
			return fmt.Errorf("send to dlq: %w", err)
		}

		if err := p.consumer.Ack(ctx, p.group, msg.RedisID); err != nil {
			return fmt.Errorf("ack after dlq: %w", err)
		}

		log.Printf("job moved to dlq job_id=%s attempts=%d max_attempts=%d dlq_id=%s error=%v", job.ID, job.Attempts, job.MaxAttempts, dlqID, processingErr)
		return nil
	}

	backoff := p.backoff(job.Attempts)
	job.Status = domain.JobStatusPending
	job.AvailableAt = time.Now().UTC().Add(backoff)

	select {
	case <-time.After(backoff):
	case <-ctx.Done():
		return ctx.Err()
	}

	requeuedID, err := p.consumer.Requeue(ctx, job)
	if err != nil {
		return fmt.Errorf("requeue failed: %w", err)
	}

	if err := p.consumer.Ack(ctx, p.group, msg.RedisID); err != nil {
		return fmt.Errorf("ack after requeue: %w", err)
	}

	log.Printf("job retry scheduled job_id=%s attempt=%d/%d backoff=%s requeued_id=%s error=%v", job.ID, job.Attempts, job.MaxAttempts, backoff, requeuedID, processingErr)
	return nil
}

func (p *Processor) backoff(attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}

	exp := math.Pow(2, float64(attempt-1))
	delay := time.Duration(float64(p.baseBackoff) * exp)
	if delay > p.maxBackoff {
		return p.maxBackoff
	}

	return delay
}

func (p *Processor) addPending(delta int64) {
	if p.metrics == nil {
		return
	}
	p.metrics.AddPending(delta)
}

func (p *Processor) addProcessing(delta int64) {
	if p.metrics == nil {
		return
	}
	p.metrics.AddProcessing(delta)
}

func (p *Processor) incCompleted() {
	if p.metrics == nil {
		return
	}
	p.metrics.IncCompleted()
}

func (p *Processor) incFailed() {
	if p.metrics == nil {
		return
	}
	p.metrics.IncFailed()
}

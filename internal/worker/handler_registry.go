package worker

import (
	"context"
	"fmt"
	"sync"

	"github.com/seu-usuario/go-job-queue/internal/domain"
)

type Handler func(ctx context.Context, job domain.Job) error

type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

func (r *Registry) Register(jobType string, handler Handler) error {
	if jobType == "" {
		return fmt.Errorf("job type is required")
	}
	if handler == nil {
		return fmt.Errorf("handler is required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[jobType]; exists {
		return fmt.Errorf("handler already registered for type %q", jobType)
	}

	r.handlers[jobType] = handler
	return nil
}

func (r *Registry) Get(jobType string) (Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	h, ok := r.handlers[jobType]
	return h, ok
}

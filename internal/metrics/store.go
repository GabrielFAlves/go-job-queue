package metrics

import (
	"fmt"
	"net/http"
	"sync"
)

type Snapshot struct {
	Pending    int64
	Processing int64
	Completed  int64
	Failed     int64
}

type Store struct {
	mu       sync.RWMutex
	pending  int64
	inFlight int64
	done     int64
	failed   int64
}

func NewStore() *Store {
	return &Store{}
}

func (s *Store) AddPending(delta int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending += delta
	if s.pending < 0 {
		s.pending = 0
	}
}

func (s *Store) AddProcessing(delta int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inFlight += delta
	if s.inFlight < 0 {
		s.inFlight = 0
	}
}

func (s *Store) IncCompleted() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.done++
}

func (s *Store) IncFailed() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failed++
}

func (s *Store) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return Snapshot{
		Pending:    s.pending,
		Processing: s.inFlight,
		Completed:  s.done,
		Failed:     s.failed,
	}
}

func (s *Store) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	snap := s.Snapshot()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	_, _ = fmt.Fprintf(w,
		"jobs_pending %d\njobs_processing %d\njobs_completed_total %d\njobs_failed_total %d\n",
		snap.Pending,
		snap.Processing,
		snap.Completed,
		snap.Failed,
	)
}

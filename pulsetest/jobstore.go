package pulsetest

import (
	"context"
	"fmt"
	"time"

	"github.com/seanly/pulse-devkit/pulse"
)

// --- JobStore ---

func (s *Store) Enqueue(_ context.Context, jobs []*pulse.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return err
	}
	for _, j := range jobs {
		stored := *j
		if stored.Status == "" {
			stored.Status = pulse.JobPending
		}
		if stored.MaxAttempts <= 0 {
			stored.MaxAttempts = 1
		}
		if stored.CreatedAt.IsZero() {
			stored.CreatedAt = s.now()
		}
		if _, exists := s.jobs[stored.ID]; !exists {
			s.order = append(s.order, stored.ID)
		}
		s.jobs[stored.ID] = &stored
	}
	return nil
}

func (s *Store) GetJob(_ context.Context, id string) (pulse.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return pulse.Job{}, err
	}
	j, ok := s.jobs[id]
	if !ok {
		return pulse.Job{}, fmt.Errorf("job %s: %w", id, pulse.ErrNotFound)
	}
	return *j, nil
}

// Lease claims pending jobs for one agent.
//
// The whole claim happens under one lock, which is this implementation's
// equivalent of the SQL version's single statement: nothing can interleave
// between choosing candidates and marking them leased, so no job is ever
// leased twice.
func (s *Store) Lease(_ context.Context, agentID string, limit int, leaseFor time.Duration) ([]pulse.Job, error) {
	if limit <= 0 {
		return nil, nil
	}
	s.mu.Lock()
	hook := s.onLease
	s.mu.Unlock()
	// Outside the lock so a hook can itself call Lease, simulating another
	// process claiming the same work.
	if hook != nil {
		hook()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return nil, err
	}
	expires := s.now().Add(leaseFor)
	var out []pulse.Job
	// Insertion order, like "ORDER BY created_at, id".
	for _, id := range s.order {
		if len(out) >= limit {
			break
		}
		j := s.jobs[id]
		if j == nil || j.AgentID != agentID || j.Status != pulse.JobPending {
			continue
		}
		j.Status = pulse.JobLeased
		j.Attempt++
		j.LeaseExpiresAt = expires
		out = append(out, *j)
	}
	return out, nil
}

func (s *Store) LeaseOne(_ context.Context, id string, leaseFor time.Duration) (pulse.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return pulse.Job{}, err
	}
	j, ok := s.jobs[id]
	if !ok {
		return pulse.Job{}, fmt.Errorf("job %s: %w", id, pulse.ErrNotFound)
	}
	if j.Status != pulse.JobPending {
		// Someone else owns delivery now. A conflict, not a fault.
		return pulse.Job{}, fmt.Errorf("job %s is %s: %w", id, j.Status, pulse.ErrConflict)
	}
	j.Status = pulse.JobLeased
	j.Attempt++
	j.LeaseExpiresAt = s.now().Add(leaseFor)
	return *j, nil
}

// RenewLease extends a leased job's deadline.
func (s *Store) RenewLease(_ context.Context, id, agentID string, leaseFor time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return err
	}
	j, ok := s.jobs[id]
	if !ok {
		return fmt.Errorf("job %s: %w", id, pulse.ErrNotFound)
	}
	// Both conditions matter. Leased, because renewing a finished job would
	// resurrect it; and this agent's, because a stale renewal from an agent
	// whose lease was already reclaimed would otherwise extend a lease now
	// held by someone else.
	if j.Status != pulse.JobLeased || j.AgentID != agentID {
		return fmt.Errorf("job %s is not leased to %s: %w", id, agentID, pulse.ErrNotFound)
	}
	j.LeaseExpiresAt = s.now().Add(leaseFor)
	return nil
}

func (s *Store) Complete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return err
	}
	j, ok := s.jobs[id]
	if !ok {
		return fmt.Errorf("job %s: %w", id, pulse.ErrNotFound)
	}
	// Matching on leased means a report that arrives after the reaper already
	// requeued the job does not resurrect it.
	if j.Status != pulse.JobLeased {
		return fmt.Errorf("job %s is %s, not leased: %w", id, j.Status, pulse.ErrNotFound)
	}
	j.Status = pulse.JobDone
	j.FinishedAt = s.now()
	j.LastError = ""
	j.LeaseExpiresAt = time.Time{}
	return nil
}

func (s *Store) Fail(_ context.Context, id, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return err
	}
	j, ok := s.jobs[id]
	if !ok {
		return fmt.Errorf("job %s: %w", id, pulse.ErrNotFound)
	}
	if j.Status != pulse.JobLeased {
		return fmt.Errorf("job %s is %s, not leased: %w", id, j.Status, pulse.ErrNotFound)
	}
	j.LastError = truncate(reason, 1000)
	j.LeaseExpiresAt = time.Time{}
	// Attempts remaining means another try; exhausted means terminal.
	if j.Attempt >= j.MaxAttempts {
		j.Status = pulse.JobFailed
		j.FinishedAt = s.now()
	} else {
		j.Status = pulse.JobPending
	}
	return nil
}

// Requeue hands a job back without burning an attempt, failing it once it has
// bounced more than maxRequeues times.
func (s *Store) Requeue(_ context.Context, id, reason string, maxRequeues int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return err
	}
	j, ok := s.jobs[id]
	if !ok {
		return fmt.Errorf("job %s: %w", id, pulse.ErrNotFound)
	}
	if j.Status != pulse.JobLeased {
		return fmt.Errorf("job %s is %s, not leased: %w", id, j.Status, pulse.ErrNotFound)
	}
	j.LastError = truncate(reason, 1000)
	j.LeaseExpiresAt = time.Time{}
	j.RequeueCount++
	// The attempt this lease consumed is given back: no work happened.
	if j.Attempt > 0 {
		j.Attempt--
	}
	// The brake, applied in the same critical section as the requeue so a
	// stale read cannot let a job past it.
	if maxRequeues > 0 && j.RequeueCount > maxRequeues {
		j.Status = pulse.JobFailed
		j.FinishedAt = s.now()
		return nil
	}
	j.Status = pulse.JobPending
	return nil
}

// FailTerminal fails a job outright, whatever its remaining attempts.
func (s *Store) FailTerminal(_ context.Context, id, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return err
	}
	j, ok := s.jobs[id]
	if !ok {
		return fmt.Errorf("job %s: %w", id, pulse.ErrNotFound)
	}
	switch j.Status {
	case pulse.JobDone, pulse.JobFailed, pulse.JobExpired, pulse.JobCancelled:
		return fmt.Errorf("job %s already finished as %s: %w", id, j.Status, pulse.ErrConflict)
	}
	j.Status = pulse.JobFailed
	j.LastError = truncate(reason, 1000)
	j.FinishedAt = s.now()
	j.LeaseExpiresAt = time.Time{}
	return nil
}

func (s *Store) Cancel(_ context.Context, id, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return err
	}
	j, ok := s.jobs[id]
	if !ok {
		return fmt.Errorf("job %s: %w", id, pulse.ErrNotFound)
	}
	switch j.Status {
	case pulse.JobDone, pulse.JobFailed, pulse.JobExpired, pulse.JobCancelled:
		return fmt.Errorf("job %s already finished as %s: %w", id, j.Status, pulse.ErrConflict)
	}
	j.Status = pulse.JobCancelled
	j.LastError = truncate(reason, 1000)
	j.FinishedAt = s.now()
	j.LeaseExpiresAt = time.Time{}
	return nil
}

func (s *Store) ReclaimExpiredLeases(_ context.Context) (pulse.ReclaimStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var stats pulse.ReclaimStats
	if err := s.taken(); err != nil {
		return stats, err
	}
	now := s.now()
	for _, id := range s.order {
		j := s.jobs[id]
		if j == nil || j.Status != pulse.JobLeased {
			continue
		}
		if j.LeaseExpiresAt.IsZero() || j.LeaseExpiresAt.After(now) {
			continue
		}
		if j.Attempt >= j.MaxAttempts {
			// Expired, not failed: the agent stopped answering, which is a
			// different problem from work that ran and reported an error.
			j.Status = pulse.JobExpired
			j.FinishedAt = now
			j.LeaseExpiresAt = time.Time{}
			j.LastError = "lease expired with no attempts remaining"
			stats.Expired = append(stats.Expired, *j)
			continue
		}
		j.Status = pulse.JobPending
		j.LeaseExpiresAt = time.Time{}
		j.LastError = "lease expired"
		stats.Requeued++
	}
	return stats, nil
}

func (s *Store) CountLeasedForAgent(_ context.Context, agentID string) (int, error) {
	return s.count(agentID, pulse.JobLeased)
}

func (s *Store) CountPendingForAgent(_ context.Context, agentID string) (int, error) {
	return s.count(agentID, pulse.JobPending)
}

func (s *Store) count(agentID, status string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return 0, err
	}
	n := 0
	for _, j := range s.jobs {
		if j.AgentID == agentID && j.Status == status {
			n++
		}
	}
	return n, nil
}

func (s *Store) PurgeFinished(_ context.Context, before time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return 0, err
	}
	var n int64
	kept := s.order[:0]
	for _, id := range s.order {
		j := s.jobs[id]
		if j == nil {
			continue
		}
		terminal := j.Status == pulse.JobDone || j.Status == pulse.JobFailed ||
			j.Status == pulse.JobExpired || j.Status == pulse.JobCancelled
		if terminal && !j.FinishedAt.IsZero() && j.FinishedAt.Before(before) {
			delete(s.jobs, id)
			n++
			continue
		}
		kept = append(kept, id)
	}
	s.order = kept
	return n, nil
}

// Jobs returns every stored job, for assertions.
func (s *Store) Jobs() []pulse.Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]pulse.Job, 0, len(s.order))
	for _, id := range s.order {
		if j := s.jobs[id]; j != nil {
			out = append(out, *j)
		}
	}
	return out
}

// SeedAgent inserts an agent directly, bypassing enrollment.
func (s *Store) SeedAgent(a pulse.Agent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Derived on write, overwriting whatever the caller passed — the same rule
	// the SQL store follows. Honouring a caller-supplied Labels here would let
	// a test set up a state the real store cannot produce.
	a.Labels = pulse.EffectiveLabels(a.ReportedLabels, a.LabelOverrides)
	if a.LastHeartbeatAt.IsZero() {
		a.LastHeartbeatAt = s.now()
	}
	if a.RegisteredAt.IsZero() {
		a.RegisteredAt = s.now()
	}
	s.agents[a.ID] = &a
}

// SeedConfig inserts a config directly at a chosen version.
func (s *Store) SeedConfig(c pulse.AgentConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c.Version == 0 {
		c.Version = 1
	}
	s.cfgs[c.AgentID] = &c
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// Compile-time proof that the fake satisfies every port.
var (
	_ pulse.AgentStore  = (*Store)(nil)
	_ pulse.TokenStore  = (*Store)(nil)
	_ pulse.ConfigStore = (*Store)(nil)
	_ pulse.JobStore    = (*Store)(nil)
)

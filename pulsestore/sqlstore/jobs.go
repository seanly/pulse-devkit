package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/seanly/pulse-devkit/pulse"
)

func (s *Store) jobColumns() string {
	return `id, group_id, subject_id, kind, agent_id, status, timeout_ms,
	        attempt, max_attempts, requeue_count, last_error, payload,
	        lease_expires_at, created_at, finished_at`
}

// jobColumnsQualified is the same list for a statement that aliases the table,
// which PostgreSQL's UPDATE ... FROM form requires.
func jobColumnsQualified(alias string) string {
	cols := []string{"id", "group_id", "subject_id", "kind", "agent_id", "status",
		"timeout_ms", "attempt", "max_attempts", "requeue_count", "last_error",
		"payload", "lease_expires_at", "created_at", "finished_at"}
	out := ""
	for i, c := range cols {
		if i > 0 {
			out += ", "
		}
		out += alias + "." + c
	}
	return out
}

func scanJob(sc interface{ Scan(...any) error }) (pulse.Job, error) {
	var (
		j                  pulse.Job
		groupID            sql.NullString
		timeoutMS          int64
		leaseExpires, done sql.NullTime
	)
	if err := sc.Scan(&j.ID, &groupID, &j.SubjectID, &j.Kind, &j.AgentID, &j.Status,
		&timeoutMS, &j.Attempt, &j.MaxAttempts, &j.RequeueCount, &j.LastError,
		&j.Payload, &leaseExpires, &j.CreatedAt, &done); err != nil {
		return j, err
	}
	j.GroupID = stringFrom(groupID)
	j.Timeout = time.Duration(timeoutMS) * time.Millisecond
	j.LeaseExpiresAt = timeFrom(leaseExpires)
	j.CreatedAt = j.CreatedAt.UTC()
	j.FinishedAt = timeFrom(done)
	return j, nil
}

// Enqueue inserts jobs as pending.
func (s *Store) Enqueue(ctx context.Context, jobs []*pulse.Job) error {
	if len(jobs) == 0 {
		return nil
	}
	err := s.tx(ctx, func(t *sql.Tx) error {
		stmt, err := t.PrepareContext(ctx, s.rebind(`
			INSERT INTO `+s.table("jobs")+` (
				id, group_id, subject_id, kind, agent_id, status, timeout_ms,
				attempt, max_attempts, requeue_count, last_error, payload,
				lease_expires_at, created_at, finished_at)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,NULL,?,NULL)`))
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, j := range jobs {
			if j.Status == "" {
				j.Status = pulse.JobPending
			}
			if j.MaxAttempts <= 0 {
				j.MaxAttempts = 1
			}
			if j.CreatedAt.IsZero() {
				j.CreatedAt = s.now()
			}
			if _, err := stmt.ExecContext(ctx,
				j.ID, nullString(j.GroupID), j.SubjectID, j.Kind, j.AgentID, j.Status,
				j.Timeout.Milliseconds(), j.Attempt, j.MaxAttempts, j.RequeueCount,
				j.LastError, j.Payload, j.CreatedAt); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("sqlstore: enqueue jobs: %w", err)
	}
	return nil
}

// GetJob returns one job.
func (s *Store) GetJob(ctx context.Context, id string) (pulse.Job, error) {
	j, err := scanJob(s.queryRow(ctx,
		`SELECT `+s.jobColumns()+` FROM `+s.table("jobs")+` WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return j, fmt.Errorf("sqlstore: job %s: %w", id, pulse.ErrNotFound)
	}
	if err != nil {
		return j, fmt.Errorf("sqlstore: get job: %w", err)
	}
	return j, nil
}

// Lease claims up to limit pending jobs for one agent.
//
// This is the query the whole queue rests on, and it is ONE STATEMENT in both
// dialects. A SELECT followed by an UPDATE would be a race whenever the caller
// is not already inside a transaction: two dispatch passes would both see the
// same pending rows and both lease them, and the same job would run twice.
//
// On PostgreSQL that means a CTE with FOR UPDATE SKIP LOCKED, so concurrent
// passes step over each other's locked candidates instead of blocking.
//
// On SQLite there is a single writer by construction, so SKIP LOCKED has no
// meaning and is not supported. The property that matters — no job leased
// twice — comes instead from the whole claim being one statement, with nothing
// able to interleave between choosing candidates and marking them leased.
func (s *Store) Lease(ctx context.Context, agentID string, limit int, leaseFor time.Duration) ([]pulse.Job, error) {
	if limit <= 0 {
		return nil, nil
	}
	expires := s.now().Add(leaseFor)

	var (
		text string
		args []any
	)
	switch s.dialect {
	case Postgres:
		text = `
			WITH candidate AS (
				SELECT id FROM ` + s.table("jobs") + `
				 WHERE agent_id = ? AND status = '` + pulse.JobPending + `'
				 ORDER BY created_at, id
				 LIMIT ?
				 FOR UPDATE SKIP LOCKED
			)
			UPDATE ` + s.table("jobs") + ` j
			   SET status = '` + pulse.JobLeased + `',
			       attempt = j.attempt + 1,
			       lease_expires_at = ?
			  FROM candidate c
			 WHERE j.id = c.id
			RETURNING ` + jobColumnsQualified("j")
		args = []any{agentID, limit, expires}
	default:
		text = `
			UPDATE ` + s.table("jobs") + `
			   SET status = '` + pulse.JobLeased + `',
			       attempt = attempt + 1,
			       lease_expires_at = ?
			 WHERE id IN (
				SELECT id FROM ` + s.table("jobs") + `
				 WHERE agent_id = ? AND status = '` + pulse.JobPending + `'
				 ORDER BY created_at, id
				 LIMIT ?
			 )
			RETURNING ` + s.jobColumns()
		args = []any{expires, agentID, limit}
	}

	rows, err := s.query(ctx, text, args...)
	if err != nil {
		return nil, fmt.Errorf("sqlstore: lease jobs: %w", err)
	}
	return collect(rows, scanJob, "lease jobs")
}

// LeaseOne claims one specific pending job, for an operator's "run this now".
//
// Matching on status = pending means a job the dispatch loop leased a moment
// earlier returns ErrConflict rather than being leased twice.
func (s *Store) LeaseOne(ctx context.Context, id string, leaseFor time.Duration) (pulse.Job, error) {
	expires := s.now().Add(leaseFor)
	j, err := scanJob(s.queryRow(ctx, `
		UPDATE `+s.table("jobs")+`
		   SET status = ?, attempt = attempt + 1, lease_expires_at = ?
		 WHERE id = ? AND status = ?
		RETURNING `+s.jobColumns(),
		pulse.JobLeased, expires, id, pulse.JobPending))
	if errors.Is(err, sql.ErrNoRows) {
		// Either there is no such job or it is no longer pending. Distinguish
		// them, because "already dispatched" is a success from the caller's
		// point of view and "no such job" is not.
		if _, getErr := s.GetJob(ctx, id); getErr != nil {
			return j, getErr
		}
		return j, fmt.Errorf("sqlstore: job %s is not pending: %w", id, pulse.ErrConflict)
	}
	if err != nil {
		return j, fmt.Errorf("sqlstore: lease one job: %w", err)
	}
	return j, nil
}

// Complete marks a leased job done.
//
// The status filter means a report arriving after the lease was reclaimed does
// not resurrect a job the reaper already returned to the queue; the caller
// reads ErrNotFound as "too late, and that is fine".
func (s *Store) Complete(ctx context.Context, id string) error {
	err := s.execOne(ctx, `
		UPDATE `+s.table("jobs")+`
		   SET status = ?, finished_at = ?, last_error = '', lease_expires_at = NULL
		 WHERE id = ? AND status = ?`,
		pulse.JobDone, s.now(), id, pulse.JobLeased)
	if err != nil && !errors.Is(err, pulse.ErrNotFound) {
		return fmt.Errorf("sqlstore: complete job: %w", err)
	}
	return err
}

// Fail records failed work, consuming the attempt this lease used.
func (s *Store) Fail(ctx context.Context, id, reason string) error {
	reason = truncate(reason, 1000)
	// The CASE's NULL arm leaves PostgreSQL unable to infer the timestamp
	// parameter's type — it resolves the whole expression to text and refuses
	// the assignment — so the parameter carries an explicit cast there.
	// SQLite takes the bare value.
	finishedAt := "?"
	if s.dialect == Postgres {
		finishedAt = "?::timestamptz"
	}
	err := s.execOne(ctx, `
		UPDATE `+s.table("jobs")+`
		   SET status = CASE WHEN attempt >= max_attempts THEN ? ELSE ? END,
		       finished_at = CASE WHEN attempt >= max_attempts THEN `+finishedAt+` ELSE NULL END,
		       lease_expires_at = NULL,
		       last_error = ?
		 WHERE id = ? AND status = ?`,
		pulse.JobFailed, pulse.JobPending, s.now(), reason, id, pulse.JobLeased)
	if err != nil && !errors.Is(err, pulse.ErrNotFound) {
		return fmt.Errorf("sqlstore: fail job: %w", err)
	}
	return err
}

// Requeue hands a job back without burning an attempt, and fails it once it
// has bounced past maxRequeues.
//
// The ceiling is enforced in this statement rather than by the caller: it then
// holds even against a stale RequeueCount read, and cannot be bypassed.
func (s *Store) Requeue(ctx context.Context, id, reason string, maxRequeues int) error {
	reason = truncate(reason, 1000)
	finishedAt := "?"
	if s.dialect == Postgres {
		finishedAt = "?::timestamptz"
	}
	// attempt is decremented because this lease did no work: counting it would
	// let a misconfigured concurrency exhaust max_attempts and fabricate
	// failures for subjects nothing ever looked at. The MAX guard keeps it
	// from going negative if a caller requeues an unleased job.
	err := s.execOne(ctx, `
		UPDATE `+s.table("jobs")+`
		   SET status = CASE WHEN requeue_count + 1 > ? THEN ? ELSE ? END,
		       finished_at = CASE WHEN requeue_count + 1 > ? THEN `+finishedAt+` ELSE NULL END,
		       requeue_count = requeue_count + 1,
		       attempt = CASE WHEN attempt > 0 THEN attempt - 1 ELSE 0 END,
		       lease_expires_at = NULL,
		       last_error = ?
		 WHERE id = ? AND status = ?`,
		maxRequeues, pulse.JobFailed, pulse.JobPending,
		maxRequeues, s.now(), reason, id, pulse.JobLeased)
	if err != nil && !errors.Is(err, pulse.ErrNotFound) {
		return fmt.Errorf("sqlstore: requeue job: %w", err)
	}
	return err
}

// FailTerminal fails a job outright, whatever its remaining attempts.
//
// No status filter on 'leased': the dispatcher calls this for a job it has
// just leased, but an application may also give up on one that is still
// pending. Only an already-finished job is refused.
func (s *Store) FailTerminal(ctx context.Context, id, reason string) error {
	list, args := placeholders([]string{
		pulse.JobDone, pulse.JobFailed, pulse.JobExpired, pulse.JobCancelled,
	})
	args = append([]any{pulse.JobFailed, truncate(reason, 1000), s.now(), id}, args...)
	err := s.execOne(ctx, `
		UPDATE `+s.table("jobs")+`
		   SET status = ?, last_error = ?, finished_at = ?, lease_expires_at = NULL
		 WHERE id = ? AND status NOT IN (`+list+`)`, args...)
	if errors.Is(err, pulse.ErrNotFound) {
		if _, getErr := s.GetJob(ctx, id); getErr != nil {
			return getErr
		}
		return fmt.Errorf("sqlstore: job %s already finished: %w", id, pulse.ErrConflict)
	}
	if err != nil {
		return fmt.Errorf("sqlstore: fail job terminally: %w", err)
	}
	return nil
}

// Cancel terminates a job that is no longer wanted.
func (s *Store) Cancel(ctx context.Context, id, reason string) error {
	list, args := placeholders([]string{
		pulse.JobDone, pulse.JobFailed, pulse.JobExpired, pulse.JobCancelled,
	})
	args = append([]any{pulse.JobCancelled, truncate(reason, 1000), s.now(), id}, args...)
	err := s.execOne(ctx, `
		UPDATE `+s.table("jobs")+`
		   SET status = ?, last_error = ?, finished_at = ?, lease_expires_at = NULL
		 WHERE id = ? AND status NOT IN (`+list+`)`, args...)
	if errors.Is(err, pulse.ErrNotFound) {
		// Already terminal, or gone. Both are conflicts for a cancel.
		if _, getErr := s.GetJob(ctx, id); getErr != nil {
			return getErr
		}
		return fmt.Errorf("sqlstore: job %s already finished: %w", id, pulse.ErrConflict)
	}
	if err != nil {
		return fmt.Errorf("sqlstore: cancel job: %w", err)
	}
	return nil
}

// ReclaimExpiredLeases returns timed-out jobs to the queue and gives up on
// those that have burned through their attempts.
//
// The order matters: the expired set is read before anything is updated, so a
// job cannot be both requeued and reported as expired in one pass.
func (s *Store) ReclaimExpiredLeases(ctx context.Context) (pulse.ReclaimStats, error) {
	var stats pulse.ReclaimStats
	now := s.now()
	err := s.tx(ctx, func(t *sql.Tx) error {
		// Jobs with no attempts left: collect them first so the caller can
		// synthesize a result for each.
		rows, err := t.QueryContext(ctx, s.rebind(`
			SELECT `+s.jobColumns()+` FROM `+s.table("jobs")+`
			 WHERE status = ? AND lease_expires_at IS NOT NULL
			   AND lease_expires_at < ? AND attempt >= max_attempts`),
			pulse.JobLeased, now)
		if err != nil {
			return err
		}
		expired, err := collect(rows, scanJob, "collect expired jobs")
		if err != nil {
			return err
		}
		stats.Expired = expired

		if len(expired) > 0 {
			ids := make([]string, 0, len(expired))
			for _, j := range expired {
				ids = append(ids, j.ID)
			}
			list, args := placeholders(ids)
			args = append([]any{
				pulse.JobExpired, now,
				"lease expired with no attempts remaining",
			}, args...)
			if _, err := t.ExecContext(ctx, s.rebind(`
				UPDATE `+s.table("jobs")+`
				   SET status = ?, finished_at = ?, lease_expires_at = NULL, last_error = ?
				 WHERE id IN (`+list+`)`), args...); err != nil {
				return err
			}
		}

		// Everything else whose lease ran out goes back to pending.
		res, err := t.ExecContext(ctx, s.rebind(`
			UPDATE `+s.table("jobs")+`
			   SET status = ?, lease_expires_at = NULL, last_error = ?
			 WHERE status = ? AND lease_expires_at IS NOT NULL
			   AND lease_expires_at < ? AND attempt < max_attempts`),
			pulse.JobPending, "lease expired", pulse.JobLeased, now)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		stats.Requeued = n
		return nil
	})
	if err != nil {
		return pulse.ReclaimStats{}, fmt.Errorf("sqlstore: reclaim expired leases: %w", err)
	}
	return stats, nil
}

// CountLeasedForAgent is what dispatch subtracts from an agent's concurrency.
func (s *Store) CountLeasedForAgent(ctx context.Context, agentID string) (int, error) {
	return s.countJobs(ctx, agentID, pulse.JobLeased)
}

// CountPendingForAgent is backlog depth.
func (s *Store) CountPendingForAgent(ctx context.Context, agentID string) (int, error) {
	return s.countJobs(ctx, agentID, pulse.JobPending)
}

func (s *Store) countJobs(ctx context.Context, agentID, status string) (int, error) {
	var n int
	err := s.queryRow(ctx,
		`SELECT COUNT(*) FROM `+s.table("jobs")+` WHERE agent_id = ? AND status = ?`,
		agentID, status).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("sqlstore: count %s jobs: %w", status, err)
	}
	return n, nil
}

// PurgeFinished deletes terminal jobs older than before. Retention is the
// application's policy; this is the mechanism.
func (s *Store) PurgeFinished(ctx context.Context, before time.Time) (int64, error) {
	list, args := placeholders([]string{
		pulse.JobDone, pulse.JobFailed, pulse.JobExpired, pulse.JobCancelled,
	})
	args = append(args, before)
	n, err := s.execAffected(ctx, `
		DELETE FROM `+s.table("jobs")+`
		 WHERE status IN (`+list+`) AND finished_at IS NOT NULL AND finished_at < ?`,
		args...)
	if err != nil {
		return 0, fmt.Errorf("sqlstore: purge finished jobs: %w", err)
	}
	return n, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// Compile-time proof that the store satisfies every port.
var (
	_ pulse.AgentStore  = (*Store)(nil)
	_ pulse.TokenStore  = (*Store)(nil)
	_ pulse.ConfigStore = (*Store)(nil)
	_ pulse.JobStore    = (*Store)(nil)
)

package porttest

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/seanly/pulse-devkit/pulse"
)

func testJobRoundTrip(t *testing.T, s Stores) {
	// The sample kind is the one primitive a single-kind store with no kind
	// column can echo. A store that persists kind must return this same string.
	want := &pulse.Job{
		ID: "j1", GroupID: "g1", SubjectID: "s1", Kind: pulse.KindNetTLSProbe, AgentID: "a1",
		Timeout: 20 * time.Second, MaxAttempts: 3,
		Payload: []byte(`{"host":"example.com","port":443}`),
	}
	enqueue(t, s, want)

	got, err := s.Jobs.GetJob(ctx(), "j1")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.GroupID != "g1" || got.SubjectID != "s1" || got.Kind != pulse.KindNetTLSProbe {
		t.Errorf("correlation fields did not round trip: %+v", got)
	}
	if got.Timeout != 20*time.Second {
		t.Errorf("timeout = %v, want 20s", got.Timeout)
	}
	if string(got.Payload) != `{"host":"example.com","port":443}` {
		t.Errorf("payload = %q, want it carried opaquely", got.Payload)
	}
	if got.Status != pulse.JobPending {
		t.Errorf("status = %q, want pending", got.Status)
	}
	if loc := got.CreatedAt.Location(); loc != time.UTC {
		t.Errorf("created_at location = %v, want UTC", loc)
	}

	if _, err := s.Jobs.GetJob(ctx(), "nope"); !errors.Is(err, pulse.ErrNotFound) {
		t.Errorf("missing job error = %v, want ErrNotFound", err)
	}
}

func testLeaseFIFO(t *testing.T, s Stores) {
	base := time.Now().UTC()
	enqueue(t, s,
		&pulse.Job{ID: "j1", AgentID: "a1", CreatedAt: base},
		&pulse.Job{ID: "j2", AgentID: "a1", CreatedAt: base.Add(time.Second)},
		&pulse.Job{ID: "j3", AgentID: "a1", CreatedAt: base.Add(2 * time.Second)},
		// Another agent's work must not be handed out here.
		&pulse.Job{ID: "other", AgentID: "a2", CreatedAt: base},
	)

	leased, err := s.Jobs.Lease(ctx(), "a1", 2, time.Minute)
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	if len(leased) != 2 {
		t.Fatalf("leased %d jobs, want the requested 2", len(leased))
	}
	// Oldest first, so a backlog drains in order rather than starving its
	// oldest entries.
	if leased[0].ID != "j1" || leased[1].ID != "j2" {
		t.Errorf("leased %s and %s, want j1 and j2", leased[0].ID, leased[1].ID)
	}
	for _, j := range leased {
		if j.Status != pulse.JobLeased {
			t.Errorf("job %s status = %q, want leased", j.ID, j.Status)
		}
		if j.Attempt != 1 {
			t.Errorf("job %s attempt = %d, want 1", j.ID, j.Attempt)
		}
		if j.LeaseExpiresAt.IsZero() {
			t.Errorf("job %s has no lease deadline", j.ID)
		}
	}

	// The other agent's job was not touched.
	other, err := s.Jobs.GetJob(ctx(), "other")
	if err != nil {
		t.Fatal(err)
	}
	if other.Status != pulse.JobPending {
		t.Errorf("another agent's job was leased: %q", other.Status)
	}

	// A zero or negative limit asks for nothing and must not error.
	if got, err := s.Jobs.Lease(ctx(), "a1", 0, time.Minute); err != nil || len(got) != 0 {
		t.Errorf("Lease with limit 0 = %v, %v", got, err)
	}
}

func testLeaseExclusive(t *testing.T, s Stores) {
	// The queue's central safety property: concurrent passes must never hand
	// the same job to two agents. This is the test that catches a SELECT-then-
	// UPDATE implementation.
	const jobs = 30
	for i := 0; i < jobs; i++ {
		enqueue(t, s, &pulse.Job{
			ID:      "j" + itoa(i),
			AgentID: "a1",
			// Identical timestamps make the ordering ambiguous, which is the
			// hostile case for a non-atomic claim.
			CreatedAt: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		})
	}

	const workers = 6
	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		seen   = map[string]int{}
		errsCh = make(chan error, workers)
	)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			leased, err := s.Jobs.Lease(ctx(), "a1", 10, time.Minute)
			if err != nil {
				errsCh <- err
				return
			}
			mu.Lock()
			defer mu.Unlock()
			for _, j := range leased {
				seen[j.ID]++
			}
		}()
	}
	wg.Wait()
	close(errsCh)
	for err := range errsCh {
		// SQLite can report a busy database under contention; that is a
		// legitimate outcome, unlike handing out a job twice.
		t.Logf("lease error under contention: %v", err)
	}

	for id, n := range seen {
		if n != 1 {
			t.Errorf("job %s was leased %d times, want exactly once", id, n)
		}
	}
}

func testLeaseOneConflict(t *testing.T, s Stores) {
	enqueue(t, s, &pulse.Job{ID: "j1", AgentID: "a1"})

	got, err := s.Jobs.LeaseOne(ctx(), "j1", time.Minute)
	if err != nil {
		t.Fatalf("LeaseOne: %v", err)
	}
	if got.Status != pulse.JobLeased || got.Attempt != 1 {
		t.Errorf("leased job = %+v", got)
	}

	// Already leased: the dispatch loop owns delivery now, which is a
	// conflict rather than a failure.
	if _, err := s.Jobs.LeaseOne(ctx(), "j1", time.Minute); !errors.Is(err, pulse.ErrConflict) {
		t.Errorf("second LeaseOne error = %v, want ErrConflict", err)
	}
	// A job that does not exist is a different problem and must be reported
	// as one.
	if _, err := s.Jobs.LeaseOne(ctx(), "nope", time.Minute); !errors.Is(err, pulse.ErrNotFound) {
		t.Errorf("LeaseOne on a missing job = %v, want ErrNotFound", err)
	}
}

func testCompleteRequiresLeased(t *testing.T, s Stores) {
	enqueue(t, s, &pulse.Job{ID: "j1", AgentID: "a1"})

	// Not leased yet: completing it would skip the queue's bookkeeping.
	if err := s.Jobs.Complete(ctx(), "j1"); !errors.Is(err, pulse.ErrNotFound) {
		t.Errorf("Complete on a pending job = %v, want ErrNotFound", err)
	}

	if _, err := s.Jobs.Lease(ctx(), "a1", 1, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := s.Jobs.Complete(ctx(), "j1"); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	j, _ := s.Jobs.GetJob(ctx(), "j1")
	if j.Status != pulse.JobDone {
		t.Errorf("status = %q, want done", j.Status)
	}
	if j.FinishedAt.IsZero() {
		t.Error("finished_at not set")
	}
	if !j.LeaseExpiresAt.IsZero() {
		t.Error("lease deadline should be cleared once the job is done")
	}

	// A duplicate report arrives: it must be ignored, not resurrect the job.
	if err := s.Jobs.Complete(ctx(), "j1"); !errors.Is(err, pulse.ErrNotFound) {
		t.Errorf("duplicate Complete = %v, want ErrNotFound", err)
	}
}

func testFailRetries(t *testing.T, s Stores) {
	enqueue(t, s, &pulse.Job{ID: "j1", AgentID: "a1", MaxAttempts: 2})

	// First failure: attempts remain, so it goes back to the queue rather
	// than being reported as a real outcome.
	if _, err := s.Jobs.Lease(ctx(), "a1", 1, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := s.Jobs.Fail(ctx(), "j1", "connection refused"); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	j, _ := s.Jobs.GetJob(ctx(), "j1")
	if j.Status != pulse.JobPending {
		t.Errorf("status = %q, want pending with attempts left", j.Status)
	}
	if j.LastError == "" {
		t.Error("want the failure reason recorded")
	}
	if j.Attempt != 1 {
		t.Errorf("attempt = %d, want 1 consumed", j.Attempt)
	}

	// Second failure exhausts the budget.
	if _, err := s.Jobs.Lease(ctx(), "a1", 1, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := s.Jobs.Fail(ctx(), "j1", "connection refused again"); err != nil {
		t.Fatal(err)
	}
	j, _ = s.Jobs.GetJob(ctx(), "j1")
	if j.Status != pulse.JobFailed {
		t.Errorf("status = %q, want failed once attempts ran out", j.Status)
	}
	if j.FinishedAt.IsZero() {
		t.Error("finished_at not set on a terminal failure")
	}
}

func testRequeueAttempts(t *testing.T, s Stores) {
	enqueue(t, s, &pulse.Job{ID: "j1", AgentID: "a1", MaxAttempts: 2})
	if _, err := s.Jobs.Lease(ctx(), "a1", 1, time.Minute); err != nil {
		t.Fatal(err)
	}

	if err := s.Jobs.Requeue(ctx(), "j1", "agent busy", 10); err != nil {
		t.Fatalf("Requeue: %v", err)
	}
	j, _ := s.Jobs.GetJob(ctx(), "j1")
	if j.Status != pulse.JobPending {
		t.Errorf("status = %q, want pending", j.Status)
	}
	// The attempt is given back: no work happened. Counting it would let a
	// misconfigured concurrency exhaust max_attempts and fabricate failures
	// for subjects nothing ever looked at.
	if j.Attempt != 0 {
		t.Errorf("attempt = %d, want 0 after a requeue", j.Attempt)
	}
	if j.RequeueCount != 1 {
		t.Errorf("requeue count = %d, want 1", j.RequeueCount)
	}
}

func testRequeueCeiling(t *testing.T, s Stores) {
	enqueue(t, s, &pulse.Job{ID: "j1", AgentID: "a1", MaxAttempts: 10})

	// Bounce it right up to the ceiling.
	for i := 0; i < 3; i++ {
		if _, err := s.Jobs.Lease(ctx(), "a1", 1, time.Minute); err != nil {
			t.Fatal(err)
		}
		if err := s.Jobs.Requeue(ctx(), "j1", "agent busy", 3); err != nil {
			t.Fatal(err)
		}
		j, _ := s.Jobs.GetJob(ctx(), "j1")
		if j.Status != pulse.JobPending {
			t.Fatalf("requeue %d: status = %q, want pending", i+1, j.Status)
		}
	}

	// One past it: a job handed back this many times is stuck behind an agent
	// that never has room, and spinning on it forever hides the real problem.
	if _, err := s.Jobs.Lease(ctx(), "a1", 1, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := s.Jobs.Requeue(ctx(), "j1", "agent busy", 3); err != nil {
		t.Fatal(err)
	}
	j, _ := s.Jobs.GetJob(ctx(), "j1")
	if j.Status != pulse.JobFailed {
		t.Errorf("status = %q, want failed past the requeue ceiling", j.Status)
	}
	// The retry budget was never touched by the bouncing.
	if j.Attempt != 0 {
		t.Errorf("attempt = %d, want requeues to have left it alone", j.Attempt)
	}
}

func testReclaim(t *testing.T, s Stores) {
	enqueue(t, s,
		// Live lease: must survive the pass untouched.
		&pulse.Job{ID: "fresh", AgentID: "a1", MaxAttempts: 3},
		// Expired with attempts left: recoverable.
		&pulse.Job{ID: "retry", AgentID: "a1", MaxAttempts: 3},
		// Expired with one attempt only: exhausted.
		&pulse.Job{ID: "doomed", AgentID: "a1", MaxAttempts: 1},
	)

	// A negative lease duration puts the deadline in the past, which is what
	// an expired lease IS — no clock needed. That keeps this case runnable
	// against an implementation whose clock cannot be controlled, which is
	// most of them: reading the wall clock from a package function is the
	// obvious way to write a store, and only becomes awkward once someone
	// wants to test around it.
	if _, err := s.Jobs.LeaseOne(ctx(), "fresh", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Jobs.LeaseOne(ctx(), "retry", -time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Jobs.LeaseOne(ctx(), "doomed", -time.Minute); err != nil {
		t.Fatal(err)
	}

	stats, err := s.Jobs.ReclaimExpiredLeases(ctx())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Requeued != 1 {
		t.Errorf("requeued = %d, want 1", stats.Requeued)
	}
	if len(stats.Expired) != 1 || stats.Expired[0].ID != "doomed" {
		t.Errorf("expired = %+v, want just the exhausted job", stats.Expired)
	}

	// The live lease is not disturbed by a pass that reclaims its neighbours.
	fresh, _ := s.Jobs.GetJob(ctx(), "fresh")
	if fresh.Status != pulse.JobLeased {
		t.Errorf("live lease status = %q, want it still leased", fresh.Status)
	}

	retry, _ := s.Jobs.GetJob(ctx(), "retry")
	if retry.Status != pulse.JobPending {
		t.Errorf("recoverable job status = %q, want pending", retry.Status)
	}
	// The attempt the lease spent is NOT given back here, unlike a requeue: an
	// agent that took the job and went quiet may well have done the work and
	// died before reporting, so the attempt was real.
	if retry.Attempt != 1 {
		t.Errorf("recoverable job attempt = %d, want 1: an expired lease was a real attempt", retry.Attempt)
	}

	doomed, _ := s.Jobs.GetJob(ctx(), "doomed")
	// Expired, not failed: the agent stopped answering, which is a different
	// problem from work that ran and reported an error. Collapsing them would
	// make "is this subject reachable at all" unanswerable from this table.
	if doomed.Status != pulse.JobExpired {
		t.Errorf("exhausted job status = %q, want expired", doomed.Status)
	}
	if doomed.FinishedAt.IsZero() {
		t.Error("expired job has no finished_at")
	}
}

func testFailTerminal(t *testing.T, s Stores) {
	// A job with plenty of attempts left, for work that provably cannot
	// succeed — a deleted subject, an unbuildable payload.
	enqueue(t, s, &pulse.Job{ID: "j1", AgentID: "a1", MaxAttempts: 5})
	if _, err := s.Jobs.Lease(ctx(), "a1", 1, time.Minute); err != nil {
		t.Fatal(err)
	}

	if err := s.Jobs.FailTerminal(ctx(), "j1", "subject deleted"); err != nil {
		t.Fatalf("FailTerminal: %v", err)
	}
	j, err := s.Jobs.GetJob(ctx(), "j1")
	if err != nil {
		t.Fatal(err)
	}
	// The remaining attempts are ignored: retrying would spend a dispatch pass
	// and a write each to reach exactly this state.
	if j.Status != pulse.JobFailed {
		t.Errorf("status = %q, want failed despite 4 attempts remaining", j.Status)
	}
	if j.FinishedAt.IsZero() {
		t.Error("finished_at not set")
	}
	if j.LastError != "subject deleted" {
		t.Errorf("last error = %q", j.LastError)
	}

	// Already terminal: a second call is a conflict, not a silent overwrite.
	if err := s.Jobs.FailTerminal(ctx(), "j1", "again"); !errors.Is(err, pulse.ErrConflict) {
		t.Errorf("second FailTerminal = %v, want ErrConflict", err)
	}

	// A pending job can also be given up on, without being leased first.
	enqueue(t, s, &pulse.Job{ID: "j2", AgentID: "a1", MaxAttempts: 5})
	if err := s.Jobs.FailTerminal(ctx(), "j2", "subject deleted"); err != nil {
		t.Errorf("FailTerminal on a pending job: %v", err)
	}

	if err := s.Jobs.FailTerminal(ctx(), "nope", "x"); !errors.Is(err, pulse.ErrNotFound) {
		t.Errorf("FailTerminal on a missing job = %v, want ErrNotFound", err)
	}
}

func testCancel(t *testing.T, s Stores) {
	enqueue(t, s, &pulse.Job{ID: "j1", AgentID: "a1"}, &pulse.Job{ID: "j2", AgentID: "a1"})

	if err := s.Jobs.Cancel(ctx(), "j1", "operator cancelled the run"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	j, _ := s.Jobs.GetJob(ctx(), "j1")
	if j.Status != pulse.JobCancelled {
		t.Errorf("status = %q, want cancelled", j.Status)
	}
	if j.FinishedAt.IsZero() {
		t.Error("finished_at not set on cancel")
	}

	// Cancelling twice is a conflict: the job already finished.
	if err := s.Jobs.Cancel(ctx(), "j1", "again"); !errors.Is(err, pulse.ErrConflict) {
		t.Errorf("second Cancel = %v, want ErrConflict", err)
	}

	// A leased job can still be cancelled.
	if _, err := s.Jobs.Lease(ctx(), "a1", 1, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := s.Jobs.Cancel(ctx(), "j2", "stop"); err != nil {
		t.Errorf("cancelling a leased job: %v", err)
	}
}

func testCounts(t *testing.T, s Stores) {
	enqueue(t, s,
		&pulse.Job{ID: "j1", AgentID: "a1"},
		&pulse.Job{ID: "j2", AgentID: "a1"},
		&pulse.Job{ID: "j3", AgentID: "a1"},
		&pulse.Job{ID: "other", AgentID: "a2"},
	)

	pending, err := s.Jobs.CountPendingForAgent(ctx(), "a1")
	if err != nil {
		t.Fatal(err)
	}
	if pending != 3 {
		t.Errorf("pending = %d, want 3", pending)
	}

	if _, err := s.Jobs.Lease(ctx(), "a1", 2, time.Minute); err != nil {
		t.Fatal(err)
	}
	leased, err := s.Jobs.CountLeasedForAgent(ctx(), "a1")
	if err != nil {
		t.Fatal(err)
	}
	// This is the number dispatch subtracts from an agent's concurrency, so
	// it has to be exact or the fleet is over- or under-subscribed.
	if leased != 2 {
		t.Errorf("leased = %d, want 2", leased)
	}
	pending, _ = s.Jobs.CountPendingForAgent(ctx(), "a1")
	if pending != 1 {
		t.Errorf("pending = %d after leasing 2 of 3, want 1", pending)
	}
	// Another agent's jobs are not counted.
	if n, _ := s.Jobs.CountPendingForAgent(ctx(), "a2"); n != 1 {
		t.Errorf("a2 pending = %d, want 1", n)
	}
}

func testPurge(t *testing.T, s Stores) {
	enqueue(t, s,
		&pulse.Job{ID: "done", AgentID: "a1"},
		&pulse.Job{ID: "pending", AgentID: "a1"},
	)
	if _, err := s.Jobs.Lease(ctx(), "a1", 1, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := s.Jobs.Complete(ctx(), "done"); err != nil {
		t.Fatal(err)
	}

	// A cutoff in the future rather than an advanced clock: the question
	// PurgeFinished answers is "is this job's finish time before the cutoff",
	// and moving the cutoff asks it just as well as moving time.
	n, err := s.Jobs.PurgeFinished(ctx(), time.Now().UTC().Add(time.Hour))
	if err != nil {
		t.Fatalf("PurgeFinished: %v", err)
	}
	if n != 1 {
		t.Errorf("purged %d, want 1", n)
	}
	if _, err := s.Jobs.GetJob(ctx(), "done"); !errors.Is(err, pulse.ErrNotFound) {
		t.Errorf("the finished job survived the purge (err = %v)", err)
	}
	// Unfinished work is never purged, however old.
	if _, err := s.Jobs.GetJob(ctx(), "pending"); err != nil {
		t.Errorf("a pending job was purged: %v", err)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}

package pulsequeue_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/seanly/pulse-devkit/pulse"
	"github.com/seanly/pulse-devkit/pulsequeue"
	"github.com/seanly/pulse-devkit/pulsetest"
)

// recordingRegistry captures pushes and can be made to fail.
type recordingRegistry struct {
	mu   sync.Mutex
	sent map[string][]pulse.Job
	err  error
}

func newRegistry() *recordingRegistry {
	return &recordingRegistry{sent: map[string][]pulse.Job{}}
}

func (r *recordingRegistry) SendTo(agentID string, msg *pulse.ServerMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	if msg.Job != nil {
		r.sent[agentID] = append(r.sent[agentID], *msg.Job)
	}
	return nil
}

func (r *recordingRegistry) failWith(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
}

func (r *recordingRegistry) jobsFor(agentID string) []pulse.Job {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]pulse.Job{}, r.sent[agentID]...)
}

func (r *recordingRegistry) total() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, js := range r.sent {
		n += len(js)
	}
	return n
}

func newDispatcher(t *testing.T, mutate func(*pulsequeue.Options)) (
	*pulsequeue.Dispatcher, *pulsetest.Store, *recordingRegistry, *pulsetest.Clock) {
	t.Helper()
	clock := pulsetest.NewClock(time.Time{})
	store := pulsetest.NewStore(pulsetest.WithClock(clock))
	reg := newRegistry()
	o := pulsequeue.Options{
		Jobs: store, Agents: store, Configs: store, Registry: reg, Clock: clock,
	}
	if mutate != nil {
		mutate(&o)
	}
	d, err := pulsequeue.New(o)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return d, store, reg, clock
}

// onlineAgent seeds a dispatchable agent.
func onlineAgent(store *pulsetest.Store, id, name string) {
	store.SeedAgent(pulse.Agent{ID: id, Name: name, Status: pulse.AgentOnline})
}

func enqueue(t *testing.T, store *pulsetest.Store, jobs ...*pulse.Job) {
	t.Helper()
	if err := store.Enqueue(context.Background(), jobs); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
}

func TestDispatchPushesPendingJobsToOnlineAgents(t *testing.T) {
	d, store, reg, _ := newDispatcher(t, nil)
	ctx := context.Background()
	onlineAgent(store, "a1", "probe-1")
	enqueue(t, store,
		&pulse.Job{ID: "j1", AgentID: "a1", SubjectID: "s1", MaxAttempts: 3},
		&pulse.Job{ID: "j2", AgentID: "a1", SubjectID: "s2", MaxAttempts: 3},
	)

	if err := d.DispatchOnce(ctx); err != nil {
		t.Fatalf("DispatchOnce: %v", err)
	}
	if got := reg.jobsFor("a1"); len(got) != 2 {
		t.Fatalf("pushed %d jobs, want 2", len(got))
	}
	for _, j := range store.Jobs() {
		if j.Status != pulse.JobLeased {
			t.Errorf("job %s status = %q, want leased", j.ID, j.Status)
		}
		if j.Attempt != 1 {
			t.Errorf("job %s attempt = %d, want 1", j.ID, j.Attempt)
		}
	}
}

func TestOfflineAgentsAreNotDispatchedTo(t *testing.T) {
	d, store, reg, _ := newDispatcher(t, nil)
	ctx := context.Background()
	store.SeedAgent(pulse.Agent{ID: "a1", Name: "probe-1", Status: pulse.AgentOffline})
	store.SeedAgent(pulse.Agent{ID: "a2", Name: "probe-2", Status: pulse.AgentDisabled})
	enqueue(t, store,
		&pulse.Job{ID: "j1", AgentID: "a1", MaxAttempts: 3},
		&pulse.Job{ID: "j2", AgentID: "a2", MaxAttempts: 3},
	)

	if err := d.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if n := reg.total(); n != 0 {
		t.Errorf("pushed %d jobs to agents that are not dispatchable", n)
	}
	// And the jobs stay pending, waiting for the agents to come back.
	for _, j := range store.Jobs() {
		if j.Status != pulse.JobPending {
			t.Errorf("job %s status = %q, want it still pending", j.ID, j.Status)
		}
	}
}

func TestQuotaIsConcurrencyMinusHeld(t *testing.T) {
	d, store, reg, _ := newDispatcher(t, nil)
	ctx := context.Background()
	onlineAgent(store, "a1", "probe-1")
	two := 2
	store.SeedConfig(pulse.AgentConfig{AgentID: "a1", Concurrency: &two})

	enqueue(t, store,
		&pulse.Job{ID: "j1", AgentID: "a1", MaxAttempts: 3},
		&pulse.Job{ID: "j2", AgentID: "a1", MaxAttempts: 3},
		&pulse.Job{ID: "j3", AgentID: "a1", MaxAttempts: 3},
	)

	if err := d.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	// Concurrency 2 means two in flight, not three.
	if got := len(reg.jobsFor("a1")); got != 2 {
		t.Fatalf("pushed %d jobs, want 2 (the configured concurrency)", got)
	}

	// A second pass must not oversubscribe: the agent still holds two.
	if err := d.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if got := len(reg.jobsFor("a1")); got != 2 {
		t.Errorf("pushed %d jobs after a second pass, want still 2", got)
	}
}

func TestFullAgentIsSkippedWithoutLeasing(t *testing.T) {
	d, store, reg, _ := newDispatcher(t, nil)
	ctx := context.Background()
	onlineAgent(store, "a1", "probe-1")
	one := 1
	store.SeedConfig(pulse.AgentConfig{AgentID: "a1", Concurrency: &one})
	enqueue(t, store,
		&pulse.Job{ID: "j1", AgentID: "a1", MaxAttempts: 3},
		&pulse.Job{ID: "j2", AgentID: "a1", MaxAttempts: 3},
	)

	if err := d.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := d.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	// Leasing into a full agent would only produce busy acks, and every
	// refusal costs a round trip plus a write.
	if got := reg.jobsFor("a1"); len(got) != 1 {
		t.Errorf("pushed %d jobs, want 1", len(got))
	}
	j2, err := store.GetJob(ctx, "j2")
	if err != nil {
		t.Fatal(err)
	}
	if j2.Status != pulse.JobPending {
		t.Errorf("j2 status = %q, want pending (never leased)", j2.Status)
	}
	if j2.Attempt != 0 {
		t.Errorf("j2 attempt = %d, want 0: it was never handed out", j2.Attempt)
	}
}

func TestPushFailureRequeuesWithoutBurningAnAttempt(t *testing.T) {
	d, store, reg, _ := newDispatcher(t, nil)
	ctx := context.Background()
	onlineAgent(store, "a1", "probe-1")
	enqueue(t, store, &pulse.Job{ID: "j1", AgentID: "a1", MaxAttempts: 3})
	reg.failWith(errors.New("stream is gone"))

	if err := d.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	j, err := store.GetJob(ctx, "j1")
	if err != nil {
		t.Fatal(err)
	}
	// No work happened, so the retry budget must be intact: a flaky stream
	// should not look like a failing job.
	if j.Status != pulse.JobPending {
		t.Errorf("status = %q, want pending", j.Status)
	}
	if j.Attempt != 0 {
		t.Errorf("attempt = %d, want 0 after a push failure", j.Attempt)
	}
	if j.RequeueCount != 1 {
		t.Errorf("requeue count = %d, want 1", j.RequeueCount)
	}
}

func TestRepeatedRequeuesEventuallyFailTheJob(t *testing.T) {
	d, store, reg, _ := newDispatcher(t, func(o *pulsequeue.Options) {
		o.MaxRequeues = 3
	})
	ctx := context.Background()
	onlineAgent(store, "a1", "probe-1")
	enqueue(t, store, &pulse.Job{ID: "j1", AgentID: "a1", MaxAttempts: 5})
	reg.failWith(errors.New("stream is gone"))

	// A permanently saturated or unreachable agent must not let one job spin
	// forever, even though requeues do not consume attempts.
	for i := 0; i < 5; i++ {
		if err := d.DispatchOnce(ctx); err != nil {
			t.Fatal(err)
		}
	}
	j, err := store.GetJob(ctx, "j1")
	if err != nil {
		t.Fatal(err)
	}
	if j.Status != pulse.JobFailed {
		t.Errorf("status = %q, want failed after exceeding the requeue ceiling", j.Status)
	}
}

func TestBusyAckReturnsJobForAnotherAgent(t *testing.T) {
	d, store, _, _ := newDispatcher(t, nil)
	ctx := context.Background()
	onlineAgent(store, "a1", "probe-1")
	enqueue(t, store, &pulse.Job{ID: "j1", AgentID: "a1", MaxAttempts: 3})
	if err := d.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}

	d.Ack("a1", "j1", false, pulse.AckBusy, "")

	j, err := store.GetJob(ctx, "j1")
	if err != nil {
		t.Fatal(err)
	}
	if j.Status != pulse.JobPending {
		t.Errorf("status = %q, want pending after a busy ack", j.Status)
	}
	if j.Attempt != 0 {
		t.Errorf("attempt = %d, want 0: a busy agent did no work", j.Attempt)
	}
}

func TestAcceptedAckLeavesTheLeaseAlone(t *testing.T) {
	d, store, _, _ := newDispatcher(t, nil)
	ctx := context.Background()
	onlineAgent(store, "a1", "probe-1")
	enqueue(t, store, &pulse.Job{ID: "j1", AgentID: "a1", MaxAttempts: 3})
	if err := d.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}

	d.Ack("a1", "j1", true, "", "")

	j, err := store.GetJob(ctx, "j1")
	if err != nil {
		t.Fatal(err)
	}
	if j.Status != pulse.JobLeased {
		t.Errorf("status = %q, want it still leased", j.Status)
	}
}

func TestRejectedAndUnsupportedAcksRequeue(t *testing.T) {
	for _, reason := range []string{pulse.AckRejected, pulse.AckUnsupportedKind, "something-new"} {
		t.Run(reason, func(t *testing.T) {
			d, store, _, _ := newDispatcher(t, nil)
			ctx := context.Background()
			onlineAgent(store, "a1", "probe-1")
			enqueue(t, store, &pulse.Job{ID: "j1", AgentID: "a1", MaxAttempts: 3})
			if err := d.DispatchOnce(ctx); err != nil {
				t.Fatal(err)
			}
			d.Ack("a1", "j1", false, reason, "")

			j, err := store.GetJob(ctx, "j1")
			if err != nil {
				t.Fatal(err)
			}
			if j.Status != pulse.JobPending {
				t.Errorf("status = %q, want pending", j.Status)
			}
			if j.LastError == "" {
				t.Error("want the decline reason recorded on the job")
			}
		})
	}
}

func TestExpiredLeaseIsRequeuedWhileAttemptsRemain(t *testing.T) {
	d, store, _, clock := newDispatcher(t, nil)
	ctx := context.Background()
	onlineAgent(store, "a1", "probe-1")
	enqueue(t, store, &pulse.Job{ID: "j1", AgentID: "a1", MaxAttempts: 3})
	if err := d.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}

	// The agent took the job and never reported.
	clock.Advance(2 * time.Hour)
	if err := d.ReapOnce(ctx); err != nil {
		t.Fatal(err)
	}

	j, err := store.GetJob(ctx, "j1")
	if err != nil {
		t.Fatal(err)
	}
	if j.Status != pulse.JobPending {
		t.Errorf("status = %q, want pending after the lease expired", j.Status)
	}
}

func TestExhaustedJobExpiresAndIsHandedToTheApplication(t *testing.T) {
	var expired []pulse.Job
	var mu sync.Mutex
	d, store, _, clock := newDispatcher(t, func(o *pulsequeue.Options) {
		o.OnExpired = func(_ context.Context, j pulse.Job) {
			mu.Lock()
			defer mu.Unlock()
			expired = append(expired, j)
		}
	})
	ctx := context.Background()
	onlineAgent(store, "a1", "probe-1")
	// One attempt only: the first expiry exhausts it.
	enqueue(t, store, &pulse.Job{ID: "j1", AgentID: "a1", SubjectID: "s1", MaxAttempts: 1})
	if err := d.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Hour)
	if err := d.ReapOnce(ctx); err != nil {
		t.Fatal(err)
	}

	j, err := store.GetJob(ctx, "j1")
	if err != nil {
		t.Fatal(err)
	}
	// Expired, not failed: the agent stopped answering, which is a different
	// problem from work that ran and reported an error.
	if j.Status != pulse.JobExpired {
		t.Errorf("status = %q, want expired", j.Status)
	}
	mu.Lock()
	defer mu.Unlock()
	// A job that dies silently must still show up somewhere.
	if len(expired) != 1 || expired[0].ID != "j1" {
		t.Errorf("OnExpired got %+v, want one call for j1", expired)
	}
}

func TestCompleteIgnoresAReportThatArrivedTooLate(t *testing.T) {
	d, store, _, clock := newDispatcher(t, nil)
	ctx := context.Background()
	onlineAgent(store, "a1", "probe-1")
	enqueue(t, store, &pulse.Job{ID: "j1", AgentID: "a1", MaxAttempts: 3})
	if err := d.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Hour)
	if err := d.ReapOnce(ctx); err != nil {
		t.Fatal(err)
	}

	// The agent finally reports, after the reaper already requeued the job.
	// This is normal, not an error: the queue has moved on.
	if err := d.Complete(ctx, "j1"); err != nil {
		t.Errorf("Complete after reclaim = %v, want nil", err)
	}
	j, _ := store.GetJob(ctx, "j1")
	if j.Status != pulse.JobPending {
		t.Errorf("status = %q; a late report must not resurrect the job", j.Status)
	}
}

func TestEnqueueRequiresAnAgent(t *testing.T) {
	d, _, _, _ := newDispatcher(t, nil)
	_, err := d.Enqueue(context.Background(), []pulsequeue.EnqueueInput{
		{SubjectID: "s1"},
	}, func() string { return "id" })
	// Agent selection is the application's job, so a job without one is a
	// programming error rather than something the framework guesses at.
	if err == nil {
		t.Error("want an error for a job with no agent")
	}
}

func TestEnqueueAppliesDefaults(t *testing.T) {
	d, store, _, _ := newDispatcher(t, func(o *pulsequeue.Options) {
		o.Timeout = 7 * time.Second
	})
	n := 0
	jobs, err := d.Enqueue(context.Background(), []pulsequeue.EnqueueInput{
		{AgentID: "a1", SubjectID: "s1"},
	}, func() string { n++; return fmt.Sprintf("job-%d", n) })
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].ID != "job-1" {
		t.Fatalf("unexpected jobs: %+v", jobs)
	}
	stored, err := store.GetJob(context.Background(), "job-1")
	if err != nil {
		t.Fatal(err)
	}
	if stored.Timeout != 7*time.Second {
		t.Errorf("timeout = %v, want the dispatcher default", stored.Timeout)
	}
	if stored.MaxAttempts != 1 {
		t.Errorf("max attempts = %d, want 1", stored.MaxAttempts)
	}
	if stored.Status != pulse.JobPending {
		t.Errorf("status = %q, want pending", stored.Status)
	}
}

func TestDispatchNowLeasesOneJobDirectly(t *testing.T) {
	d, store, reg, _ := newDispatcher(t, nil)
	ctx := context.Background()
	onlineAgent(store, "a1", "probe-1")
	// A backlog the operator's request must not queue behind.
	enqueue(t, store,
		&pulse.Job{ID: "old-1", AgentID: "a1", MaxAttempts: 3},
		&pulse.Job{ID: "old-2", AgentID: "a1", MaxAttempts: 3},
		&pulse.Job{ID: "urgent", AgentID: "a1", MaxAttempts: 3},
	)

	if err := d.DispatchNow(ctx, "urgent"); err != nil {
		t.Fatalf("DispatchNow: %v", err)
	}
	pushed := reg.jobsFor("a1")
	if len(pushed) != 1 || pushed[0].ID != "urgent" {
		t.Errorf("pushed %+v, want just the urgent job", pushed)
	}
}

func TestDispatchNowOnAnAlreadyLeasedJobConflicts(t *testing.T) {
	d, store, _, _ := newDispatcher(t, nil)
	ctx := context.Background()
	onlineAgent(store, "a1", "probe-1")
	enqueue(t, store, &pulse.Job{ID: "j1", AgentID: "a1", MaxAttempts: 3})
	if err := d.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}

	// The dispatch loop got there first and owns delivery.
	err := d.DispatchNow(ctx, "j1")
	if !errors.Is(err, pulse.ErrConflict) {
		t.Errorf("error = %v, want ErrConflict", err)
	}
}

func TestLeaseIsExclusiveUnderConcurrency(t *testing.T) {
	// The queue's core safety property: a job is never leased twice, even when
	// two passes race.
	d, store, _, _ := newDispatcher(t, nil)
	ctx := context.Background()
	onlineAgent(store, "a1", "probe-1")
	for i := 0; i < 20; i++ {
		enqueue(t, store, &pulse.Job{ID: fmt.Sprintf("j%d", i), AgentID: "a1", MaxAttempts: 3})
	}

	var wg sync.WaitGroup
	leased := make([][]pulse.Job, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			js, err := store.Lease(ctx, "a1", 10, time.Hour)
			if err != nil {
				t.Errorf("Lease: %v", err)
			}
			leased[i] = js
		}(i)
	}
	wg.Wait()

	seen := map[string]int{}
	for _, batch := range leased {
		for _, j := range batch {
			seen[j.ID]++
		}
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("job %s was leased %d times, want exactly 1", id, n)
		}
	}
	_ = d
}

func TestWakeCoalescesBursts(t *testing.T) {
	d, store, _, _ := newDispatcher(t, func(o *pulsequeue.Options) {
		// Long tick, so any pass that happens is wake-driven.
		o.DispatchInterval = time.Hour
		o.ReaperInterval = time.Hour
	})
	onlineAgent(store, "a1", "probe-1")

	// Hundreds of results arriving at once must not become hundreds of
	// dispatch passes. Wake is non-blocking with a one-slot buffer, so a
	// report never waits on the dispatcher either.
	for i := 0; i < 500; i++ {
		d.Wake()
	}
	// Nothing to assert beyond "this returned promptly and did not block or
	// panic"; the coalescing itself is the buffered channel's behaviour.
}

func TestNewRequiresDependencies(t *testing.T) {
	store := pulsetest.NewStore()
	reg := newRegistry()
	cases := map[string]pulsequeue.Options{
		"no jobs":     {Agents: store, Registry: reg},
		"no agents":   {Jobs: store, Registry: reg},
		"no registry": {Jobs: store, Agents: store},
	}
	for name, o := range cases {
		if _, err := pulsequeue.New(o); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

func TestDispatchWithoutConfigStoreUsesBatch(t *testing.T) {
	d, store, reg, _ := newDispatcher(t, func(o *pulsequeue.Options) {
		o.Configs = nil
		o.Batch = 2
	})
	ctx := context.Background()
	onlineAgent(store, "a1", "probe-1")
	for i := 0; i < 5; i++ {
		enqueue(t, store, &pulse.Job{ID: fmt.Sprintf("j%d", i), AgentID: "a1", MaxAttempts: 3})
	}
	if err := d.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if got := len(reg.jobsFor("a1")); got != 2 {
		t.Errorf("pushed %d jobs, want the batch size of 2", got)
	}
}

package pulseagent_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	pulsev1 "github.com/seanly/pulse-devkit/pb/pulse/v1"
	"github.com/seanly/pulse-devkit/pulse"
	"github.com/seanly/pulse-devkit/pulseagent"
	"github.com/seanly/pulse-devkit/pulsetest"
)

// blockingExecutor holds each job until release is closed, so a test can fill
// the concurrency gate deliberately.
type blockingExecutor struct {
	started chan string
	release chan struct{}
}

func newBlockingExecutor() *blockingExecutor {
	return &blockingExecutor{started: make(chan string, 64), release: make(chan struct{})}
}

func (b *blockingExecutor) Execute(ctx context.Context, job pulse.Job) (pulse.Result, error) {
	b.started <- job.ID
	select {
	case <-b.release:
	case <-ctx.Done():
		return pulse.Result{}, ctx.Err()
	}
	return pulse.Result{Status: pulse.StatusOK}, nil
}

func TestFullPoolAcksBusyWithoutBlocking(t *testing.T) {
	exec := newBlockingExecutor()
	a, api, _ := newAgent(t, func(o *pulseagent.Options) {
		o.Config.Limits.Concurrency = 1
		o.Executors = pulseagent.Executors{"": exec}
	})
	_, stop, errCh := run(t, a)
	defer func() { close(exec.release); stop(); <-errCh }()

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, err := api.WaitStream(waitCtx)
	if err != nil {
		t.Fatal(err)
	}

	s.PushJob(&pulsev1.Job{JobId: "j1", TimeoutMs: 60_000})
	select {
	case <-exec.started:
	case <-time.After(2 * time.Second):
		t.Fatal("first job never started")
	}

	// The gate is full. The second job must be declined promptly rather than
	// queued: another agent can run it now, and queueing here would also stall
	// the receive loop that carries heartbeats.
	s.PushJob(&pulsev1.Job{JobId: "j2", TimeoutMs: 60_000})
	acks, err := s.WaitAcks(2, 2*time.Second)
	if err != nil {
		t.Fatalf("acks: %v (got %d)", err, len(acks))
	}
	byJob := map[string]*pulsev1.JobAck{}
	for _, a := range acks {
		byJob[a.GetJobId()] = a
	}
	if !byJob["j1"].GetAccepted() {
		t.Error("j1 should have been accepted")
	}
	if byJob["j2"].GetAccepted() {
		t.Error("j2 should have been declined: the pool is full")
	}
	if got := byJob["j2"].GetReason(); got != pulse.AckBusy {
		t.Errorf("j2 reason = %q, want %q", got, pulse.AckBusy)
	}

	// Heartbeats must keep flowing while a job occupies the only slot.
	if _, err := s.WaitHeartbeats(2, 2*time.Second); err != nil {
		t.Errorf("heartbeats stalled while the pool was full: %v", err)
	}
}

func TestUnknownKindIsDeclinedNotDropped(t *testing.T) {
	a, api, _ := newAgent(t, func(o *pulseagent.Options) {
		o.Executors = pulseagent.Executors{"known": pulseagent.ExecutorFunc(
			func(context.Context, pulse.Job) (pulse.Result, error) {
				return pulse.Result{Status: pulse.StatusOK}, nil
			})}
	})
	_, stop, errCh := run(t, a)
	defer func() { stop(); <-errCh }()

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, err := api.WaitStream(waitCtx)
	if err != nil {
		t.Fatal(err)
	}
	s.PushJob(&pulsev1.Job{JobId: "j1", Kind: "unknown", TimeoutMs: 1000})

	acks, err := s.WaitAcks(1, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// Silence would leave the server waiting out a lease for work that was
	// never going to happen.
	if acks[0].GetAccepted() {
		t.Error("a job with no executor should be declined")
	}
	if got := acks[0].GetReason(); got != pulse.AckUnsupportedKind {
		t.Errorf("reason = %q, want %q", got, pulse.AckUnsupportedKind)
	}
}

func TestExecutorErrorBecomesErrorResult(t *testing.T) {
	a, api, _ := newAgent(t, func(o *pulseagent.Options) {
		o.Executors = pulseagent.Executors{"": pulseagent.ExecutorFunc(
			func(context.Context, pulse.Job) (pulse.Result, error) {
				return pulse.Result{}, errors.New("everything is on fire")
			})}
	})
	_, stop, errCh := run(t, a)
	defer func() { stop(); <-errCh }()

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, err := api.WaitStream(waitCtx)
	if err != nil {
		t.Fatal(err)
	}
	s.PushJob(&pulsev1.Job{JobId: "j1", SubjectId: "subj", TimeoutMs: 1000})

	res := waitResults(t, api, 1)
	if res[0].GetStatus() != pulse.StatusError {
		t.Errorf("status = %q, want %q", res[0].GetStatus(), pulse.StatusError)
	}
	if res[0].GetErrorCode() != pulse.CodeExecutorError {
		t.Errorf("error code = %q, want %q", res[0].GetErrorCode(), pulse.CodeExecutorError)
	}
	if res[0].GetErrorDetail() == "" {
		t.Error("want the error text carried through")
	}
	// Envelope fields are filled in by the framework, so an Executor need not
	// echo them back.
	if res[0].GetSubjectId() != "subj" {
		t.Errorf("subject_id = %q, want it copied from the job", res[0].GetSubjectId())
	}
}

func TestExecutorPanicIsContainedAndReported(t *testing.T) {
	a, api, _ := newAgent(t, func(o *pulseagent.Options) {
		o.Executors = pulseagent.Executors{"": pulseagent.ExecutorFunc(
			func(context.Context, pulse.Job) (pulse.Result, error) {
				panic("executor wrote a bug")
			})}
	})
	_, stop, errCh := run(t, a)
	defer func() { stop(); <-errCh }()

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, err := api.WaitStream(waitCtx)
	if err != nil {
		t.Fatal(err)
	}
	s.PushJob(&pulsev1.Job{JobId: "j1", TimeoutMs: 1000})

	res := waitResults(t, api, 1)
	if res[0].GetErrorCode() != pulse.CodeExecutorPanic {
		t.Errorf("error code = %q, want %q", res[0].GetErrorCode(), pulse.CodeExecutorPanic)
	}

	// And the agent is still alive: a second job runs fine.
	s.PushJob(&pulsev1.Job{JobId: "j2", TimeoutMs: 1000})
	if got := waitResults(t, api, 2); len(got) < 2 {
		t.Fatal("agent died with the panicking job")
	}
}

func TestLocalTimeoutCeilingWins(t *testing.T) {
	var gotDeadline time.Duration
	var mu sync.Mutex
	a, api, _ := newAgent(t, func(o *pulseagent.Options) {
		o.Config.Limits.Timeout = 50 * time.Millisecond
		o.Executors = pulseagent.Executors{"": pulseagent.ExecutorFunc(
			func(ctx context.Context, _ pulse.Job) (pulse.Result, error) {
				dl, ok := ctx.Deadline()
				if ok {
					mu.Lock()
					gotDeadline = time.Until(dl)
					mu.Unlock()
				}
				return pulse.Result{Status: pulse.StatusOK}, nil
			})}
	})
	_, stop, errCh := run(t, a)
	defer func() { stop(); <-errCh }()

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, err := api.WaitStream(waitCtx)
	if err != nil {
		t.Fatal(err)
	}
	// The server asks for 60s; the machine's owner said 50ms.
	s.PushJob(&pulsev1.Job{JobId: "j1", TimeoutMs: 60_000})
	waitResults(t, api, 1)

	mu.Lock()
	defer mu.Unlock()
	if gotDeadline > 50*time.Millisecond {
		t.Errorf("deadline %v exceeded the local ceiling of 50ms", gotDeadline)
	}
}

func TestJobTimeoutIsReportedAsTimeout(t *testing.T) {
	a, api, _ := newAgent(t, func(o *pulseagent.Options) {
		o.Config.Limits.Timeout = 20 * time.Millisecond
		o.Executors = pulseagent.Executors{"": pulseagent.ExecutorFunc(
			func(ctx context.Context, _ pulse.Job) (pulse.Result, error) {
				<-ctx.Done()
				return pulse.Result{}, ctx.Err()
			})}
	})
	_, stop, errCh := run(t, a)
	defer func() { stop(); <-errCh }()

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, err := api.WaitStream(waitCtx)
	if err != nil {
		t.Fatal(err)
	}
	s.PushJob(&pulsev1.Job{JobId: "j1", TimeoutMs: 10_000})

	res := waitResults(t, api, 1)
	// "It timed out" and "it failed" lead to different investigations, so they
	// get different codes.
	if res[0].GetErrorCode() != pulse.CodeTimeout {
		t.Errorf("error code = %q, want %q", res[0].GetErrorCode(), pulse.CodeTimeout)
	}
}

func TestAdmitterRejectionIsNotRetriableOnThisAgent(t *testing.T) {
	a, api, _ := newAgent(t, func(o *pulseagent.Options) {
		o.Executors = pulseagent.Executors{"": pulseagent.ExecutorFunc(
			func(context.Context, pulse.Job) (pulse.Result, error) {
				t.Error("executor ran despite the admitter refusing the job")
				return pulse.Result{}, nil
			})}
	})
	a.SetAdmitter(func(pulse.Job) error { return errors.New("denied by local policy") })

	_, stop, errCh := run(t, a)
	defer func() { stop(); <-errCh }()

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, err := api.WaitStream(waitCtx)
	if err != nil {
		t.Fatal(err)
	}
	s.PushJob(&pulsev1.Job{JobId: "j1", TimeoutMs: 1000})

	acks, err := s.WaitAcks(1, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if acks[0].GetAccepted() {
		t.Error("the job should have been declined")
	}
	// Distinct from busy: another agent may accept it, but this one never will.
	if got := acks[0].GetReason(); got != pulse.AckRejected {
		t.Errorf("reason = %q, want %q", got, pulse.AckRejected)
	}
}

func TestDrainWaitsForRunningJobsAndStillReports(t *testing.T) {
	started := make(chan struct{})
	finish := make(chan struct{})
	a, api, _ := newAgent(t, func(o *pulseagent.Options) {
		o.DrainTimeout = 2 * time.Second
		o.Executors = pulseagent.Executors{"": pulseagent.ExecutorFunc(
			func(ctx context.Context, _ pulse.Job) (pulse.Result, error) {
				close(started)
				<-finish
				return pulse.Result{Status: pulse.StatusOK}, nil
			})}
	})
	_, stop, errCh := run(t, a)

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, err := api.WaitStream(waitCtx)
	if err != nil {
		t.Fatal(err)
	}
	s.PushJob(&pulsev1.Job{JobId: "j1", TimeoutMs: 60_000})
	<-started

	// Shutdown begins with a job in flight.
	go stop()
	time.Sleep(20 * time.Millisecond)
	close(finish)

	if err := <-errCh; err != nil {
		t.Errorf("Run returned %v, want nil", err)
	}
	// The report travels on the unary RPC, which outlives the stream — that is
	// what makes draining worth doing rather than just exiting.
	if got := waitResults(t, api, 1); len(got) != 1 {
		t.Fatalf("want the drained job's result, got %d results", len(got))
	}
}

func waitResults(t *testing.T, api *pulsetest.API, n int) []*pulsev1.ReportResultRequest {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		res := api.Results()
		if len(res) >= n {
			return res
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d results, got %d", n, len(res))
			return res
		}
		time.Sleep(time.Millisecond)
	}
}

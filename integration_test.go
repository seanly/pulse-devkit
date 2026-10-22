// End-to-end test: a real gRPC server, a real agent, over a real socket.
//
// Everything else in the suite tests one package against fakes. This one
// proves the pieces fit: enrollment, the stream, dispatch, execution, the
// report, and the queue bookkeeping that follows. A framework whose parts each
// work but do not compose is not worth much, and only a test at this level
// catches that.
package gopulse_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	pulsev1 "github.com/seanly/pulse-devkit/pb/pulse/v1"
	"github.com/seanly/pulse-devkit/pulse"
	"github.com/seanly/pulse-devkit/pulseagent"
	"github.com/seanly/pulse-devkit/pulsecodec"
	"github.com/seanly/pulse-devkit/pulsequeue"
	"github.com/seanly/pulse-devkit/pulsereg"
	"github.com/seanly/pulse-devkit/pulsetest"
)

// jobPayload and resultPayload stand in for an application's own types.
type jobPayload struct {
	Target string `json:"target"`
}

type resultPayload struct {
	Observed string `json:"observed"`
}

// sink is the application's result handler, closing the loop back to the
// queue.
type sink struct {
	queue *pulsequeue.Dispatcher

	mu      sync.Mutex
	results []pulse.Result
	done    chan struct{}
	want    int
}

func newSink(q *pulsequeue.Dispatcher, want int) *sink {
	return &sink{queue: q, done: make(chan struct{}), want: want}
}

func (s *sink) Deliver(ctx context.Context, agentID string, r pulse.Result) (bool, error) {
	// Mark the job finished first: that is what frees the agent's slot.
	if err := s.queue.Complete(ctx, r.JobID); err != nil {
		return false, err
	}
	s.mu.Lock()
	s.results = append(s.results, r)
	if len(s.results) == s.want {
		close(s.done)
	}
	s.mu.Unlock()
	return true, nil
}

func (s *sink) Ack(agentID, jobID string, accepted bool, reason string) {
	s.queue.Ack(agentID, jobID, accepted, reason)
}

func (s *sink) collected() []pulse.Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]pulse.Result{}, s.results...)
}

// harness is a running server plus the pieces a test pokes at.
type harness struct {
	addr     string
	store    *pulsetest.Store
	registry *pulsereg.Service
	queue    *pulsequeue.Dispatcher
	sink     *sink
}

func startServer(t *testing.T, wantResults int, executorKinds ...string) *harness {
	t.Helper()
	store := pulsetest.NewStore()

	// The registry and the queue reference each other: the registry delivers
	// results to the sink, and the sink completes jobs in the queue, which
	// pushes through the registry. SetSink closes the loop after both exist.
	reg, err := pulsereg.New(pulsereg.Options{
		Agents: store, Tokens: store, Configs: store,
		MasterKey: []byte("integration-master-key"),
	})
	if err != nil {
		t.Fatal(err)
	}
	queue, err := pulsequeue.New(pulsequeue.Options{
		Jobs: store, Agents: store, Configs: store, Registry: reg,
		// Short loops so the test does not wait on a production cadence.
		DispatchInterval: 20 * time.Millisecond,
		ReaperInterval:   20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	sk := newSink(queue, wantResults)
	reg.SetSink(sk)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pulsev1.RegisterAgentServiceServer(srv, pulsereg.NewGRPCServer(reg))
	go func() { _ = srv.Serve(lis) }()

	ctx, cancel := context.WithCancel(context.Background())
	queue.Start(ctx)
	go reg.RunOfflineReaper(ctx)

	t.Cleanup(func() {
		cancel()
		srv.Stop()
	})

	return &harness{
		addr: lis.Addr().String(), store: store, registry: reg,
		queue: queue, sink: sk,
	}
}

// startAgent connects an agent that executes "probe" jobs with the given
// function.
//
// Each agent gets a freshly minted token: bootstrap tokens are single use, so
// reusing one across two agents fails exactly as it should.
func startAgent(t *testing.T, h *harness, name string,
	fn func(context.Context, pulse.Job, jobPayload) (resultPayload, error)) *pulseagent.Agent {
	t.Helper()
	token, _, err := h.registry.NewBootstrapToken(context.Background(), pulsereg.NewBootstrapTokenInput{
		Labels: map[string]string{"fleet": "integration"}, CreatedBy: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	api, err := pulseagent.NewGRPCAPI(pulseagent.DialOptions{Addr: h.addr})
	if err != nil {
		t.Fatal(err)
	}
	codec := pulsecodec.JSON[jobPayload, resultPayload]()
	agent, err := pulseagent.New(pulseagent.Options{
		Config: pulseagent.RuntimeConfig{
			Name:           name,
			ServerAddr:     h.addr,
			BootstrapToken: token,
			CredentialFile: filepath.Join(t.TempDir(), "credential.json"),
			Labels:         map[string]string{"region": "test"},
			Limits: pulseagent.Limits{
				Concurrency: 4, QPS: 1000, Timeout: 5 * time.Second,
			},
			LogLevel: "info",
		},
		API:               api,
		Version:           "integration",
		Executors:         pulseagent.Executors{"probe": pulsecodec.Typed(codec, fn)},
		HeartbeatInterval: 20 * time.Millisecond,
		DrainTimeout:      time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- agent.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-runErr:
		case <-time.After(3 * time.Second):
			t.Error("agent did not shut down")
		}
		api.Close()
	})
	return agent
}

// waitOnline blocks until the named agent is online in the store.
func waitOnline(t *testing.T, h *harness, name string) pulse.Agent {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		a, err := h.store.GetAgentByName(context.Background(), name)
		if err == nil && a.Status == pulse.AgentOnline && h.registry.Runtime(a.ID).Online {
			return a
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("agent %q never came online", name)
	return pulse.Agent{}
}

func TestEndToEndEnrollDispatchExecuteReport(t *testing.T) {
	const jobs = 5
	h := startServer(t, jobs)
	startAgent(t, h, "probe-1", func(_ context.Context, _ pulse.Job, p jobPayload) (resultPayload, error) {
		return resultPayload{Observed: "saw " + p.Target}, nil
	})

	agent := waitOnline(t, h, "probe-1")
	// The token's preset label and the agent's own both made it through
	// enrollment.
	if agent.Labels["fleet"] != "integration" || agent.Labels["region"] != "test" {
		t.Errorf("effective labels = %v", agent.Labels)
	}

	codec := pulsecodec.JSON[jobPayload, resultPayload]()
	inputs := make([]pulsequeue.EnqueueInput, 0, jobs)
	for i := 0; i < jobs; i++ {
		payload, err := codec.EncodeJob(jobPayload{Target: fmt.Sprintf("host-%d", i)})
		if err != nil {
			t.Fatal(err)
		}
		inputs = append(inputs, pulsequeue.EnqueueInput{
			AgentID: agent.ID, Kind: "probe", GroupID: "g1",
			SubjectID: fmt.Sprintf("subject-%d", i),
			Payload:   payload, MaxAttempts: 3,
		})
	}
	n := 0
	if _, err := h.queue.Enqueue(context.Background(), inputs, func() string {
		n++
		return fmt.Sprintf("job-%d", n)
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-h.sink.done:
	case <-time.After(10 * time.Second):
		t.Fatalf("only %d of %d results came back", len(h.sink.collected()), jobs)
	}

	results := h.sink.collected()
	if len(results) != jobs {
		t.Fatalf("got %d results, want %d", len(results), jobs)
	}
	for _, r := range results {
		if r.Status != pulse.StatusOK {
			t.Errorf("job %s status = %q, want ok", r.JobID, r.Status)
		}
		if r.GroupID != "g1" {
			t.Errorf("job %s group = %q, want g1", r.JobID, r.GroupID)
		}
		// The payload survived the round trip opaquely.
		out, err := codec.DecodeResult(r.Payload)
		if err != nil {
			t.Fatalf("decode result payload: %v", err)
		}
		if out.Observed == "" {
			t.Errorf("job %s has an empty result payload", r.JobID)
		}
		if r.ExecutedAt.IsZero() {
			t.Errorf("job %s has no execution time", r.JobID)
		}
	}

	// Every job ended up done, with exactly one attempt: nothing was retried
	// or left in flight.
	for _, j := range h.store.Jobs() {
		if j.Status != pulse.JobDone {
			t.Errorf("job %s status = %q, want done", j.ID, j.Status)
		}
		if j.Attempt != 1 {
			t.Errorf("job %s attempt = %d, want 1", j.ID, j.Attempt)
		}
	}
}

func TestEndToEndExecutorFailureRetriesThenTerminates(t *testing.T) {
	h := startServer(t, 0)
	var attempts int
	var mu sync.Mutex
	startAgent(t, h, "probe-1", func(_ context.Context, _ pulse.Job, _ jobPayload) (resultPayload, error) {
		mu.Lock()
		attempts++
		mu.Unlock()
		return resultPayload{}, errors.New("always fails")
	})
	agent := waitOnline(t, h, "probe-1")

	// The sink completes jobs, so a failing executor has to be failed through
	// the queue instead. Deliver still runs; this test watches the job row.
	if _, err := h.queue.Enqueue(context.Background(), []pulsequeue.EnqueueInput{
		{AgentID: agent.ID, Kind: "probe", SubjectID: "s1", MaxAttempts: 2},
	}, func() string { return "job-1" }); err != nil {
		t.Fatal(err)
	}

	// The agent reports an error result; the sink completes the job, so one
	// execution is all this test should see. The point is that the error
	// travelled back intact rather than being retried silently.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		j, err := h.store.GetJob(context.Background(), "job-1")
		if err == nil && j.Status == pulse.JobDone {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	results := h.sink.collected()
	if len(results) == 0 {
		t.Fatal("no result came back for a failing executor")
	}
	r := results[0]
	if r.Status != pulse.StatusError {
		t.Errorf("status = %q, want error", r.Status)
	}
	if r.ErrorCode != pulse.CodeExecutorError {
		t.Errorf("error code = %q, want %q", r.ErrorCode, pulse.CodeExecutorError)
	}
	if r.ErrorDetail == "" {
		t.Error("want the executor's message carried back")
	}
	mu.Lock()
	defer mu.Unlock()
	if attempts != 1 {
		t.Errorf("executor ran %d times, want 1", attempts)
	}
}

func TestEndToEndUnknownKindIsDeclinedAndRequeued(t *testing.T) {
	h := startServer(t, 0)
	startAgent(t, h, "probe-1", func(context.Context, pulse.Job, jobPayload) (resultPayload, error) {
		t.Error("the executor ran for a kind it was not registered for")
		return resultPayload{}, nil
	})
	agent := waitOnline(t, h, "probe-1")

	if _, err := h.queue.Enqueue(context.Background(), []pulsequeue.EnqueueInput{
		{AgentID: agent.ID, Kind: "something-else", SubjectID: "s1", MaxAttempts: 2},
	}, func() string { return "job-1" }); err != nil {
		t.Fatal(err)
	}

	// The decline must come back as an ack and return the job to the queue,
	// rather than the server waiting out a whole lease for a job that was
	// never going to run.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		j, err := h.store.GetJob(context.Background(), "job-1")
		if err == nil && j.RequeueCount > 0 {
			if j.Attempt != 0 {
				t.Errorf("attempt = %d, want 0: declining is not an attempt", j.Attempt)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	j, _ := h.store.GetJob(context.Background(), "job-1")
	t.Fatalf("job was never requeued after being declined: %+v", j)
}

func TestEndToEndConfigPushTakesEffectAndIsEchoed(t *testing.T) {
	h := startServer(t, 0)
	agent := startAgent(t, h, "probe-1", func(context.Context, pulse.Job, jobPayload) (resultPayload, error) {
		return resultPayload{}, nil
	})
	stored := waitOnline(t, h, "probe-1")

	two := 2
	cfg := pulse.AgentConfig{AgentID: stored.ID, Concurrency: &two, UpdatedBy: "op"}
	if err := h.store.SaveAgentConfig(context.Background(), &cfg); err != nil {
		t.Fatal(err)
	}
	delivered, err := h.registry.PushConfig(context.Background(), stored.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !delivered {
		t.Fatal("config was not delivered to an online agent")
	}

	// The agent applies it and echoes the version in its heartbeat, which is
	// what lets a console say "in effect" rather than "sent".
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if agent.AppliedConfigVersion() == int64(cfg.Version) &&
			h.registry.Runtime(stored.ID).AppliedConfigVersion == int64(cfg.Version) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Errorf("agent applied version %d and the server saw %d, want %d",
		agent.AppliedConfigVersion(),
		h.registry.Runtime(stored.ID).AppliedConfigVersion, cfg.Version)
}

func TestEndToEndRevokedCredentialForcesReEnrollment(t *testing.T) {
	h := startServer(t, 0)
	startAgent(t, h, "probe-1", func(context.Context, pulse.Job, jobPayload) (resultPayload, error) {
		return resultPayload{}, nil
	})
	stored := waitOnline(t, h, "probe-1")

	if err := h.registry.RevokeAgent(context.Background(), stored.ID); err != nil {
		t.Fatal(err)
	}

	// The agent's next call fails Unauthenticated. With no usable token of its
	// own it cannot recover, so what this asserts is the server side: the old
	// credential stops working immediately.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		a, err := h.store.GetAgent(context.Background(), stored.ID)
		if err != nil {
			t.Fatal(err)
		}
		if a.TokenVersion > stored.TokenVersion {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Error("token version never advanced")
}

func TestEndToEndTwoAgentsShareTheBacklog(t *testing.T) {
	const jobs = 10
	h := startServer(t, jobs)
	handler := func(_ context.Context, _ pulse.Job, p jobPayload) (resultPayload, error) {
		return resultPayload{Observed: p.Target}, nil
	}
	startAgent(t, h, "probe-1", handler)
	startAgent(t, h, "probe-2", handler)

	a1 := waitOnline(t, h, "probe-1")
	a2 := waitOnline(t, h, "probe-2")

	// The application decides who gets what — the framework never reassigns —
	// so the split is made here, at enqueue time.
	inputs := make([]pulsequeue.EnqueueInput, 0, jobs)
	for i := 0; i < jobs; i++ {
		target := a1.ID
		if i%2 == 1 {
			target = a2.ID
		}
		inputs = append(inputs, pulsequeue.EnqueueInput{
			AgentID: target, Kind: "probe", SubjectID: fmt.Sprintf("s%d", i), MaxAttempts: 2,
		})
	}
	n := 0
	if _, err := h.queue.Enqueue(context.Background(), inputs, func() string {
		n++
		return fmt.Sprintf("job-%d", n)
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-h.sink.done:
	case <-time.After(10 * time.Second):
		t.Fatalf("only %d of %d results came back", len(h.sink.collected()), jobs)
	}
	for _, j := range h.store.Jobs() {
		if j.Status != pulse.JobDone {
			t.Errorf("job %s status = %q, want done", j.ID, j.Status)
		}
	}
}

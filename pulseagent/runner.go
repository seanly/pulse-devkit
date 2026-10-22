// The worker pool: jobs come off the stream, get checked, acked, executed and
// reported. The receive loop must never block on execution — a slow job that
// stalled message intake would also stall heartbeats, which the server reads
// as the agent going offline.
package pulseagent

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	pulsev1 "github.com/seanly/pulse-devkit/pb/pulse/v1"
	"github.com/seanly/pulse-devkit/pulse"
)

// runner owns everything between "a job arrived on the stream" and "its result
// was sent".
type runner struct {
	log       *slog.Logger
	metrics   pulse.Metrics
	clock     pulse.Clock
	executors atomic.Pointer[Executors]

	// admit, when non-nil, vets a job before it is accepted. pulseguard
	// installs one; a plain agent has none.
	admit func(pulse.Job) error

	// qps is adjusted in place; timeout is read per job so a new ceiling
	// applies to new jobs only and never shortens a job already running.
	qps     *rate.Limiter
	timeout atomic.Int64 // time.Duration

	// gate bounds in-flight jobs at the effective concurrency. There is
	// deliberately no queue beyond the slots themselves.
	gate     gate
	inflight sync.WaitGroup

	running atomic.Int64
	// drained flips once drain starts, so a job arriving during shutdown is
	// declined rather than accepted into a pool that is closing.
	drained atomic.Bool

	// leaseRenewEvery is how often a running job renews its lease.
	leaseRenewEvery time.Duration

	// cancels holds one cancel func per running job, so a KIND_CANCEL can
	// reach exactly the job it names and leave the others alone.
	cancels sync.Map // job ID -> context.CancelFunc
}

// cancel abandons one running job. A job that is not running is a no-op: a
// cancel can cross paths with the job's own completion, and that race has an
// obvious right answer.
func (r *runner) cancel(jobID string) {
	if jobID == "" {
		return
	}
	stop, ok := r.cancels.Load(jobID)
	if !ok {
		r.log.Debug("cancel for a job that is not running", "job_id", jobID)
		return
	}
	r.log.Info("cancelling job", "job_id", jobID)
	stop.(context.CancelFunc)()
}

func newRunner(o *Options, lim Limits, admit func(pulse.Job) error) *runner {
	r := &runner{
		log:       o.Logger,
		metrics:   o.Metrics,
		clock:     o.Clock,
		admit:     admit,
		// Burst equals the limit: a burst of 1 would serialise jobs that the
		// concurrency gate has already agreed to run in parallel.
		qps:             rate.NewLimiter(rate.Limit(lim.QPS), lim.QPS),
		leaseRenewEvery: o.HeartbeatInterval,
	}
	execs := o.Executors
	if execs == nil {
		execs = Executors{}
	}
	r.executors.Store(&execs)
	r.timeout.Store(int64(lim.Timeout))
	r.gate.setLimit(lim.Concurrency)
	return r
}

// sendFunc queues a message to the server. It never blocks; a full queue is an
// error, because blocking here would stall the receive loop.
type sendFunc func(*pulsev1.AgentMessage) error

// reportFunc delivers a finished result over the unary RPC, which survives the
// stream closing.
type reportFunc func(context.Context, *pulsev1.ReportResultRequest)

// dispatch takes one job off the stream. It MUST return promptly: everything
// slow happens on a new goroutine.
func (r *runner) dispatch(ctx context.Context, msg *pulsev1.Job, send sendFunc, report reportFunc) {
	r.metrics.Inc(pulse.MetricJobsReceived)
	job := jobFromProto(msg)

	ack := func(accepted bool, reason, detail string) {
		if !accepted {
			r.metrics.Inc(pulse.MetricJobsRejected)
		}
		if err := send(&pulsev1.AgentMessage{Payload: &pulsev1.AgentMessage_JobAck{
			JobAck: &pulsev1.JobAck{JobId: job.ID, Accepted: accepted, Reason: reason, Detail: detail},
		}}); err != nil {
			r.log.Warn("ack failed", "job_id", job.ID, "error", err)
		}
	}

	// Shutting down: decline so the server re-dispatches elsewhere instead of
	// waiting out a lease on an agent that is leaving.
	if r.drained.Load() {
		ack(false, pulse.AckBusy, "")
		return
	}

	exec, ok := r.lookup(job.Kind)
	if !ok {
		// Saying so beats silence: the skeleton runs end to end and reports
		// why nothing happened.
		r.log.Warn("no executor for kind; declining", "job_id", job.ID, "kind", job.Kind)
		ack(false, pulse.AckUnsupportedKind, job.Kind)
		return
	}

	if r.admit != nil {
		if err := r.admit(job); err != nil {
			r.log.Info("job rejected by local policy",
				"job_id", job.ID, "subject", job.SubjectID, "error", err)
			ack(false, pulse.AckRejected, err.Error())
			return
		}
	}

	// Try to take a slot without waiting. A full pool is not an error — it is
	// the signal that another agent should take this job.
	if !r.gate.tryAcquire() {
		ack(false, pulse.AckBusy, "")
		return
	}

	ack(true, "", "")
	r.inflight.Add(1)
	r.running.Add(1)
	r.metrics.Set(pulse.MetricRunningJobs, float64(r.running.Load()))

	// A per-job context so a cancel reaches exactly this job. Registered
	// before the goroutine starts, so a cancel arriving immediately after the
	// ack still finds it.
	jobCtx, cancelJob := context.WithCancel(ctx)
	r.cancels.Store(job.ID, cancelJob)

	go func() {
		defer func() {
			cancelJob()
			r.cancels.Delete(job.ID)
			r.gate.release()
			r.running.Add(-1)
			r.metrics.Set(pulse.MetricRunningJobs, float64(r.running.Load()))
			r.inflight.Done()
		}()

		// Renew the lease while the work runs. Without this a job that outlives
		// its lease is dispatched a second time — waste for a probe, corruption
		// for a write.
		stopRenew := r.renewLease(jobCtx, job.ID, send)
		stdout := newProgressWriter(job.ID, pulse.StreamStdout, progressBudget, send, r.log)
		stderr := newProgressWriter(job.ID, pulse.StreamStderr, progressBudget, send, r.log)
		jobCtx = pulse.ContextWithStreams(jobCtx, pulse.Streams{Stdout: stdout, Stderr: stderr})
		res := r.execute(jobCtx, job, exec)
		stopRenew()
		r.metrics.Inc(pulse.MetricJobsExecuted)
		// A detached context: the report must go out even when the session
		// context is already cancelled by shutdown. The unary RPC outlives the
		// stream, which is what makes drain worth doing at all.
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		report(rctx, resultToProto(res))
	}()
}

// execute runs one job under its effective timeout, converting errors and
// panics into results.
func (r *runner) execute(ctx context.Context, job pulse.Job, exec Executor) pulse.Result {
	// QPS gate. Waiting here is correct: the slot is already held, so this
	// paces starts without blocking the receive loop.
	if err := r.qps.Wait(ctx); err != nil {
		return pulse.Result{
			JobID: job.ID, GroupID: job.GroupID, SubjectID: job.SubjectID, Kind: job.Kind,
			Status: pulse.StatusError, ErrorCode: pulse.CodeExecutorError,
			ErrorDetail: "qps wait: " + err.Error(), ExecutedAt: r.clock.Now(),
		}
	}

	// The server's request, truncated to the local ceiling: the machine's
	// owner decides how long it may be occupied.
	timeout := time.Duration(r.timeout.Load())
	if job.Timeout > 0 && job.Timeout < timeout {
		timeout = job.Timeout
	}
	jctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	started := r.clock.Now()
	res, err := r.safeExecute(jctx, job, exec)
	res.JobID = job.ID
	res.GroupID = job.GroupID
	res.SubjectID = job.SubjectID
	res.Kind = job.Kind
	if res.ExecutedAt.IsZero() {
		res.ExecutedAt = started
	}
	if res.Duration == 0 {
		res.Duration = r.clock.Now().Sub(started)
	}
	if err != nil {
		res.Status = pulse.StatusError
		if res.ErrorCode == "" {
			res.ErrorCode = pulse.CodeExecutorError
			// A deadline that fired is worth distinguishing: "it timed out" and
			// "it failed" lead to different investigations.
			if jctx.Err() != nil && ctx.Err() == nil {
				res.ErrorCode = pulse.CodeTimeout
			}
		}
		if res.ErrorDetail == "" {
			res.ErrorDetail = err.Error()
		}
	}
	if res.Status == "" {
		res.Status = pulse.StatusOK
	}
	return res
}

// safeExecute turns a panicking Executor into an error. Library users will
// write executors that panic; one job must not take the agent down.
func (r *runner) safeExecute(ctx context.Context, job pulse.Job, exec Executor) (res pulse.Result, err error) {
	defer func() {
		if p := recover(); p != nil {
			r.metrics.Inc(pulse.MetricJobsPanicked)
			r.log.Error("executor panicked", "job_id", job.ID, "kind", job.Kind, "panic", p)
			res = pulse.Result{ErrorCode: pulse.CodeExecutorPanic}
			err = fmt.Errorf("executor panicked: %v", p)
		}
	}()
	return exec.Execute(ctx, job)
}

// drain stops accepting work and waits for running jobs, bounded by timeout.
// Past the bound it returns anyway: the process exits and the server's lease
// expiry takes over.
func (r *runner) drain(timeout time.Duration) {
	r.drained.Store(true)
	done := make(chan struct{})
	go func() {
		r.inflight.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		r.log.Warn("drain deadline exceeded; leaving jobs to the server's lease expiry",
			"running", r.running.Load())
	}
}

func (r *runner) setQPS(qps int) {
	r.qps.SetLimit(rate.Limit(qps))
	r.qps.SetBurst(qps)
}

func (r *runner) setTimeout(d time.Duration) { r.timeout.Store(int64(d)) }

func (r *runner) runningJobs() int32 { return int32(r.running.Load()) }

// gate is a resizable counting semaphore. Resizing matters because a config
// push may change concurrency on a live agent: lowering it must not kill
// running jobs, only stop handing out slots until the count falls back.
type gate struct {
	mu    sync.Mutex
	limit int
	held  int
}

func (g *gate) setLimit(n int) {
	if n < 1 {
		n = 1
	}
	g.mu.Lock()
	g.limit = n
	g.mu.Unlock()
}

func (g *gate) limitNow() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.limit
}

func (g *gate) tryAcquire() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.held >= g.limit {
		return false
	}
	g.held++
	return true
}

func (g *gate) release() {
	g.mu.Lock()
	if g.held > 0 {
		g.held--
	}
	g.mu.Unlock()
}

// renewLease sends JobLease for a running job until the returned func is
// called or ctx ends. The returned func is idempotent.
//
// The cadence is a fraction of the lease so that losing one message does not
// lose the lease: the server grants roughly twice the job timeout, and renewing
// every heartbeat interval means several renewals have to go missing in a row
// before anything expires.
func (r *runner) renewLease(ctx context.Context, jobID string, send sendFunc) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(r.leaseRenewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// A full send queue drops the renewal rather than blocking: this
				// runs alongside the receive loop, and a stalled send there is
				// what gets an agent declared offline. One missed renewal is
				// survivable; a stalled heartbeat is not.
				if err := send(&pulsev1.AgentMessage{Payload: &pulsev1.AgentMessage_Lease{
					Lease: &pulsev1.JobLease{JobId: jobID},
				}}); err != nil {
					r.log.Debug("lease renewal not sent", "job_id", jobID, "error", err)
				}
			}
		}
	}()
	return cancel
}

// progressBudget is how much of one stream is forwarded. Past it the writer
// drops bytes and remembers that it did.
const progressBudget = 256 << 10

func (r *runner) lookup(kind string) (Executor, bool) {
	m := r.executors.Load()
	if m == nil {
		return nil, false
	}
	e, ok := (*m)[kind]
	return e, ok
}

// setExecutors replaces the kind map. A tightening push calls this so the
// next job sees the narrower set; a job already running keeps its executor.
func (r *runner) setExecutors(e Executors) {
	r.executors.Store(&e)
}

// progressWriter turns an executor's output stream into JobProgress messages.
//
// Rate-limited on purpose: a runaway process can print tens of megabytes a
// second, and an agent that forwards all of it becomes a denial of service
// against its own server. Over the budget it stops forwarding and records that
// it did — a visibly truncated log is safe to read, a quietly truncated one is
// not.
type progressWriter struct {
	jobID  string
	stream string
	send   sendFunc
	log    *slog.Logger

	seq       atomic.Uint64
	budget    atomic.Int64 // bytes remaining
	truncated atomic.Bool
}

func newProgressWriter(jobID, stream string, budget int64, send sendFunc, log *slog.Logger) *progressWriter {
	w := &progressWriter{jobID: jobID, stream: stream, send: send, log: log}
	w.budget.Store(budget)
	return w
}

func (w *progressWriter) Write(p []byte) (int, error) {
	// Always report the full length written: the caller's output is not the
	// agent's to reject, and a short write would look like a broken pipe to a
	// process that is behaving normally.
	if w.budget.Load() <= 0 {
		w.truncated.Store(true)
		return len(p), nil
	}
	chunk := p
	if int64(len(chunk)) > w.budget.Load() {
		chunk = chunk[:w.budget.Load()]
		w.truncated.Store(true)
	}
	w.budget.Add(-int64(len(chunk)))

	// Copy: the caller owns p and may reuse it before the sender goroutine
	// gets to this message.
	buf := make([]byte, len(chunk))
	copy(buf, chunk)
	if err := w.send(&pulsev1.AgentMessage{Payload: &pulsev1.AgentMessage_Progress{
		Progress: &pulsev1.JobProgress{
			JobId: w.jobID, Seq: w.seq.Add(1), Stream: w.stream, Chunk: buf,
		},
	}}); err != nil {
		// Dropped rather than retried, and the gap is detectable server-side
		// because seq already advanced.
		w.log.Debug("progress chunk not sent", "job_id", w.jobID, "error", err)
	}
	return len(p), nil
}

// Truncated reports whether any output was dropped.
func (w *progressWriter) Truncated() bool { return w.truncated.Load() }

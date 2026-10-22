// Package pulsequeue turns queued jobs into dispatched work and expired
// leases back into queued jobs.
//
// The split against the application is deliberate and worth stating plainly:
// this package answers "how does a job safely reach an agent and come back",
// never "which jobs should exist" or "what does this result mean". Choosing
// the agent happens at enqueue time, in the application — a rule engine, a
// round robin, an operator's click — and arrives here as Job.AgentID.
//
// Two loops drive it. Dispatch scans the backlog for online agents; the reaper
// returns expired leases to the queue. The reaper runs more often than
// dispatch so an expired lease does not wait a whole dispatch cycle.
package pulsequeue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/seanly/pulse-devkit/pulse"
)

// Loop and sizing defaults.
const (
	// DefaultDispatchInterval is how often the backlog is scanned. Report-
	// driven wake-ups make this a ceiling rather than the usual latency.
	DefaultDispatchInterval = 5 * time.Second
	// DefaultReaperInterval is how often expired leases are reclaimed. Finer
	// than dispatch on purpose.
	DefaultReaperInterval = 2 * time.Second
	// DefaultBatch is how many jobs one pass leases per agent when no
	// server-side concurrency is configured for it.
	//
	// It is a guess, and worth knowing as one: without a configured
	// concurrency the server cannot know how much the agent will take, so it
	// offers a modest number and lets busy acks correct it.
	DefaultBatch = 8
	// DefaultTimeout is the job timeout when neither the job nor the caller
	// specifies one.
	DefaultTimeout = 30 * time.Second
	// DefaultMaxRequeues caps how often one job may be handed back without
	// running. Past it the job fails rather than spinning forever against a
	// permanently saturated fleet.
	DefaultMaxRequeues = 10
	// minDispatchGap throttles report-driven dispatch. A run reporting
	// hundreds of results a second must not turn the dispatch pass into a busy
	// loop.
	minDispatchGap = 200 * time.Millisecond
)

// Options configures a Dispatcher.
type Options struct {
	// Jobs is the work queue. Required.
	Jobs pulse.JobStore
	// Agents supplies the dispatchable set. Required.
	Agents pulse.AgentStore
	// Configs supplies per-agent concurrency. Optional: without it every agent
	// gets Batch.
	Configs pulse.ConfigStore
	// Registry delivers job messages. Required. An agent without a live
	// stream fails the push and the job is requeued — there is no dead letter
	// here to lose work in.
	Registry pulse.Registry
	// OnExpired is called for each job the reaper gives up on, so the
	// application can record that a subject stopped being worked on. A job
	// that vanishes silently is worse than one that reports a failure.
	OnExpired func(ctx context.Context, j pulse.Job)
	// ResolvePayload, when set, is consulted for every job just before it is
	// pushed, and decides what the agent receives — or that it should not be
	// pushed at all.
	//
	// It exists because a payload stored at enqueue time is a snapshot. An
	// application whose work is described by mutable state wants the state as
	// it is now: a certificate probe should use the target's current port, and
	// a deploy should resolve the current release. Both would otherwise have
	// to re-enqueue every job after any edit.
	//
	// It also handles the cases where dispatch should stop: the subject was
	// deleted, or disabled, or its lookup failed. See Decision.
	//
	// Nil means jobs are pushed with the payload they were enqueued with.
	ResolvePayload PayloadResolver
	// Logger receives framework events. Nil discards them.
	Logger *slog.Logger
	// Metrics receives counters. Nil discards them.
	Metrics pulse.Metrics
	// Clock overrides time. Nil means pulse.SystemClock.
	Clock pulse.Clock

	// DispatchInterval, ReaperInterval, Batch, Timeout and MaxRequeues
	// override the defaults; zero means the default.
	DispatchInterval time.Duration
	ReaperInterval   time.Duration
	Batch            int
	Timeout          time.Duration
	MaxRequeues      int
}

// Dispatcher owns the job pipeline: dispatch, lease reaping, acks.
type Dispatcher struct {
	jobs     pulse.JobStore
	agents   pulse.AgentStore
	configs  pulse.ConfigStore
	registry pulse.Registry

	onExpired      func(context.Context, pulse.Job)
	resolvePayload PayloadResolver
	log            *slog.Logger
	metrics        pulse.Metrics
	clock          pulse.Clock

	dispatchEvery time.Duration
	reapEvery     time.Duration
	batch         int
	timeout       time.Duration
	maxRequeues   int

	// wake carries at most one pending dispatch wake-up: a burst of 500
	// reports coalesces into one extra pass, not 500. Buffered with capacity
	// one and written non-blockingly, so a report never waits on the
	// dispatcher.
	wake chan struct{}

	mu       sync.Mutex
	lastPass time.Time
}

// New validates its dependencies and returns a Dispatcher. Start it with
// Start, or drive the passes directly.
func New(o Options) (*Dispatcher, error) {
	if o.Jobs == nil {
		return nil, errors.New("pulsequeue: Jobs store is required")
	}
	if o.Agents == nil {
		return nil, errors.New("pulsequeue: Agents store is required")
	}
	if o.Registry == nil {
		return nil, errors.New("pulsequeue: Registry is required")
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	if o.Metrics == nil {
		o.Metrics = pulse.NopMetrics{}
	}
	if o.Clock == nil {
		o.Clock = pulse.SystemClock
	}
	d := &Dispatcher{
		jobs:           o.Jobs,
		agents:         o.Agents,
		configs:        o.Configs,
		registry:       o.Registry,
		onExpired:      o.OnExpired,
		resolvePayload: o.ResolvePayload,
		log:            o.Logger,
		metrics:        o.Metrics,
		clock:          o.Clock,
		dispatchEvery:  orDefault(o.DispatchInterval, DefaultDispatchInterval),
		reapEvery:      orDefault(o.ReaperInterval, DefaultReaperInterval),
		batch:          orDefaultInt(o.Batch, DefaultBatch),
		timeout:        orDefault(o.Timeout, DefaultTimeout),
		maxRequeues:    orDefaultInt(o.MaxRequeues, DefaultMaxRequeues),
		wake:           make(chan struct{}, 1),
	}
	return d, nil
}

func orDefault(v, def time.Duration) time.Duration {
	if v <= 0 {
		return def
	}
	return v
}

func orDefaultInt(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}

func (d *Dispatcher) now() time.Time { return d.clock.Now().UTC() }

// leaseFor is how long a lease is granted for: twice the job timeout plus
// slack, so a job that runs right up to its deadline and reports late still
// finds its lease alive.
func (d *Dispatcher) leaseFor() time.Duration {
	return 2*d.timeout + 30*time.Second
}

// Start runs the dispatch and reaper loops until ctx is cancelled. Both run
// their pass immediately, so a restarted server drains the backlog at once
// rather than after one interval.
func (d *Dispatcher) Start(ctx context.Context) {
	go d.dispatchLoop(ctx)
	go d.loop(ctx, d.reapEvery, d.ReapOnce, "reaper")
}

func (d *Dispatcher) dispatchLoop(ctx context.Context) {
	ticker := time.NewTicker(d.dispatchEvery)
	defer ticker.Stop()
	if err := d.DispatchOnce(ctx); err != nil && ctx.Err() == nil {
		d.log.Error("dispatch pass", "error", err)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-d.wake:
			// Throttle: a flood of reports must not turn this into a busy
			// loop.
			d.mu.Lock()
			since := d.now().Sub(d.lastPass)
			d.mu.Unlock()
			if since < minDispatchGap {
				continue
			}
		}
		if err := d.DispatchOnce(ctx); err != nil && ctx.Err() == nil {
			d.log.Error("dispatch pass", "error", err)
		}
	}
}

func (d *Dispatcher) loop(ctx context.Context, every time.Duration, pass func(context.Context) error, name string) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	if err := pass(ctx); err != nil && ctx.Err() == nil {
		d.log.Error(name, "error", err)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := pass(ctx); err != nil && ctx.Err() == nil {
				d.log.Error(name, "error", err)
			}
		}
	}
}

// Wake asks for a dispatch pass soon.
//
// Call it after storing a result: the finished job just freed a slot on that
// agent, and filling it now beats waiting for the next tick. Wake-ups coalesce
// and are throttled, so calling it per result is fine.
func (d *Dispatcher) Wake() {
	select {
	case d.wake <- struct{}{}:
	default:
		// Already one pending; one extra pass covers both.
	}
}

// DispatchOnce scans for dispatchable agents and pushes each one its pending
// jobs.
//
// Exposed as a method rather than a private pass so tests and an operator's
// "drain now" path can drive it directly.
func (d *Dispatcher) DispatchOnce(ctx context.Context) error {
	d.mu.Lock()
	d.lastPass = d.now()
	d.mu.Unlock()

	agents, err := d.agents.ListDispatchableAgents(ctx)
	if err != nil {
		return fmt.Errorf("pulsequeue: list dispatchable agents: %w", err)
	}
	configs := map[string]pulse.AgentConfig{}
	if d.configs != nil {
		configs, err = d.configs.ListAgentConfigs(ctx)
		if err != nil {
			return fmt.Errorf("pulsequeue: list agent configs: %w", err)
		}
	}

	for _, agent := range agents {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		quota, err := d.quotaFor(ctx, agent, configs[agent.ID])
		if err != nil {
			return err
		}
		if quota <= 0 {
			// The agent is full. Leasing anyway would only produce jobs it
			// refuses as busy, and every refusal is a round trip plus a write.
			d.log.Debug("agent at capacity; skipping", "agent", agent.Name)
			continue
		}
		jobs, err := d.jobs.Lease(ctx, agent.ID, quota, d.leaseFor())
		if err != nil {
			return fmt.Errorf("pulsequeue: lease for %s: %w", agent.Name, err)
		}
		for _, j := range jobs {
			d.push(ctx, agent, j)
		}
	}
	return nil
}

// quotaFor is how many jobs this pass may lease to one agent: its effective
// concurrency minus what it already holds.
func (d *Dispatcher) quotaFor(ctx context.Context, agent pulse.Agent, cfg pulse.AgentConfig) (int, error) {
	limit := d.batch
	if cfg.Concurrency != nil && *cfg.Concurrency > 0 {
		limit = *cfg.Concurrency
	}
	leased, err := d.jobs.CountLeasedForAgent(ctx, agent.ID)
	if err != nil {
		return 0, fmt.Errorf("pulsequeue: count leased for %s: %w", agent.Name, err)
	}
	return limit - leased, nil
}

// push delivers one leased job.
//
// Every branch must land the job somewhere. A leased job that is neither
// pushed nor returned to the queue is invisible until the reaper notices,
// which for a fresh lease is a whole lease period of nothing happening.
func (d *Dispatcher) push(ctx context.Context, agent pulse.Agent, j pulse.Job) {
	// Resolution comes first: it may replace the payload with current state,
	// or decide this job must not be dispatched at all.
	if !d.resolve(ctx, &j) {
		return
	}
	if j.Timeout <= 0 {
		j.Timeout = d.timeout
	}
	if err := d.registry.SendTo(agent.ID, &pulse.ServerMessage{Job: &j}); err != nil {
		d.log.Warn("push failed; requeueing", "job_id", j.ID, "agent", agent.Name, "error", err)
		d.requeue(ctx, j, "push failed: "+err.Error())
		return
	}
	d.metrics.Inc(pulse.MetricJobsDispatched)
	d.log.Debug("job dispatched", "job_id", j.ID, "agent", agent.Name, "subject", j.SubjectID)
}

// requeue hands a job back without burning an attempt.
//
// A full agent must not consume a job's retry budget — no work was attempted —
// but an unbounded requeue would let a permanently saturated fleet spin a job
// forever. The ceiling is enforced by the store, in the same statement as the
// requeue, so it holds even against a stale RequeueCount read from the job we
// are holding here.
func (d *Dispatcher) requeue(ctx context.Context, j pulse.Job, reason string) {
	d.metrics.Inc(pulse.MetricJobsRequeued)
	if err := d.jobs.Requeue(ctx, j.ID, reason, d.maxRequeues); err != nil &&
		!errors.Is(err, pulse.ErrNotFound) {
		d.log.Error("requeue job", "job_id", j.ID, "error", err)
	}
}

// ReapOnce sweeps expired leases: returning the recoverable ones to the queue
// and handing the exhausted ones to OnExpired, so no job ever disappears
// without a trace.
func (d *Dispatcher) ReapOnce(ctx context.Context) error {
	stats, err := d.jobs.ReclaimExpiredLeases(ctx)
	if err != nil {
		return fmt.Errorf("pulsequeue: reclaim leases: %w", err)
	}
	if stats.Requeued > 0 {
		d.log.Info("expired leases returned to the queue", "jobs", stats.Requeued)
		d.metrics.Add(pulse.MetricLeasesExpired, float64(stats.Requeued))
	}
	for _, j := range stats.Expired {
		d.log.Warn("job expired with no attempts left",
			"job_id", j.ID, "agent_id", j.AgentID, "subject", j.SubjectID)
		d.metrics.Inc(pulse.MetricLeasesExpired)
		if d.onExpired != nil {
			d.onExpired(ctx, j)
		}
	}
	if stats.Requeued > 0 {
		d.Wake()
	}
	return nil
}

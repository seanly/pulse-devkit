package pulse

import (
	"context"
	"errors"
	"time"
)

// Sentinel errors every store implementation must use. They are part of the
// port contract, not an implementation detail: the control plane has to tell
// "no such agent" from "the database is down", and a lost race from a fault.
//
// Implementations wrap them (fmt.Errorf("...: %w", pulse.ErrNotFound)) and
// callers test with errors.Is.
var (
	// ErrNotFound is returned by lookups that address a single row by
	// identity.
	ErrNotFound = errors.New("pulse: not found")
	// ErrConflict is returned when a row exists but its state does not admit
	// the requested transition — leasing a job someone else just took,
	// completing a job whose lease was already reclaimed. These are normal
	// races, not faults, and callers are expected to handle them.
	ErrConflict = errors.New("pulse: state conflict")
)

// AgentStore persists agent identity and liveness.
//
// All methods are called concurrently from gRPC handlers and background loops.
type AgentStore interface {
	// GetAgent returns one agent by ID, ErrNotFound if there is none.
	GetAgent(ctx context.Context, id string) (Agent, error)
	// GetAgentByName returns one agent by name, ErrNotFound if there is none.
	// Registration uses this to reuse a row rather than duplicate an identity.
	GetAgentByName(ctx context.Context, name string) (Agent, error)
	// ListAgents returns every agent, for console listings.
	ListAgents(ctx context.Context) ([]Agent, error)
	// ListDispatchableAgents returns agents eligible for work: online and not
	// disabled. Dispatch calls this every pass, so it should be indexed.
	ListDispatchableAgents(ctx context.Context) ([]Agent, error)
	// UpdateAgentMeta writes the mutable identity fields of an existing agent.
	// Labels must be recomputed from ReportedLabels and LabelOverrides by the
	// implementation (see EffectiveLabels) so the derived column cannot drift.
	UpdateAgentMeta(ctx context.Context, a *Agent) error
	// SetLabelOverrides replaces the server-side label opinion for one agent
	// and recomputes the effective set.
	SetLabelOverrides(ctx context.Context, agentID string, overrides map[string]*string) error
	// BumpTokenVersion revokes every credential issued for this agent by
	// advancing its revocation counter, and returns the new value.
	BumpTokenVersion(ctx context.Context, agentID string) (int, error)
	// SetAgentStatus moves an agent between AgentOnline/Offline/Disabled.
	SetAgentStatus(ctx context.Context, agentID, status string) error
	// Heartbeat records liveness. The peer address comes from the connection,
	// never from the payload. It flips a stale agent back to online.
	Heartbeat(ctx context.Context, agentID, version, peerIP string) error
	// MarkStaleAgentsOffline flips agents whose last heartbeat is older than
	// staleFor and returns their IDs so the caller can log the transition.
	//
	// It must not touch those agents' jobs: a lease has its own expiry, and a
	// network blip should not requeue work that is still running fine.
	//
	// staleFor is used as given, including when it is zero or negative — which
	// puts the cutoff at or after the present and makes every agent stale.
	// Implementations must not clamp it. Production never passes a negative
	// window, but it is how a test reaches this behaviour without a
	// controllable clock, and clamping would silently turn that test green.
	MarkStaleAgentsOffline(ctx context.Context, staleFor time.Duration) ([]string, error)
	// DeleteAgent removes an agent. Its credentials must stop verifying
	// immediately, which falls out of GetAgent returning ErrNotFound.
	DeleteAgent(ctx context.Context, agentID string) error
}

// TokenStore persists bootstrap tokens and performs enrollment.
type TokenStore interface {
	// CreateBootstrapToken stores a token by hash. The plaintext is shown to
	// the operator once and must never be persisted.
	CreateBootstrapToken(ctx context.Context, t *BootstrapToken) error
	// ListBootstrapTokens returns tokens for a console listing. TokenHash may
	// be elided; the plaintext is gone either way.
	ListBootstrapTokens(ctx context.Context) ([]BootstrapToken, error)
	// DeleteBootstrapToken removes an unused token.
	DeleteBootstrapToken(ctx context.Context, id string) error

	// Enroll atomically writes the agent row and claims the token.
	//
	// Atomic because the claim records which agent used it, so the agent must
	// exist first, and because an invalid token must leave nothing behind —
	// not half an agent row. Implementations do this in a transaction (SQL) or
	// under one lock (in-memory); the port deliberately does not expose a
	// transaction handle, which would leak database/sql's shape into it.
	//
	// The claimed token's preset labels are merged INTO a.ReportedLabels,
	// winning over what the agent reported, and the effective set is
	// recomputed. An operator minting a token for a specific purpose outranks
	// an agent's self-description. The merge happens here rather than in the
	// caller because the token's labels are not known until it is claimed, and
	// splitting the write would leave a window where the agent row exists with
	// the wrong labels.
	//
	// a is updated in place to the stored state, so the caller can report the
	// effective labels without reading the row back.
	//
	// Returns ErrInvalidBootstrap for a token that is unknown, already used or
	// expired. The three are one error on purpose: an attacker probing tokens
	// learns nothing about which case they hit.
	Enroll(ctx context.Context, a *Agent, tokenHash string) (BootstrapToken, error)
}

// ErrInvalidBootstrap covers an unknown, already-used or expired bootstrap
// token. Deliberately a single error; see TokenStore.Enroll.
var ErrInvalidBootstrap = errors.New("pulse: invalid bootstrap token")

// ConfigStore persists pushed agent configuration.
type ConfigStore interface {
	// GetAgentConfig returns the override for one agent, ErrNotFound when the
	// agent has none. "No override" is a normal state, not an error condition:
	// the agent's local configuration governs.
	GetAgentConfig(ctx context.Context, agentID string) (AgentConfig, error)
	// ListAgentConfigs returns every override keyed by agent ID. Dispatch
	// reads this once per pass rather than per agent.
	ListAgentConfigs(ctx context.Context) (map[string]AgentConfig, error)
	// SaveAgentConfig upserts an override and advances its version.
	SaveAgentConfig(ctx context.Context, cfg *AgentConfig) error
	// DeleteAgentConfig removes an override entirely, so the agent falls back
	// to its local configuration on the next push.
	DeleteAgentConfig(ctx context.Context, agentID string) error
}

// JobStore persists the work queue.
//
// Lease is the method that matters: see its documentation for why it must be a
// single statement.
type JobStore interface {
	// Enqueue inserts jobs as pending.
	Enqueue(ctx context.Context, jobs []*Job) error
	// GetJob returns one job, ErrNotFound if there is none.
	GetJob(ctx context.Context, id string) (Job, error)

	// Lease claims up to limit pending jobs for one agent, marks them leased,
	// increments their attempt and sets a lease deadline.
	//
	// It MUST be a single statement. A SELECT followed by an UPDATE is a race
	// whenever the caller is not already inside a transaction, and the whole
	// point of the queue is that no job is ever leased twice.
	//
	// On PostgreSQL that means a CTE with FOR UPDATE SKIP LOCKED; on SQLite,
	// whose single-writer semantics make SKIP LOCKED meaningless and
	// unsupported, an UPDATE ... WHERE id IN (SELECT ...) RETURNING.
	Lease(ctx context.Context, agentID string, limit int, leaseFor time.Duration) ([]Job, error)
	// LeaseOne claims one specific pending job, for an operator-triggered
	// "run this now" path that should not queue behind a backlog. Returns
	// ErrConflict if the job is no longer pending — the dispatch loop got it
	// first and owns delivery.
	//
	// leaseFor is used as given, including when it is negative — which dates
	// the lease deadline into the past, making the job immediately reclaimable.
	// Implementations must not clamp it; it is how a test produces an expired
	// lease without a controllable clock.
	LeaseOne(ctx context.Context, id string, leaseFor time.Duration) (Job, error)
	// RenewLease pushes a leased job's deadline out, for work that takes
	// longer than one lease period.
	//
	// It must match on the job being leased AND on agentID owning it. The
	// agent ID check is not ceremony: without it a stale renewal from an agent
	// whose lease was already reclaimed would extend a lease now held by
	// somebody else, and the first agent's eventual report would land on work
	// the second one is doing.
	//
	// ErrNotFound when the job is not leased to that agent — the caller treats
	// it as "too late", because by then the reaper has already moved on.
	RenewLease(ctx context.Context, id, agentID string, leaseFor time.Duration) error

	// Complete marks a leased job done. It must match on status = leased, so a
	// report arriving after the lease was reclaimed does not resurrect a job
	// the reaper already returned to the queue; the caller reads ErrNotFound
	// as "too late, and that is fine".
	Complete(ctx context.Context, id string) error
	// Fail records failed work. The job returns to pending while attempts
	// remain, so a transient error retries rather than being reported as a
	// real result. Past MaxAttempts it becomes JobFailed.
	Fail(ctx context.Context, id, reason string) error
	// FailTerminal fails a job outright, ignoring its remaining attempts.
	//
	// For work that provably cannot succeed: the subject was deleted, the
	// payload cannot be built, the job asks for something impossible.
	// Retrying it would cost a dispatch pass and a write per attempt to reach
	// the same end state, and would leave a misleading trail of "failures" for
	// something that was never attempted.
	//
	// Unlike Cancel this lands on JobFailed, because "nobody wanted this" and
	// "this could never work" are different things to read in a job history.
	FailTerminal(ctx context.Context, id, reason string) error
	// Requeue hands a job back WITHOUT burning an attempt, for when no work
	// happened at all: the agent was full, or the push failed. Counting those
	// as attempts would let a misconfigured concurrency exhaust the retry
	// budget and fabricate failures for subjects nothing ever looked at.
	//
	// RequeueCount is the brake instead. Past maxRequeues the job is FAILED
	// rather than returned to the queue: a job handed back that many times is
	// not meeting a busy agent occasionally, it is stuck behind one that never
	// has room, and spinning on it forever hides the real problem.
	//
	// The ceiling is enforced here, in the same statement as the requeue,
	// rather than by the caller — so it holds even for a caller that reads a
	// stale RequeueCount, and cannot be bypassed by calling Requeue directly.
	Requeue(ctx context.Context, id, reason string, maxRequeues int) error
	// Cancel terminates a job that is no longer wanted.
	Cancel(ctx context.Context, id, reason string) error

	// ReclaimExpiredLeases returns timed-out jobs to the queue, and gives up
	// on those that have burned through MaxAttempts.
	//
	// The given-up jobs are returned so the caller can synthesize a result for
	// each: a subject whose jobs all died silently must show up somewhere as
	// "not being worked on", not merely stop having rows.
	ReclaimExpiredLeases(ctx context.Context) (ReclaimStats, error)

	// CountLeasedForAgent is how many jobs an agent currently holds, which is
	// what dispatch subtracts from its concurrency to get a quota.
	CountLeasedForAgent(ctx context.Context, agentID string) (int, error)
	// CountPendingForAgent is backlog depth, for display and for deciding
	// whether a dispatch pass has anything to do.
	CountPendingForAgent(ctx context.Context, agentID string) (int, error)
	// PurgeFinished deletes terminal jobs older than before, returning how
	// many went. Retention is the application's policy; this is the mechanism.
	PurgeFinished(ctx context.Context, before time.Time) (int64, error)
}

// ReclaimStats is what one lease-reaper pass did.
type ReclaimStats struct {
	// Requeued is how many jobs went back to pending with attempts left.
	Requeued int64
	// Expired are jobs that burned through their attempts. The caller
	// synthesizes a result for each.
	Expired []Job
}

// Registry sends messages down live agent streams. The queue depends on this
// rather than on the control plane, so dispatch is testable without gRPC.
type Registry interface {
	// SendTo pushes a message to one agent. An error means the stream is gone
	// and the job must be requeued rather than retried on the same handle.
	SendTo(agentID string, msg *ServerMessage) error
}

// ServerMessage is one message bound for an agent. Exactly one field is set.
//
// This mirrors the proto oneof without making the queue import the generated
// package: pulsequeue builds these, and the control plane translates them on
// the way out.
type ServerMessage struct {
	Job     *Job
	Command *Command
	Config  *AgentConfig
}

// Command is an instruction to an agent.
//
// A struct rather than a bare kind because cancel names a job. Reload and
// shutdown are about the agent; cancel is about one piece of its work.
type Command struct {
	Kind CommandKind
	// JobID is meaningful only for CommandCancel.
	JobID string
}

// CommandKind enumerates the instructions.
type CommandKind int

const (
	// CommandReload asks the agent to re-read its local configuration.
	CommandReload CommandKind = 1
	// CommandShutdown asks the agent to finish running jobs and exit.
	CommandShutdown CommandKind = 2
	// CommandCancel abandons one running job.
	//
	// An agent too old to know it logs a line and carries on, so the job runs
	// to completion — the behaviour from before cancel existed, rather than a
	// failure.
	CommandCancel CommandKind = 3
)

// Reload returns a reload command.
func Reload() *Command { return &Command{Kind: CommandReload} }

// Shutdown returns a shutdown command.
func Shutdown() *Command { return &Command{Kind: CommandShutdown} }

// Cancel returns a cancel command for one job.
func Cancel(jobID string) *Command {
	return &Command{Kind: CommandCancel, JobID: jobID}
}

// Sink receives finished work. The application implements it: the framework
// knows a result arrived, the application knows what it means.
//
// Deliver is called from a stream's receive loop, which also carries that
// agent's heartbeats. An implementation must hand off quickly — anything slow
// belongs on its own goroutine, or heartbeats stall behind it and the server
// decides the agent went offline.
type Sink interface {
	// Deliver stores one result. accepted=false means the server already has
	// it (a replay, or an unknown job) and the agent should not retry.
	Deliver(ctx context.Context, agentID string, r Result) (accepted bool, err error)
	// Ack reacts to an agent's accept/reject of a dispatched job. Reasons are
	// the Ack* constants. detail is the operator-readable explanation; the
	// dispatcher still branches on reason alone.
	Ack(agentID string, jobID string, accepted bool, reason, detail string)
	// Progress receives incremental output from a running job.
	//
	// Called from the receive loop like Ack, and under far more pressure: a
	// chatty job produces these continuously for minutes. An implementation
	// that writes each chunk to a database synchronously will stall that
	// agent's heartbeats and get it declared offline — buffer, or hand off.
	//
	// Chunks may arrive out of order or with gaps after a reconnect. Seq is
	// what makes that detectable; an implementation that ignores it will
	// silently assemble a log with a hole in it.
	Progress(agentID string, p ProgressChunk)
}

// FactsStore persists what an agent reported about its own host.
//
// The bytes are opaque. The framework moves them; the application decides
// what they mean. That is the same rule as a job payload: two applications
// reading one agent's facts can care about different fields.
type FactsStore interface {
	SaveFacts(ctx context.Context, agentID string, raw []byte) error
	GetFacts(ctx context.Context, agentID string) ([]byte, error)
	// ListFacts is one read of the whole fleet, for a caller that is about to
	// choose targets from what the machines themselves reported.
	ListFacts(ctx context.Context) (map[string][]byte, error)
}

// ProgressChunk is incremental output from a job that is still running.
type ProgressChunk struct {
	JobID string
	// Seq starts at 1 per job and increases by one.
	Seq uint64
	// Stream is StreamStdout or StreamStderr, kept apart because merging them
	// cannot be undone.
	Stream string
	Chunk  []byte
}

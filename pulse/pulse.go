// Package pulse is pulse-devkit's vocabulary: the types, ports and constants that
// the agent runtime, the control plane and the queue all agree on.
//
// It depends on nothing else in this module. Everything else depends on it,
// which is what keeps the dependency graph a tree rather than a web — and what
// lets a project adopt only the half of the framework it needs.
package pulse

import "time"

// Protocol timing. Both sides import these from here, so there is exactly one
// place where the cadence is written down.
//
// Agents heartbeat every HeartbeatInterval; the server allows three to go
// missing before declaring an agent offline, which is long enough to ride out
// a TLS renegotiation and short enough that a crashed agent is visible within
// a minute.
const (
	// HeartbeatInterval is how often a well-behaved agent heartbeats.
	HeartbeatInterval = 15 * time.Second
	// StaleTimeout is how long without a heartbeat flips an agent offline.
	StaleTimeout = 45 * time.Second
	// ReaperInterval is how often the offline sweep runs.
	ReaperInterval = 15 * time.Second
	// DrainTimeout bounds graceful shutdown: running jobs get this long to
	// finish, after which the process exits and the server's lease expiry
	// takes over.
	DrainTimeout = 30 * time.Second
)

// Job statuses, as stored by a JobStore.
//
// JobExpired is deliberately distinct from JobFailed: a job that keeps timing
// out means the agent stopped answering, which is an agent problem, while a
// failed job means work ran and reported something. Collapsing the two would
// make "is this subject reachable at all" unanswerable from the job table.
const (
	JobPending   = "pending"
	JobLeased    = "leased"
	JobDone      = "done"
	JobFailed    = "failed"
	JobExpired   = "expired"
	JobCancelled = "cancelled"
)

// Result statuses. The framework uses these only to classify metrics; what
// each means is the application's business — "warning" is "expiring soon" for
// a certificate probe and "flaky tests" for a build, and both are right.
const (
	StatusOK      = "ok"
	StatusWarning = "warning"
	StatusError   = "error"
	StatusSkipped = "skipped"
)

// Ack reasons. An agent returns one of these when it declines a job; the
// dispatcher branches on them, so they are part of the protocol.
const (
	// AckBusy means the agent is at capacity. Another agent will do.
	AckBusy = "busy"
	// AckRejected means local policy refuses this job. Re-dispatching it to a
	// different agent is still worth trying — policy is per-agent — but the
	// same agent will refuse it again.
	AckRejected = "rejected"
	// AckUnsupportedKind means this agent has no executor for the job's kind.
	AckUnsupportedKind = "unsupported_kind"
)

// Framework-generated error codes. An application's own codes travel in the
// result payload or in ErrorCode; these are the ones pulse-devkit itself produces,
// prefixed so they cannot collide with an application's vocabulary.
const (
	// CodeExecutorError is an Executor that returned an error.
	CodeExecutorError = "pulse_executor_error"
	// CodeExecutorPanic is an Executor that panicked. The agent survives.
	CodeExecutorPanic = "pulse_executor_panic"
	// CodePayloadDecode is a payload the codec could not decode. The executor
	// was never called — it could not have done anything about it.
	CodePayloadDecode = "pulse_payload_decode"
	// CodeNoExecutor is a job whose kind has no registered executor.
	CodeNoExecutor = "pulse_no_executor"
	// CodeJobExpired is a job that burned through its attempts without ever
	// reporting. Synthesized server-side by the lease reaper.
	CodeJobExpired = "pulse_job_expired"
	// CodeTimeout is a job whose execution exceeded its deadline.
	CodeTimeout = "pulse_timeout"
)

// Agent statuses.
const (
	// AgentOnline means a heartbeat arrived within StaleTimeout.
	AgentOnline = "online"
	// AgentOffline means it did not. Jobs are untouched: a lease has its own
	// expiry, and a five-second blip should not requeue work still running.
	AgentOffline = "offline"
	// AgentDisabled is an operator decision. A disabled agent cannot register,
	// cannot connect, and is never dispatched to.
	AgentDisabled = "disabled"
)

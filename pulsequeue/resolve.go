package pulsequeue

import (
	"context"
	"errors"
	"time"

	"github.com/seanly/pulse-devkit/pulse"
)

// PayloadResolver decides what a job carries at the moment it is dispatched,
// or that it should not be dispatched.
//
// Called once per job per dispatch attempt, on the dispatch goroutine, so it
// should be a lookup rather than real work.
type PayloadResolver func(ctx context.Context, j pulse.Job) Decision

// Decision is a resolver's answer.
//
// The zero value pushes the job with the payload it already has, so a resolver
// that only wants to intervene in some cases can return Decision{} for the
// rest.
type Decision struct {
	// Action says what to do. The zero value is ActionPush.
	Action Action
	// Payload replaces the job's payload when Action is ActionPush and this is
	// non-nil. A nil payload keeps whatever the job was enqueued with — which
	// is not the same as replacing it with an empty one.
	Payload []byte
	// Timeout overrides the job's timeout when non-zero.
	Timeout time.Duration
	// Reason explains a Fail, Requeue or Drop, and is recorded on the job for
	// Fail and Requeue.
	Reason string
}

// Action is what the dispatcher does with a job after resolution.
type Action int

const (
	// ActionPush dispatches the job. The default.
	ActionPush Action = iota
	// ActionFail gives up on the job permanently, ignoring its remaining
	// attempts (JobStore.FailTerminal).
	//
	// For a subject that no longer exists, or a payload that cannot be built:
	// requeueing would loop forever, and retrying would burn attempts to reach
	// the same end state.
	ActionFail
	// ActionRequeue returns the job to the queue without consuming an attempt,
	// for a lookup that failed transiently. The requeue ceiling still applies,
	// so a lookup that never recovers does not spin forever.
	ActionRequeue
	// ActionDrop means the application has already recorded an outcome for
	// this job and the dispatcher should stop touching it.
	//
	// For a subject that exists but must not be worked on right now — disabled,
	// out of scope, excluded by a rule. The application typically writes a
	// "skipped" result of its own, which is why the framework must neither
	// requeue the job (it would come straight back) nor fail it (a rule
	// exclusion is not a failure). The application owns the job's final state
	// in this case.
	ActionDrop
)

// resolve applies the resolver to a job about to be pushed. It reports whether
// the dispatcher should carry on with the push.
func (d *Dispatcher) resolve(ctx context.Context, j *pulse.Job) bool {
	if d.resolvePayload == nil {
		return true
	}
	decision := d.resolvePayload(ctx, *j)
	switch decision.Action {
	case ActionPush:
		if decision.Payload != nil {
			j.Payload = decision.Payload
		}
		if decision.Timeout > 0 {
			j.Timeout = decision.Timeout
		}
		return true
	case ActionFail:
		d.log.Info("job will not be dispatched; failing it",
			"job_id", j.ID, "subject", j.SubjectID, "reason", decision.Reason)
		// Terminal, not Fail: this work provably cannot succeed, so spending
		// its remaining attempts to reach the same end state would only leave
		// a misleading trail of failures behind.
		if err := d.jobs.FailTerminal(ctx, j.ID, decision.Reason); err != nil &&
			!errors.Is(err, pulse.ErrNotFound) && !errors.Is(err, pulse.ErrConflict) {
			d.log.Error("fail undispatchable job", "job_id", j.ID, "error", err)
		}
		return false
	case ActionRequeue:
		d.log.Warn("job cannot be dispatched yet; requeueing",
			"job_id", j.ID, "subject", j.SubjectID, "reason", decision.Reason)
		d.requeue(ctx, *j, decision.Reason)
		return false
	case ActionDrop:
		// The application has recorded its own outcome and owns the job's
		// state from here. Touching it would overwrite that.
		d.log.Debug("job dropped by the application",
			"job_id", j.ID, "subject", j.SubjectID, "reason", decision.Reason)
		return false
	default:
		// An unknown action is a programming error in the resolver. Requeueing
		// is the safe reading: the job stays in the queue rather than being
		// lost or dispatched with an unresolved payload.
		d.log.Error("resolver returned an unknown action; requeueing",
			"job_id", j.ID, "action", decision.Action)
		d.requeue(ctx, *j, "resolver returned an unknown action")
		return false
	}
}

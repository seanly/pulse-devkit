package pulsequeue_test

import (
	"context"
	"testing"
	"time"

	"github.com/seanly/pulse-devkit/pulse"
	"github.com/seanly/pulse-devkit/pulsequeue"
)

func TestResolverReplacesPayloadAtDispatchTime(t *testing.T) {
	// The reason this hook exists: an application whose work is described by
	// mutable state wants that state as of dispatch, not as of enqueue.
	var seen pulse.Job
	d, store, reg, _ := newDispatcher(t, func(o *pulsequeue.Options) {
		o.ResolvePayload = func(_ context.Context, j pulse.Job) pulsequeue.Decision {
			seen = j
			return pulsequeue.Decision{Payload: []byte(`{"port":8443}`)}
		}
	})
	ctx := context.Background()
	onlineAgent(store, "a1", "probe-1")
	enqueue(t, store, &pulse.Job{
		ID: "j1", AgentID: "a1", SubjectID: "s1",
		Payload: []byte(`{"port":443}`), MaxAttempts: 3,
	})

	if err := d.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	pushed := reg.jobsFor("a1")
	if len(pushed) != 1 {
		t.Fatalf("pushed %d jobs, want 1", len(pushed))
	}
	if string(pushed[0].Payload) != `{"port":8443}` {
		t.Errorf("pushed payload = %q, want the resolved one", pushed[0].Payload)
	}
	// The resolver sees the stored job, so it can key off the subject.
	if seen.SubjectID != "s1" {
		t.Errorf("resolver saw subject %q", seen.SubjectID)
	}
}

func TestResolverZeroValueKeepsTheStoredPayload(t *testing.T) {
	// A resolver that only intervenes sometimes returns Decision{} for the
	// rest, and that must not blank the payload.
	d, store, reg, _ := newDispatcher(t, func(o *pulsequeue.Options) {
		o.ResolvePayload = func(context.Context, pulse.Job) pulsequeue.Decision {
			return pulsequeue.Decision{}
		}
	})
	ctx := context.Background()
	onlineAgent(store, "a1", "probe-1")
	enqueue(t, store, &pulse.Job{
		ID: "j1", AgentID: "a1", Payload: []byte(`{"port":443}`), MaxAttempts: 3,
	})
	if err := d.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	pushed := reg.jobsFor("a1")
	if len(pushed) != 1 || string(pushed[0].Payload) != `{"port":443}` {
		t.Errorf("pushed payload = %q, want the enqueued one preserved", pushed[0].Payload)
	}
}

func TestResolverFailStopsTheJobPermanently(t *testing.T) {
	d, store, reg, _ := newDispatcher(t, func(o *pulsequeue.Options) {
		o.ResolvePayload = func(context.Context, pulse.Job) pulsequeue.Decision {
			return pulsequeue.Decision{
				Action: pulsequeue.ActionFail, Reason: "subject deleted",
			}
		}
	})
	ctx := context.Background()
	onlineAgent(store, "a1", "probe-1")
	enqueue(t, store, &pulse.Job{ID: "j1", AgentID: "a1", MaxAttempts: 3})

	if err := d.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if n := reg.total(); n != 0 {
		t.Errorf("pushed %d jobs despite the resolver failing them", n)
	}
	j, err := store.GetJob(ctx, "j1")
	if err != nil {
		t.Fatal(err)
	}
	// A subject that no longer exists can never succeed, so requeueing would
	// loop forever. The job has to end.
	if j.Status != pulse.JobFailed {
		t.Errorf("status = %q, want failed", j.Status)
	}
	if j.LastError != "subject deleted" {
		t.Errorf("last error = %q, want the resolver's reason", j.LastError)
	}
}

func TestResolverRequeueDoesNotBurnAnAttempt(t *testing.T) {
	d, store, reg, _ := newDispatcher(t, func(o *pulsequeue.Options) {
		o.ResolvePayload = func(context.Context, pulse.Job) pulsequeue.Decision {
			return pulsequeue.Decision{
				Action: pulsequeue.ActionRequeue, Reason: "lookup failed",
			}
		}
	})
	ctx := context.Background()
	onlineAgent(store, "a1", "probe-1")
	enqueue(t, store, &pulse.Job{ID: "j1", AgentID: "a1", MaxAttempts: 3})

	if err := d.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if n := reg.total(); n != 0 {
		t.Errorf("pushed %d jobs despite the resolver requeueing them", n)
	}
	j, _ := store.GetJob(ctx, "j1")
	if j.Status != pulse.JobPending {
		t.Errorf("status = %q, want pending", j.Status)
	}
	// A transient lookup failure is not the job's fault.
	if j.Attempt != 0 {
		t.Errorf("attempt = %d, want 0", j.Attempt)
	}
	if j.RequeueCount != 1 {
		t.Errorf("requeue count = %d, want 1", j.RequeueCount)
	}
}

func TestResolverDropLeavesTheJobToTheApplication(t *testing.T) {
	// The resolver needs the store it is given, so the hook is installed
	// through a pointer the factory fills in.
	var jobs pulse.JobStore
	d, store, reg, _ := newDispatcher(t, func(o *pulsequeue.Options) {
		o.ResolvePayload = func(_ context.Context, j pulse.Job) pulsequeue.Decision {
			// Stand-in for an application recording its own "skipped" result.
			if err := jobs.Cancel(context.Background(), j.ID, "subject disabled"); err != nil {
				t.Errorf("application-side write: %v", err)
			}
			return pulsequeue.Decision{
				Action: pulsequeue.ActionDrop, Reason: "subject disabled",
			}
		}
	})
	jobs = store
	ctx := context.Background()
	onlineAgent(store, "a1", "probe-1")
	enqueue(t, store, &pulse.Job{ID: "j1", AgentID: "a1", MaxAttempts: 3})

	if err := d.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if n := reg.total(); n != 0 {
		t.Errorf("pushed %d jobs despite the resolver dropping them", n)
	}
	j, _ := store.GetJob(ctx, "j1")
	// The framework must not have touched it: requeueing would bring it
	// straight back, and failing it would call a deliberate exclusion a
	// failure.
	if j.Status != pulse.JobCancelled {
		t.Errorf("status = %q, want the application's own outcome preserved", j.Status)
	}
}

func TestResolverCanOverrideTheTimeout(t *testing.T) {
	d, store, reg, _ := newDispatcher(t, func(o *pulsequeue.Options) {
		o.ResolvePayload = func(context.Context, pulse.Job) pulsequeue.Decision {
			return pulsequeue.Decision{Timeout: 90 * time.Second}
		}
	})
	ctx := context.Background()
	onlineAgent(store, "a1", "probe-1")
	enqueue(t, store, &pulse.Job{
		ID: "j1", AgentID: "a1", Timeout: 10 * time.Second, MaxAttempts: 3,
	})
	if err := d.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	pushed := reg.jobsFor("a1")
	if len(pushed) != 1 || pushed[0].Timeout != 90*time.Second {
		t.Errorf("pushed timeout = %v, want 90s", pushed[0].Timeout)
	}
}

func TestResolverAppliesToDispatchNow(t *testing.T) {
	// The operator-triggered path must see the same resolution, or a manual
	// run would dispatch a stale payload that the scheduled path would not.
	d, store, reg, _ := newDispatcher(t, func(o *pulsequeue.Options) {
		o.ResolvePayload = func(context.Context, pulse.Job) pulsequeue.Decision {
			return pulsequeue.Decision{Payload: []byte(`{"resolved":true}`)}
		}
	})
	ctx := context.Background()
	onlineAgent(store, "a1", "probe-1")
	enqueue(t, store, &pulse.Job{ID: "j1", AgentID: "a1", MaxAttempts: 3})

	if err := d.DispatchNow(ctx, "j1"); err != nil {
		t.Fatal(err)
	}
	pushed := reg.jobsFor("a1")
	if len(pushed) != 1 || string(pushed[0].Payload) != `{"resolved":true}` {
		t.Errorf("pushed payload = %q, want the resolved one", pushed[0].Payload)
	}
}

func TestUnknownResolverActionRequeuesRatherThanLosingTheJob(t *testing.T) {
	d, store, reg, _ := newDispatcher(t, func(o *pulsequeue.Options) {
		o.ResolvePayload = func(context.Context, pulse.Job) pulsequeue.Decision {
			return pulsequeue.Decision{Action: pulsequeue.Action(99)}
		}
	})
	ctx := context.Background()
	onlineAgent(store, "a1", "probe-1")
	enqueue(t, store, &pulse.Job{ID: "j1", AgentID: "a1", MaxAttempts: 3})

	if err := d.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if n := reg.total(); n != 0 {
		t.Error("pushed a job with an unresolved payload")
	}
	// A buggy resolver should not lose work.
	j, _ := store.GetJob(ctx, "j1")
	if j.Status != pulse.JobPending {
		t.Errorf("status = %q, want pending", j.Status)
	}
}

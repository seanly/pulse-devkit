package pulsequeue_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seanly/pulse-devkit/pulse"
	"github.com/seanly/pulse-devkit/pulsequeue"
	"github.com/seanly/pulse-devkit/pulsetest"
)

func job(id, agent string, attempts int) *pulse.Job {
	return &pulse.Job{ID: id, AgentID: agent, Kind: "fs.put", SubjectID: agent, MaxAttempts: attempts}
}

func works(n int) []pulsequeue.Work {
	out := make([]pulsequeue.Work, n)
	for i := range out {
		id := string(rune('a' + i))
		out[i] = pulsequeue.Work{Key: id, Jobs: []*pulse.Job{job(id, id, 1)}}
	}
	return out
}

func finish(t *testing.T, store *pulsetest.Store, jobs []*pulse.Job, fail map[string]bool) {
	t.Helper()
	ctx := context.Background()
	for _, j := range jobs {
		for {
			if _, err := store.LeaseOne(ctx, j.ID, time.Minute); err != nil {
				t.Fatalf("lease %s: %v", j.ID, err)
			}
			var err error
			if fail[j.ID] {
				err = store.Fail(ctx, j.ID, "no")
			} else {
				err = store.Complete(ctx, j.ID)
			}
			if err != nil {
				t.Fatalf("finish %s: %v", j.ID, err)
			}
			got, err := store.GetJob(ctx, j.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.Status != pulse.JobPending {
				break
			}
		}
	}
}

func TestRolloutStopsOnGateHaltAndLeavesFinishedWork(t *testing.T) {
	store := pulsetest.NewStore()
	ctx := context.Background()
	var batches [][]string
	sum, err := (pulsequeue.Rollout{Jobs: store}).Run(ctx, works(4), pulsequeue.RolloutOptions{
		Batches: []int{1, 10, 100, 0},
		AfterEnqueue: func(ctx context.Context, jobs []*pulse.Job) error {
			var keys []string
			for _, j := range jobs {
				keys = append(keys, j.ID)
			}
			batches = append(batches, keys)
			finish(t, store, jobs, nil)
			return nil
		},
		Gate: func(context.Context, pulsequeue.BatchResult) pulsequeue.GateDecision {
			if len(batches) == 1 {
				return pulsequeue.GateHalt
			}
			return pulsequeue.GateContinue
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if sum.Reason != "gate_halt" {
		t.Fatalf("reason = %q", sum.Reason)
	}
	if len(sum.Ran) != 1 || sum.Ran[0].Key != "a" || sum.Ran[0].Failed {
		t.Fatalf("ran = %+v", sum.Ran)
	}
	if len(sum.NotRun) != 3 {
		t.Fatalf("not run = %v", sum.NotRun)
	}
	if _, err := store.GetJob(ctx, "a"); err != nil {
		t.Fatalf("finished job should still be there: %v", err)
	}
	if got, err := store.GetJob(ctx, "a"); err != nil || got.Status != pulse.JobDone {
		t.Fatalf("finished job status = %+v %v, want done and untouched", got, err)
	}
	for _, id := range []string{"b", "c", "d"} {
		_, err := store.GetJob(ctx, id)
		if !errors.Is(err, pulse.ErrNotFound) {
			t.Fatalf("job %s was inserted after the halt: %v", id, err)
		}
	}
	if len(batches) != 1 {
		t.Fatalf("batches dispatched = %v, want only the first", batches)
	}
}

func TestMaxFailureRateStopsWithoutAskingTheGate(t *testing.T) {
	store := pulsetest.NewStore()
	asked := false
	sum, err := (pulsequeue.Rollout{Jobs: store}).Run(context.Background(), works(3), pulsequeue.RolloutOptions{
		Batches:        []int{1, 0},
		MaxFailureRate: 0,
		AfterEnqueue: func(_ context.Context, jobs []*pulse.Job) error {
			finish(t, store, jobs, map[string]bool{"a": true})
			return nil
		},
		Gate: func(context.Context, pulsequeue.BatchResult) pulsequeue.GateDecision {
			asked = true
			return pulsequeue.GateContinue
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if asked {
		t.Fatal("gate was called after the failure rate was already exceeded")
	}
	if sum.Reason != "max_failure_rate" {
		t.Fatalf("reason = %q", sum.Reason)
	}
	if len(sum.Ran) != 1 || !sum.Ran[0].Failed {
		t.Fatalf("ran = %+v", sum.Ran)
	}
	if len(sum.NotRun) != 2 {
		t.Fatalf("not run = %v", sum.NotRun)
	}
	got, err := store.GetJob(context.Background(), "a")
	if err != nil || got.Status != pulse.JobFailed {
		t.Fatalf("failed job = %+v %v; a halt must not undo it", got, err)
	}
}

func TestContinueGateIsStillBoundedByFailureRate(t *testing.T) {
	store := pulsetest.NewStore()
	sum, err := (pulsequeue.Rollout{Jobs: store}).Run(context.Background(), works(2), pulsequeue.RolloutOptions{
		Batches:        []int{1, 0},
		MaxFailureRate: 0,
		AfterEnqueue: func(_ context.Context, jobs []*pulse.Job) error {
			finish(t, store, jobs, map[string]bool{jobs[0].ID: true})
			return nil
		},
		Gate: func(context.Context, pulsequeue.BatchResult) pulsequeue.GateDecision {
			return pulsequeue.GateContinue
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if sum.Reason != "max_failure_rate" || len(sum.NotRun) != 1 {
		t.Fatalf("summary = %+v", sum)
	}
}

func TestSettleForHappensBeforeTheGate(t *testing.T) {
	store := pulsetest.NewStore()
	var order []string
	_, err := (pulsequeue.Rollout{Jobs: store}).Run(context.Background(), works(1), pulsequeue.RolloutOptions{
		Batches:   []int{0},
		SettleFor: time.Hour,
		AfterEnqueue: func(_ context.Context, jobs []*pulse.Job) error {
			order = append(order, "enqueued")
			finish(t, store, jobs, nil)
			return nil
		},
		Sleep: func(_ context.Context, d time.Duration) error {
			if d != time.Hour {
				t.Errorf("slept %s, want the configured settle", d)
			}
			order = append(order, "settle")
			return nil
		},
		Gate: func(context.Context, pulsequeue.BatchResult) pulsequeue.GateDecision {
			order = append(order, "gate")
			return pulsequeue.GateContinue
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(order) != 3 || order[0] != "enqueued" || order[1] != "settle" || order[2] != "gate" {
		t.Fatalf("order = %v", order)
	}
}

func TestBatchWaitsUntilRetriesAreExhausted(t *testing.T) {
	store := pulsetest.NewStore()
	w := pulsequeue.Work{Key: "a", Jobs: []*pulse.Job{job("a", "a", 2)}}
	next := pulsequeue.Work{Key: "b", Jobs: []*pulse.Job{job("b", "b", 1)}}
	var gateStatus string
	_, err := (pulsequeue.Rollout{Jobs: store, Poll: time.Millisecond}).Run(context.Background(),
		[]pulsequeue.Work{w, next},
		pulsequeue.RolloutOptions{
			Batches:        []int{1, 0},
			MaxFailureRate: -1,
			AfterEnqueue: func(ctx context.Context, jobs []*pulse.Job) error {
				if jobs[0].ID != "a" {
					finish(t, store, jobs, nil)
					return nil
				}
				if _, err := store.LeaseOne(ctx, "a", time.Minute); err != nil {
					t.Fatal(err)
				}
				if err := store.Fail(ctx, "a", "once"); err != nil {
					t.Fatal(err)
				}
				got, err := store.GetJob(ctx, "a")
				if err != nil {
					t.Fatal(err)
				}
				if got.Status != pulse.JobPending {
					t.Fatalf("first failure status = %s, want pending while an attempt remains", got.Status)
				}
				go func() {
					time.Sleep(15 * time.Millisecond)
					if _, err := store.LeaseOne(context.Background(), "a", time.Minute); err != nil {
						return
					}
					_ = store.Fail(context.Background(), "a", "twice")
				}()
				return nil
			},
			Gate: func(_ context.Context, batch pulsequeue.BatchResult) pulsequeue.GateDecision {
				if batch.Index == 0 {
					gateStatus = batch.Targets[0].Jobs[0].Status
				}
				return pulsequeue.GateContinue
			},
		})
	if err != nil {
		t.Fatal(err)
	}
	if gateStatus != pulse.JobFailed {
		t.Fatalf("gate saw %q, want the job failed only after retries were spent", gateStatus)
	}
}

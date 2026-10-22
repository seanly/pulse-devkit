package pulsequeue

import (
	"context"
	"fmt"
	"time"

	"github.com/seanly/pulse-devkit/pulse"
)

// GateDecision is what an application says after looking at one finished batch.
type GateDecision int

const (
	// GateContinue releases the next batch.
	GateContinue GateDecision = iota
	// GateHalt stops later batches. Work already enqueued is left as it is.
	GateHalt
)

// RolloutOptions is the application's policy for one rollout. The queue
// owns the sequencing; the application owns the numbers and the meaning of
// healthy.
type RolloutOptions struct {
	// Batches is how many targets each wave takes. A 0 takes whatever is
	// left. An empty list is one wave of everything.
	Batches []int
	// Gate runs after a batch is terminal and SettleFor has elapsed. Nil
	// continues. It is not called when MaxFailureRate has already stopped
	// the rollout.
	Gate func(ctx context.Context, batch BatchResult) GateDecision
	// SettleFor is the pause between "every job in this batch is terminal"
	// and Gate. Failures that only show up after a reconnect need that gap.
	SettleFor time.Duration
	// MaxFailureRate stops the rollout when the fraction of failed targets
	// in the batch just finished is greater than this. Zero means the first
	// failure stops it. A negative value turns the check off.
	MaxFailureRate float64
	// Sleep waits out SettleFor. Nil sleeps on the real clock, honouring
	// ctx. Tests pass a function that records the wait and returns.
	Sleep func(ctx context.Context, d time.Duration) error
	// AfterEnqueue runs after a batch is inserted and before the wait. Nil
	// in production, where a dispatcher is already draining the queue. Tests
	// use it to drive jobs to a terminal status, including burning retries.
	AfterEnqueue func(ctx context.Context, jobs []*pulse.Job) error
}

// Work is one target and the jobs that change it. Jobs are inserted together
// when the target's batch starts, and not before.
type Work struct {
	// Key is how the summary names this target. The agent id is the usual value.
	Key  string
	Jobs []*pulse.Job
}

// TargetOutcome is one target after its jobs have all finished.
type TargetOutcome struct {
	Key    string
	Jobs   []pulse.Job
	Failed bool
}

// BatchResult is what Gate sees.
type BatchResult struct {
	Index    int
	Targets  []TargetOutcome
	Failures int
	Rate     float64
}

// Summary is the queryable record of a rollout, including one that stopped
// halfway. Nothing here is undone.
type Summary struct {
	// Ran is every target whose jobs were inserted, in batch order.
	Ran []TargetOutcome
	// NotRun is every target that never left the plan.
	NotRun []string
	// Reason is empty when every batch ran. Otherwise it is why later
	// batches were not started: "max_failure_rate", "gate_halt", or
	// "cancelled".
	Reason string
	// Rate is the failure rate of the batch that stopped the rollout, or of
	// the last batch when the rollout finished.
	Rate float64
}

// Rollout sequences work across a fleet. It does not know how to undo a
// change, and it does not try.
type Rollout struct {
	Jobs pulse.JobStore
	// Poll is how often a batch is re-read while jobs are still pending or
	// leased. Zero means 50ms.
	Poll time.Duration
}

// Run inserts each batch only after the previous one has finished, settled,
// and been allowed to continue.
func (r Rollout) Run(ctx context.Context, works []Work, opt RolloutOptions) (Summary, error) {
	if r.Jobs == nil {
		return Summary{}, fmt.Errorf("pulsequeue: rollout requires a job store")
	}
	waves := split(works, opt.Batches)
	var sum Summary
	for i, wave := range waves {
		if err := ctx.Err(); err != nil {
			sum.Reason = "cancelled"
			sum.NotRun = rest(works, waves, i)
			return sum, err
		}
		var jobs []*pulse.Job
		for _, w := range wave {
			jobs = append(jobs, w.Jobs...)
		}
		if err := r.Jobs.Enqueue(ctx, jobs); err != nil {
			sum.NotRun = rest(works, waves, i)
			return sum, err
		}
		if opt.AfterEnqueue != nil {
			if err := opt.AfterEnqueue(ctx, jobs); err != nil {
				sum.NotRun = rest(works, waves, i+1)
				return sum, err
			}
		}
		finished, err := r.wait(ctx, jobs)
		if err != nil {
			sum.Reason = "cancelled"
			sum.NotRun = rest(works, waves, i+1)
			return sum, err
		}
		batch := outcome(i, wave, finished)
		sum.Ran = append(sum.Ran, batch.Targets...)
		sum.Rate = batch.Rate
		if opt.MaxFailureRate >= 0 && batch.Rate > opt.MaxFailureRate {
			sum.Reason = "max_failure_rate"
			sum.NotRun = rest(works, waves, i+1)
			return sum, nil
		}
		if opt.SettleFor > 0 {
			sleep := opt.Sleep
			if sleep == nil {
				sleep = sleepCtx
			}
			if err := sleep(ctx, opt.SettleFor); err != nil {
				sum.Reason = "cancelled"
				sum.NotRun = rest(works, waves, i+1)
				return sum, err
			}
		}
		if opt.Gate != nil && opt.Gate(ctx, batch) == GateHalt {
			sum.Reason = "gate_halt"
			sum.NotRun = rest(works, waves, i+1)
			return sum, nil
		}
	}
	sum.NotRun = rest(works, waves, len(waves))
	return sum, nil
}

// rest names every target from wave index onward, plus any the plan never
// covered. Those jobs were not inserted.
func rest(works []Work, waves [][]Work, from int) []string {
	var out []string
	if from < len(waves) {
		out = append(out, keys(flatten(waves[from:]))...)
	}
	covered := 0
	for _, w := range waves {
		covered += len(w)
	}
	if covered < len(works) {
		out = append(out, keys(works[covered:])...)
	}
	return out
}

func (r Rollout) wait(ctx context.Context, jobs []*pulse.Job) (map[string]pulse.Job, error) {
	poll := r.Poll
	if poll <= 0 {
		poll = 50 * time.Millisecond
	}
	ids := make([]string, 0, len(jobs))
	for _, j := range jobs {
		if j != nil && j.ID != "" {
			ids = append(ids, j.ID)
		}
	}
	for {
		out := make(map[string]pulse.Job, len(ids))
		ready := true
		for _, id := range ids {
			j, err := r.Jobs.GetJob(ctx, id)
			if err != nil {
				return nil, err
			}
			if !terminal(j.Status) {
				ready = false
				break
			}
			out[id] = j
		}
		if ready {
			return out, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(poll):
		}
	}
}

func terminal(status string) bool {
	switch status {
	case pulse.JobDone, pulse.JobFailed, pulse.JobExpired, pulse.JobCancelled:
		return true
	default:
		return false
	}
}

func outcome(index int, wave []Work, finished map[string]pulse.Job) BatchResult {
	b := BatchResult{Index: index}
	for _, w := range wave {
		o := TargetOutcome{Key: w.Key}
		failed := false
		for _, j := range w.Jobs {
			if j == nil {
				continue
			}
			got := finished[j.ID]
			o.Jobs = append(o.Jobs, got)
			if got.Status != pulse.JobDone {
				failed = true
			}
		}
		o.Failed = failed
		if failed {
			b.Failures++
		}
		b.Targets = append(b.Targets, o)
	}
	if n := len(b.Targets); n > 0 {
		b.Rate = float64(b.Failures) / float64(n)
	}
	return b
}

func split(works []Work, batches []int) [][]Work {
	if len(works) == 0 {
		return nil
	}
	if len(batches) == 0 {
		return [][]Work{works}
	}
	var waves [][]Work
	rest := works
	for _, n := range batches {
		if len(rest) == 0 {
			break
		}
		if n <= 0 || n > len(rest) {
			n = len(rest)
		}
		waves = append(waves, rest[:n])
		rest = rest[n:]
	}
	return waves
}

func keys(works []Work) []string {
	out := make([]string, 0, len(works))
	for _, w := range works {
		out = append(out, w.Key)
	}
	return out
}

func flatten(waves [][]Work) []Work {
	var out []Work
	for _, w := range waves {
		out = append(out, w...)
	}
	return out
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

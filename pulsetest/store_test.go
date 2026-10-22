package pulsetest_test

import (
	"context"
	"testing"
	"time"

	"github.com/seanly/pulse-devkit/pulse"
	"github.com/seanly/pulse-devkit/pulsestore/porttest"
	"github.com/seanly/pulse-devkit/pulsetest"
)

// The in-memory store is held to the same contract as the SQL one. Without
// this, a test that passes against the fake would prove nothing about the
// behaviour an application actually deploys.
func TestInMemoryStoreSatisfiesThePortContract(t *testing.T) {
	porttest.Run(t, func(t *testing.T) porttest.Stores {
		clock := pulsetest.NewClock(time.Time{})
		s := pulsetest.NewStore(pulsetest.WithClock(clock))
		return porttest.Stores{
			Agents: s, Tokens: s, Configs: s, Jobs: s,
			Clock: clock,
			CreateAgent: func(_ context.Context, a *pulse.Agent) error {
				s.SeedAgent(*a)
				return nil
			},
		}
	})
}

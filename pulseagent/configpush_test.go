package pulseagent_test

import (
	"context"
	"errors"
	"testing"
	"time"

	pulsev1 "github.com/seanly/pulse-devkit/pb/pulse/v1"
	"github.com/seanly/pulse-devkit/pulse"
	"github.com/seanly/pulse-devkit/pulseagent"
	"github.com/seanly/pulse-devkit/pulsetest"
)

// waitConfigVersion polls until the agent reports the wanted applied version.
func waitConfigVersion(t *testing.T, a *pulseagent.Agent, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if a.AppliedConfigVersion() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("applied config version = %d, want %d", a.AppliedConfigVersion(), want)
}

func TestPushedConfigAppliesAndIsEchoedInHeartbeat(t *testing.T) {
	levels := pulsetest.NewLevelSetter()
	a, api, _ := newAgent(t, func(o *pulseagent.Options) {
		o.LevelSetter = levels
	})
	_, stop, errCh := run(t, a)
	defer func() { stop(); <-errCh }()

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, err := api.WaitStream(waitCtx)
	if err != nil {
		t.Fatal(err)
	}

	s.PushConfig(&pulsev1.AgentConfig{
		Version: 7, Concurrency: 4, Qps: 50, TimeoutMs: 5000, LogLevel: "debug",
	})
	waitConfigVersion(t, a, 7)

	if got := levels.Last(); got != "debug" {
		t.Errorf("log level = %q, want debug", got)
	}
	// The heartbeat is what lets a console say "in effect" rather than "sent".
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		hbs := s.Heartbeats()
		if len(hbs) > 0 && hbs[len(hbs)-1].GetConfigVersion() == 7 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Error("heartbeat never reported the applied config version")
}

func TestClearedOverrideFallsBackToLocalValue(t *testing.T) {
	levels := pulsetest.NewLevelSetter()
	a, api, _ := newAgent(t, func(o *pulseagent.Options) {
		o.Config.LogLevel = "warn" // the local value to fall back to
		o.LevelSetter = levels
	})
	_, stop, errCh := run(t, a)
	defer func() { stop(); <-errCh }()

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, err := api.WaitStream(waitCtx)
	if err != nil {
		t.Fatal(err)
	}

	s.PushConfig(&pulsev1.AgentConfig{Version: 1, LogLevel: "debug"})
	waitConfigVersion(t, a, 1)
	if got := levels.Last(); got != "debug" {
		t.Fatalf("log level = %q, want debug after the push", got)
	}

	// Clearing an override sends it as empty. "Unset" means "no opinion", so
	// the agent must return to its own value rather than keep the last pushed
	// one — this is what makes clearing work without a reconnect.
	s.PushConfig(&pulsev1.AgentConfig{Version: 2})
	waitConfigVersion(t, a, 2)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if levels.Last() == "warn" {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Errorf("log level = %q after the override was cleared, want the local warn", levels.Last())
}

func TestInvalidPushedConfigIsDiscardedWholesale(t *testing.T) {
	levels := pulsetest.NewLevelSetter()
	a, api, _ := newAgent(t, func(o *pulseagent.Options) {
		o.LevelSetter = levels
	})
	_, stop, errCh := run(t, a)
	defer func() { stop(); <-errCh }()

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, err := api.WaitStream(waitCtx)
	if err != nil {
		t.Fatal(err)
	}

	s.PushConfig(&pulsev1.AgentConfig{Version: 1, Concurrency: 4, LogLevel: "info"})
	waitConfigVersion(t, a, 1)

	// One bad field discards the whole message. Half a config applied is
	// harder to debug than the previous one still running, and the stalled
	// version number is how an operator sees that something went wrong.
	s.PushConfig(&pulsev1.AgentConfig{Version: 2, Concurrency: 4, LogLevel: "nonsense"})
	time.Sleep(50 * time.Millisecond)

	if got := a.AppliedConfigVersion(); got != 1 {
		t.Errorf("applied version = %d, want it to stay at 1 after an invalid push", got)
	}
	for _, l := range levels.Levels() {
		if l == "nonsense" {
			t.Error("an invalid level was applied")
		}
	}
}

func TestOutOfRangeFieldsAreRejected(t *testing.T) {
	cases := []struct {
		name string
		cfg  *pulsev1.AgentConfig
	}{
		{"concurrency too high", &pulsev1.AgentConfig{Version: 2, Concurrency: 99999}},
		{"negative concurrency", &pulsev1.AgentConfig{Version: 2, Concurrency: -1}},
		{"qps too high", &pulsev1.AgentConfig{Version: 2, Qps: 999999}},
		{"timeout too small", &pulsev1.AgentConfig{Version: 2, TimeoutMs: 10}},
		{"timeout too large", &pulsev1.AgentConfig{Version: 2, TimeoutMs: 7_200_000}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, api, _ := newAgent(t, nil)
			_, stop, errCh := run(t, a)
			defer func() { stop(); <-errCh }()

			waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			s, err := api.WaitStream(waitCtx)
			if err != nil {
				t.Fatal(err)
			}
			s.PushConfig(&pulsev1.AgentConfig{Version: 1})
			waitConfigVersion(t, a, 1)

			s.PushConfig(tc.cfg)
			time.Sleep(30 * time.Millisecond)
			if got := a.AppliedConfigVersion(); got != 1 {
				t.Errorf("applied version = %d, want 1: the push should have been rejected", got)
			}
		})
	}
}

func TestRejectedPayloadDiscardsTheWholeConfig(t *testing.T) {
	a, api, _ := newAgent(t, func(o *pulseagent.Options) {
		o.OnConfigPayload = func(p []byte) error {
			if string(p) == "bad" {
				return errors.New("cannot parse that")
			}
			return nil
		}
	})
	_, stop, errCh := run(t, a)
	defer func() { stop(); <-errCh }()

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, err := api.WaitStream(waitCtx)
	if err != nil {
		t.Fatal(err)
	}

	s.PushConfig(&pulsev1.AgentConfig{Version: 1, Payload: []byte("fine")})
	waitConfigVersion(t, a, 1)

	// A config is one unit: the application rejecting its own section must
	// discard the framework fields too, not leave them half applied.
	s.PushConfig(&pulsev1.AgentConfig{Version: 2, Concurrency: 9, Payload: []byte("bad")})
	time.Sleep(50 * time.Millisecond)
	if got := a.AppliedConfigVersion(); got != 1 {
		t.Errorf("applied version = %d, want 1 after the payload was rejected", got)
	}
}

func TestReconnectDoesNotLookLikeARollback(t *testing.T) {
	a, api, _ := newAgent(t, func(o *pulseagent.Options) {
		o.Backoff = pulseBackoffFast()
	})
	_, stop, errCh := run(t, a)
	defer func() { stop(); <-errCh }()

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	first, err := api.WaitStream(waitCtx)
	if err != nil {
		t.Fatal(err)
	}
	first.PushConfig(&pulsev1.AgentConfig{Version: 5, Concurrency: 3})
	waitConfigVersion(t, a, 5)

	first.Break(errors.New("connection reset"))
	second, err := api.WaitStream(waitCtx)
	if err != nil {
		t.Fatal(err)
	}

	// The applied version lives on the Agent, not the session: a reconnect
	// must not report version 0 and make the server think the config was lost.
	hbs, err := second.WaitHeartbeats(1, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got := hbs[0].GetConfigVersion(); got != 5 {
		t.Errorf("config version after reconnect = %d, want 5", got)
	}
}

func TestReloadKeepsPushedOverridesInEffect(t *testing.T) {
	levels := pulsetest.NewLevelSetter()
	a, api, _ := newAgent(t, func(o *pulseagent.Options) {
		o.Config.LogLevel = "warn"
		o.LevelSetter = levels
		o.ReloadConfig = func() (pulseagent.RuntimeConfig, error) {
			return pulseagent.RuntimeConfig{
				Limits:   pulseagent.Limits{Concurrency: 2, QPS: 10, Timeout: time.Second},
				LogLevel: "warn",
			}, nil
		}
	})
	_, stop, errCh := run(t, a)
	defer func() { stop(); <-errCh }()

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, err := api.WaitStream(waitCtx)
	if err != nil {
		t.Fatal(err)
	}

	s.PushConfig(&pulsev1.AgentConfig{Version: 3, LogLevel: "debug"})
	waitConfigVersion(t, a, 3)

	// RELOAD re-reads the local file. Without re-applying the pushed overrides
	// afterwards, the agent would quietly drop back to the local level while
	// the server still believed its own value was in effect and the version
	// number still agreed with it.
	s.PushCommand(pulsev1.Command_KIND_RELOAD)
	time.Sleep(50 * time.Millisecond)

	if got := levels.Last(); got != "debug" {
		t.Errorf("log level = %q after RELOAD, want the pushed debug to survive", got)
	}
	if got := a.AppliedConfigVersion(); got != 3 {
		t.Errorf("applied version = %d after RELOAD, want 3", got)
	}
}

func TestLevelPushWithoutSetterIsNotAnError(t *testing.T) {
	// A deployment that does not want its log level changed remotely is a
	// legitimate choice, not a misconfiguration: the rest of the config must
	// still apply.
	a, api, _ := newAgent(t, func(o *pulseagent.Options) {
		o.LevelSetter = nil
	})
	_, stop, errCh := run(t, a)
	defer func() { stop(); <-errCh }()

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, err := api.WaitStream(waitCtx)
	if err != nil {
		t.Fatal(err)
	}
	s.PushConfig(&pulsev1.AgentConfig{Version: 1, Concurrency: 4, LogLevel: "debug"})
	waitConfigVersion(t, a, 1)
}

// pulseBackoffFast is a no-wait backoff for tests that need a reconnect
// without waiting out a real schedule.
func pulseBackoffFast() pulse.Backoff {
	return pulse.Backoff{Base: time.Millisecond, Cap: 2 * time.Millisecond}
}

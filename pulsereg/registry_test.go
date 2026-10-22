package pulsereg_test

import (
	"context"
	"errors"
	"testing"
	"time"

	pulsev1 "github.com/seanly/pulse-devkit/pb/pulse/v1"
	"github.com/seanly/pulse-devkit/pulse"
	"github.com/seanly/pulse-devkit/pulsereg"
)

// seedAgent registers an agent and returns its ID.
func seedAgent(t *testing.T, svc *pulsereg.Service, name string) string {
	t.Helper()
	res, err := svc.Register(context.Background(), pulsereg.RegisterInput{
		BootstrapToken: mintToken(t, svc, nil), Name: name,
	})
	if err != nil {
		t.Fatal(err)
	}
	return res.AgentID
}

func TestReconnectReplacesStreamAndOldDetachIsHarmless(t *testing.T) {
	svc, _, _ := newService(t, nil)
	id := seedAgent(t, svc, "probe-1")

	var firstGot, secondGot []*pulsev1.ServerMessage
	detachFirst := svc.Attach(id, func(m *pulsev1.ServerMessage) error {
		firstGot = append(firstGot, m)
		return nil
	})
	// A reconnect attaches a newer stream before the old one's defer runs.
	svc.Attach(id, func(m *pulsev1.ServerMessage) error {
		secondGot = append(secondGot, m)
		return nil
	})

	// The old stream's detach must not remove the new stream. Without the
	// identity comparison in detach, one reconnect would silently make the
	// agent undispatchable.
	detachFirst()

	if err := svc.SendTo(id, &pulse.ServerMessage{Command: pulse.Reload()}); err != nil {
		t.Fatalf("SendTo after reconnect: %v", err)
	}
	if len(secondGot) != 1 {
		t.Errorf("new stream got %d messages, want 1", len(secondGot))
	}
	if len(firstGot) != 0 {
		t.Errorf("old stream got %d messages, want 0", len(firstGot))
	}
}

func TestSendToWithoutStreamFails(t *testing.T) {
	svc, _, _ := newService(t, nil)
	id := seedAgent(t, svc, "probe-1")

	// An offline agent must fail the push, so the dispatcher requeues the work
	// rather than believing it was delivered.
	if err := svc.SendTo(id, &pulse.ServerMessage{Command: pulse.Reload()}); err == nil {
		t.Error("want an error when the agent has no live stream")
	}
}

func TestRuntimeEmptiesWhenStreamDetaches(t *testing.T) {
	svc, _, _ := newService(t, nil)
	id := seedAgent(t, svc, "probe-1")

	detach := svc.Attach(id, func(*pulsev1.ServerMessage) error { return nil })
	if rt := svc.Runtime(id); !rt.Online {
		t.Error("want online while the stream is attached")
	}
	detach()
	// Runtime is derived from the stream, so it must empty immediately rather
	// than report stale numbers.
	rt := svc.Runtime(id)
	if rt.Online {
		t.Error("want offline after detach")
	}
	if rt.RunningJobs != 0 || rt.AppliedConfigVersion != 0 {
		t.Errorf("stale runtime after detach: %+v", rt)
	}
}

func TestHeartbeatRecordsLivenessAndRuntime(t *testing.T) {
	svc, store, _ := newService(t, nil)
	ctx := context.Background()
	id := seedAgent(t, svc, "probe-1")
	svc.Attach(id, func(*pulsev1.ServerMessage) error { return nil })

	agent, err := store.GetAgent(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	err = svc.Heartbeat(ctx, agent, &pulsev1.Heartbeat{
		Version: "2.0.0", Load: 0.5, RunningJobs: 3, ConfigVersion: 9,
	}, "198.51.100.9")
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	// Persisted: visible to every instance, and the basis for staleness.
	stored, err := store.GetAgent(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Version != "2.0.0" {
		t.Errorf("version = %q, want the heartbeat's", stored.Version)
	}
	if stored.ObservedIP != "198.51.100.9" {
		t.Errorf("observed IP = %q, want the connection's peer", stored.ObservedIP)
	}
	if stored.LastHeartbeatAt.IsZero() {
		t.Error("heartbeat timestamp not recorded")
	}

	// In memory: what only the live stream knows.
	rt := svc.Runtime(id)
	if rt.RunningJobs != 3 || rt.AppliedConfigVersion != 9 || rt.Load != 0.5 {
		t.Errorf("runtime = %+v, want the heartbeat's values", rt)
	}
}

func TestReaperFlipsStaleAgentsOffline(t *testing.T) {
	svc, store, clock := newService(t, nil)
	ctx := context.Background()
	id := seedAgent(t, svc, "probe-1")

	agent, err := store.GetAgent(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Heartbeat(ctx, agent, &pulsev1.Heartbeat{}, "198.51.100.1"); err != nil {
		t.Fatal(err)
	}
	if stored, _ := store.GetAgent(ctx, id); stored.Status != pulse.AgentOnline {
		t.Fatalf("status = %q, want online after a heartbeat", stored.Status)
	}

	// Still within the window: three heartbeats may go missing before an agent
	// is declared offline, which rides out a brief blip.
	clock.Advance(pulse.StaleTimeout - time.Second)
	svc.ReapOffline(ctx)
	if stored, _ := store.GetAgent(ctx, id); stored.Status != pulse.AgentOnline {
		t.Error("agent went offline too early")
	}

	clock.Advance(2 * time.Second)
	svc.ReapOffline(ctx)
	if stored, _ := store.GetAgent(ctx, id); stored.Status != pulse.AgentOffline {
		t.Error("agent should be offline past the staleness window")
	}
}

func TestReaperLeavesJobsAlone(t *testing.T) {
	svc, store, clock := newService(t, nil)
	ctx := context.Background()
	id := seedAgent(t, svc, "probe-1")
	agent, _ := store.GetAgent(ctx, id)
	if err := svc.Heartbeat(ctx, agent, &pulsev1.Heartbeat{}, ""); err != nil {
		t.Fatal(err)
	}

	if err := store.Enqueue(ctx, []*pulse.Job{
		{ID: "j1", AgentID: id, MaxAttempts: 3},
	}); err != nil {
		t.Fatal(err)
	}
	leased, err := store.Lease(ctx, id, 1, time.Hour)
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease: %v (%d jobs)", err, len(leased))
	}

	clock.Advance(pulse.StaleTimeout + time.Second)
	svc.ReapOffline(ctx)

	// Going offline must not requeue work: the lease has its own expiry, and a
	// network blip should not disturb a job that is still running fine.
	j, err := store.GetJob(ctx, "j1")
	if err != nil {
		t.Fatal(err)
	}
	if j.Status != pulse.JobLeased {
		t.Errorf("job status = %q, want it still leased after the agent went offline", j.Status)
	}
}

func TestPushConfigDeliversOnlyWhenOnlineAndConfigured(t *testing.T) {
	svc, store, _ := newService(t, nil)
	ctx := context.Background()
	id := seedAgent(t, svc, "probe-1")

	// No override stored: nothing to push, and that is not an error. Pushing
	// an empty config would bump the agent's applied version to one that does
	// not exist.
	delivered, err := svc.PushConfig(ctx, id)
	if err != nil {
		t.Fatalf("PushConfig with no override: %v", err)
	}
	if delivered {
		t.Error("delivered = true with no stored override")
	}

	four := 4
	cfg := pulse.AgentConfig{AgentID: id, Concurrency: &four}
	if err := store.SaveAgentConfig(ctx, &cfg); err != nil {
		t.Fatal(err)
	}

	// Offline: stored but not delivered, so a console can say "takes effect
	// when the agent reconnects" rather than guessing.
	delivered, err = svc.PushConfig(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if delivered {
		t.Error("delivered = true for an offline agent")
	}

	var got []*pulsev1.ServerMessage
	svc.Attach(id, func(m *pulsev1.ServerMessage) error {
		got = append(got, m)
		return nil
	})
	delivered, err = svc.PushConfig(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if !delivered {
		t.Error("delivered = false for an online agent with an override")
	}
	if len(got) != 1 || got[0].GetConfig() == nil {
		t.Fatalf("want one config message, got %+v", got)
	}
	if got[0].GetConfig().GetConcurrency() != 4 {
		t.Errorf("concurrency = %d, want 4", got[0].GetConfig().GetConcurrency())
	}
}

func TestPushConfigWithoutConfigStoreIsInert(t *testing.T) {
	// A deployment that does not want server-side configuration at all is a
	// legitimate setup: agents run purely on their local files.
	svc, _, _ := newService(t, func(o *pulsereg.Options) { o.Configs = nil })
	ctx := context.Background()
	id := seedAgent(t, svc, "probe-1")
	svc.Attach(id, func(*pulsev1.ServerMessage) error { return nil })

	delivered, err := svc.PushConfig(ctx, id)
	if err != nil {
		t.Errorf("PushConfig without a config store: %v", err)
	}
	if delivered {
		t.Error("delivered = true without a config store")
	}
}

func TestSendToTranslatesEachMessageKind(t *testing.T) {
	svc, _, _ := newService(t, nil)
	id := seedAgent(t, svc, "probe-1")
	var got []*pulsev1.ServerMessage
	svc.Attach(id, func(m *pulsev1.ServerMessage) error {
		got = append(got, m)
		return nil
	})

	job := pulse.Job{ID: "j1", SubjectID: "s1", Kind: "probe", Timeout: 5 * time.Second, Attempt: 2,
		Payload: []byte(`{"host":"example.com"}`)}
	if err := svc.SendTo(id, &pulse.ServerMessage{Job: &job}); err != nil {
		t.Fatal(err)
	}
	if err := svc.SendTo(id, &pulse.ServerMessage{Command: pulse.Shutdown()}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 messages, got %d", len(got))
	}

	wj := got[0].GetJob()
	if wj == nil || wj.GetJobId() != "j1" || wj.GetTimeoutMs() != 5000 || wj.GetAttempt() != 2 {
		t.Errorf("job not translated faithfully: %+v", wj)
	}
	if string(wj.GetPayload()) != `{"host":"example.com"}` {
		t.Error("payload was not carried through opaquely")
	}
	if got[1].GetCommand().GetKind() != pulsev1.Command_KIND_SHUTDOWN {
		t.Errorf("command = %v, want shutdown", got[1].GetCommand().GetKind())
	}
}

func TestEmptyServerMessageIsAnError(t *testing.T) {
	svc, _, _ := newService(t, nil)
	id := seedAgent(t, svc, "probe-1")
	svc.Attach(id, func(*pulsev1.ServerMessage) error { return nil })

	if err := svc.SendTo(id, &pulse.ServerMessage{}); err == nil {
		t.Error("want an error for a message with no payload set")
	}
	if err := svc.SendTo(id, nil); err == nil {
		t.Error("want an error for a nil message")
	}
}

func TestConnectedAgentsCountsLiveStreams(t *testing.T) {
	svc, _, _ := newService(t, nil)
	a := seedAgent(t, svc, "probe-1")
	b := seedAgent(t, svc, "probe-2")

	if n := svc.ConnectedAgents(); n != 0 {
		t.Errorf("connected = %d, want 0", n)
	}
	detachA := svc.Attach(a, func(*pulsev1.ServerMessage) error { return nil })
	svc.Attach(b, func(*pulsev1.ServerMessage) error { return nil })
	if n := svc.ConnectedAgents(); n != 2 {
		t.Errorf("connected = %d, want 2", n)
	}
	detachA()
	if n := svc.ConnectedAgents(); n != 1 {
		t.Errorf("connected = %d, want 1 after one detach", n)
	}
}

func TestSendErrorPropagates(t *testing.T) {
	svc, _, _ := newService(t, nil)
	id := seedAgent(t, svc, "probe-1")
	want := errors.New("stream is gone")
	svc.Attach(id, func(*pulsev1.ServerMessage) error { return want })

	err := svc.SendTo(id, &pulse.ServerMessage{Command: pulse.Reload()})
	// The dispatcher branches on this to requeue, so it must not be swallowed.
	if !errors.Is(err, want) {
		t.Errorf("error = %v, want the stream's error", err)
	}
}

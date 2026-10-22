package pulseagent_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	pulsev1 "github.com/seanly/pulse-devkit/pb/pulse/v1"
	"github.com/seanly/pulse-devkit/pulse"
	"github.com/seanly/pulse-devkit/pulseagent"
	"github.com/seanly/pulse-devkit/pulsetest"
)

// newAgent builds an agent wired to a fake API, with a credential file in a
// temp dir. Returns the agent, the API and the credential path.
func newAgent(t *testing.T, mutate func(*pulseagent.Options)) (*pulseagent.Agent, *pulsetest.API, string) {
	t.Helper()
	credFile := filepath.Join(t.TempDir(), "credential.json")
	api := pulsetest.NewAPI()
	o := pulseagent.Options{
		Config: pulseagent.RuntimeConfig{
			Name:           "agent-a",
			ServerAddr:     "localhost:0",
			BootstrapToken: "boot-1",
			CredentialFile: credFile,
			Limits:         pulseagent.Limits{Concurrency: 2, QPS: 1000, Timeout: time.Second},
			LogLevel:       "info",
		},
		API:     api,
		Version: "test",
		// A fast heartbeat keeps the timing tests quick; the server-side
		// staleness window is not involved here.
		HeartbeatInterval: 5 * time.Millisecond,
		DrainTimeout:      time.Second,
	}
	if mutate != nil {
		mutate(&o)
	}
	a, err := pulseagent.New(o)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return a, api, credFile
}

// run starts the agent and returns a stop func plus the error channel.
func run(t *testing.T, a *pulseagent.Agent) (context.Context, func(), <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- a.Run(ctx) }()
	return ctx, cancel, errCh
}

func TestRegistersOnFirstStartAndPersistsCredential(t *testing.T) {
	a, api, credFile := newAgent(t, nil)
	_, stop, errCh := run(t, a)
	defer func() { stop(); <-errCh }()

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := api.WaitStream(waitCtx); err != nil {
		t.Fatalf("no stream: %v", err)
	}

	regs := api.Registrations()
	if len(regs) != 1 {
		t.Fatalf("want 1 registration, got %d", len(regs))
	}
	if regs[0].GetBootstrapToken() != "boot-1" || regs[0].GetName() != "agent-a" {
		t.Errorf("unexpected registration: %+v", regs[0])
	}

	raw, err := os.ReadFile(credFile)
	if err != nil {
		t.Fatalf("credential not persisted: %v", err)
	}
	var cred pulseagent.Credential
	if err := json.Unmarshal(raw, &cred); err != nil {
		t.Fatalf("credential unreadable: %v", err)
	}
	if cred.AgentID != "agent-1" || cred.Credential != "credential-1" {
		t.Errorf("unexpected credential contents: %+v", cred)
	}

	// 0600: anything on the box that can read this file can impersonate the
	// agent, so the mode is part of the contract.
	info, err := os.Stat(credFile)
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode != 0o600 {
		t.Errorf("credential mode = %o, want 600", mode)
	}
}

func TestReusesStoredCredentialWithoutRegistering(t *testing.T) {
	credFile := filepath.Join(t.TempDir(), "credential.json")
	if err := pulseagent.SaveCredential(credFile, pulseagent.Credential{
		AgentID: "agent-existing", Credential: "credential-existing",
	}); err != nil {
		t.Fatal(err)
	}

	api := pulsetest.NewAPI()
	a, err := pulseagent.New(pulseagent.Options{
		Config: pulseagent.RuntimeConfig{
			Name: "agent-a", CredentialFile: credFile,
			// No bootstrap token on purpose: a stored credential must be
			// enough to start, or losing the token would strand the agent.
			Limits: pulseagent.Limits{Concurrency: 1, QPS: 100, Timeout: time.Second},
		},
		API: api, HeartbeatInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, stop, errCh := run(t, a)
	defer func() { stop(); <-errCh }()

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := api.WaitStream(waitCtx); err != nil {
		t.Fatalf("no stream: %v", err)
	}
	if n := len(api.Registrations()); n != 0 {
		t.Errorf("registered %d times despite a stored credential", n)
	}
	if got := api.CredentialInUse(); got != "credential-existing" {
		t.Errorf("credential in use = %q, want the stored one", got)
	}
}

func TestNoCredentialAndNoBootstrapTokenIsFatal(t *testing.T) {
	api := pulsetest.NewAPI()
	a, err := pulseagent.New(pulseagent.Options{
		Config: pulseagent.RuntimeConfig{
			Name:           "agent-a",
			CredentialFile: filepath.Join(t.TempDir(), "missing.json"),
			Limits:         pulseagent.Limits{Concurrency: 1, QPS: 1, Timeout: time.Second},
		},
		API: api,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Fatal rather than a retry loop: there is no way forward, and a loop
	// would hide the misconfiguration behind reconnect noise.
	if err := a.Run(context.Background()); err == nil {
		t.Fatal("want an error when there is no credential and no token")
	}
}

func TestRevokedCredentialTriggersReRegistration(t *testing.T) {
	credFile := filepath.Join(t.TempDir(), "credential.json")
	if err := pulseagent.SaveCredential(credFile, pulseagent.Credential{
		AgentID: "agent-old", Credential: "credential-stale",
	}); err != nil {
		t.Fatal(err)
	}

	api := pulsetest.NewAPI().WithIdentity("agent-new", "credential-fresh")
	api.RejectCredential(true)

	a, err := pulseagent.New(pulseagent.Options{
		Config: pulseagent.RuntimeConfig{
			Name: "agent-a", BootstrapToken: "boot-1", CredentialFile: credFile,
			Limits: pulseagent.Limits{Concurrency: 1, QPS: 100, Timeout: time.Second},
		},
		API:               api,
		HeartbeatInterval: 5 * time.Millisecond,
		// No jitter and a tiny base: the test should not wait out a real
		// backoff.
		Backoff: pulse.Backoff{Base: time.Millisecond, Cap: time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, stop, errCh := run(t, a)
	defer func() { stop(); <-errCh }()

	// It should re-register, then (once the fake stops rejecting) connect with
	// the new credential.
	deadline := time.Now().Add(2 * time.Second)
	for len(api.Registrations()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(api.Registrations()) == 0 {
		t.Fatal("never re-registered after the credential was rejected")
	}
	api.RejectCredential(false)

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := api.WaitStream(waitCtx); err != nil {
		t.Fatalf("never reconnected: %v", err)
	}
	if got := api.CredentialInUse(); got != "credential-fresh" {
		t.Errorf("credential in use = %q, want the freshly registered one", got)
	}
}

func TestRevokedCredentialWithoutTokenIsFatal(t *testing.T) {
	credFile := filepath.Join(t.TempDir(), "credential.json")
	if err := pulseagent.SaveCredential(credFile, pulseagent.Credential{
		AgentID: "a", Credential: "stale",
	}); err != nil {
		t.Fatal(err)
	}
	api := pulsetest.NewAPI()
	api.RejectCredential(true)
	a, err := pulseagent.New(pulseagent.Options{
		Config: pulseagent.RuntimeConfig{
			Name: "agent-a", CredentialFile: credFile,
			Limits: pulseagent.Limits{Concurrency: 1, QPS: 1, Timeout: time.Second},
		},
		API: api, Backoff: pulse.Backoff{Base: time.Millisecond, Cap: time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = a.Run(context.Background())
	if err == nil {
		t.Fatal("want an error: the credential is dead and there is no way to get a new one")
	}
}

func TestHeartbeatsCarryVersionAndAppliedConfig(t *testing.T) {
	a, api, _ := newAgent(t, nil)
	_, stop, errCh := run(t, a)
	defer func() { stop(); <-errCh }()

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, err := api.WaitStream(waitCtx)
	if err != nil {
		t.Fatal(err)
	}

	// The first beat is immediate, not one interval late: an agent that
	// reconnects after a blip should not wait to be seen as online.
	hbs, err := s.WaitHeartbeats(2, 2*time.Second)
	if err != nil {
		t.Fatalf("heartbeats: %v (got %d)", err, len(hbs))
	}
	if hbs[0].GetVersion() != "test" {
		t.Errorf("heartbeat version = %q, want %q", hbs[0].GetVersion(), "test")
	}
	if hbs[0].GetConfigVersion() != 0 {
		t.Errorf("config version = %d before any push, want 0", hbs[0].GetConfigVersion())
	}
}

func TestReconnectsAfterStreamBreaks(t *testing.T) {
	a, api, _ := newAgent(t, func(o *pulseagent.Options) {
		o.Backoff = pulse.Backoff{Base: time.Millisecond, Cap: 2 * time.Millisecond}
	})
	_, stop, errCh := run(t, a)
	defer func() { stop(); <-errCh }()

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	first, err := api.WaitStream(waitCtx)
	if err != nil {
		t.Fatal(err)
	}
	first.Break(errors.New("connection reset"))

	second, err := api.WaitStream(waitCtx)
	if err != nil {
		t.Fatalf("did not reconnect: %v", err)
	}
	if second == first {
		t.Fatal("got the same stream back")
	}
	if _, err := second.WaitHeartbeats(1, 2*time.Second); err != nil {
		t.Errorf("no heartbeat on the new stream: %v", err)
	}
}

func TestShutdownClosesSendCleanly(t *testing.T) {
	a, api, _ := newAgent(t, nil)
	_, stop, errCh := run(t, a)

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, err := api.WaitStream(waitCtx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.WaitHeartbeats(1, time.Second); err != nil {
		t.Fatal(err)
	}

	stop()
	if err := <-errCh; err != nil {
		t.Errorf("Run returned %v, want nil on shutdown", err)
	}
	// A clean half-close distinguishes "we are leaving" from "the connection
	// dropped", which is what lets the server log the difference.
	if !s.CloseSendCalled() {
		t.Error("shutdown did not close the send side")
	}
}

func TestServerShutdownCommandStopsTheAgent(t *testing.T) {
	a, api, _ := newAgent(t, nil)
	_, stop, errCh := run(t, a)
	defer stop()

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, err := api.WaitStream(waitCtx)
	if err != nil {
		t.Fatal(err)
	}
	s.PushCommand(pulsev1.Command_KIND_SHUTDOWN)

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("Run returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not stop on a server shutdown command")
	}
}

func TestReloadReReadsConfigButKeepsIdentity(t *testing.T) {
	var reloads atomic.Int64
	a, api, credFile := newAgent(t, func(o *pulseagent.Options) {
		o.ReloadConfig = func() (pulseagent.RuntimeConfig, error) {
			reloads.Add(1)
			// A reloaded file that tries to change identity and connectivity:
			// those must be ignored on a running agent.
			return pulseagent.RuntimeConfig{
				Name:           "someone-else",
				ServerAddr:     "elsewhere:9999",
				BootstrapToken: "different",
				CredentialFile: "/tmp/elsewhere.json",
				Limits:         pulseagent.Limits{Concurrency: 7, QPS: 70, Timeout: 7 * time.Second},
				LogLevel:       "debug",
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
	s.PushCommand(pulsev1.Command_KIND_RELOAD)

	deadline := time.Now().Add(2 * time.Second)
	for reloads.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if reloads.Load() == 0 {
		t.Fatal("RELOAD did not call ReloadConfig")
	}

	// Poll: the swap happens on the receive goroutine.
	for time.Now().Before(deadline) {
		if a.Config().Limits.Concurrency == 7 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	cfg := a.Config()
	if cfg.Limits.Concurrency != 7 || cfg.Limits.QPS != 70 {
		t.Errorf("limits not reloaded: %+v", cfg.Limits)
	}
	// Identity and connectivity are pinned for the process's lifetime.
	if cfg.Name != "agent-a" {
		t.Errorf("name changed to %q on reload", cfg.Name)
	}
	if cfg.ServerAddr != "localhost:0" {
		t.Errorf("server address changed to %q on reload", cfg.ServerAddr)
	}
	if cfg.BootstrapToken != "boot-1" {
		t.Error("bootstrap token changed on reload")
	}
	if cfg.CredentialFile != credFile {
		t.Errorf("credential file changed to %q on reload", cfg.CredentialFile)
	}
}

func TestRegisterReportsCapabilitiesFromExecutors(t *testing.T) {
	a, api, _ := newAgent(t, func(o *pulseagent.Options) {
		o.Executors = pulseagent.Executors{
			"build":  pulseagent.ExecutorFunc(nil),
			"deploy": pulseagent.ExecutorFunc(nil),
		}
	})
	_, stop, errCh := run(t, a)
	defer func() { stop(); <-errCh }()

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := api.WaitStream(waitCtx); err != nil {
		t.Fatal(err)
	}
	regs := api.Registrations()
	if len(regs) == 0 {
		t.Fatal("no registration")
	}
	caps := regs[0].GetCapabilities()
	if len(caps) != 2 || caps[0] != "build" || caps[1] != "deploy" {
		t.Errorf("capabilities = %v, want [build deploy] sorted", caps)
	}
}

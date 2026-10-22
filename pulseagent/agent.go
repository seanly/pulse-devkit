// Package pulseagent is the agent-side runtime: registration, the long-lived
// connection, and the worker pool that executes jobs.
//
// The shape of the loop, in one breath: read the credential file, register if
// it is missing, connect, heartbeat every 15s, take jobs until the stream
// dies, then back off exponentially and do it again. Everything the agent
// needs to resume lives in the credential file; nothing else is state.
package pulseagent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pulsev1 "github.com/seanly/pulse-devkit/pb/pulse/v1"
	"github.com/seanly/pulse-devkit/pulse"
)

// Agent is the runnable agent. Construct with New, start with Run.
type Agent struct {
	opts Options
	log  *slog.Logger
	api  API

	// cfg is the live configuration. Swapped wholesale by RELOAD; read through
	// the pointer so the dispatch path stays lock-free.
	cfg atomic.Pointer[RuntimeConfig]
	// lim is the local limits, the floor a withdrawn override falls back to.
	lim atomic.Pointer[Limits]

	// appliedConfig is the version of the last server push this agent actually
	// applied, echoed in every heartbeat so a console can say "in effect"
	// instead of guessing. It lives on the Agent rather than the session so a
	// reconnect does not look like a rollback to version 0.
	appliedConfig atomic.Int64
	// pushed is the last config that passed validation, nil before any push.
	// Kept so a RELOAD re-reading local configuration cannot silently undo the
	// server's overrides, and so a reconnect can re-apply what we already know
	// instead of running one round on the old values.
	pushed atomic.Pointer[pulsev1.AgentConfig]
	// run is the current session's runner, nil between sessions. RELOAD needs
	// it, and RELOAD arrives on a different code path than the one that owns
	// the runner.
	run atomic.Pointer[runner]

	// admit is the optional local policy check, installed by WithAdmitter.
	admit atomic.Pointer[admitFunc]

	// baseExec is every executor the host registered, before policy filtering.
	// A push can only remove from this set, so the unfiltered map has to
	// outlive startup.
	baseExec Executors
	// localPolicy is the owner's floor. Nil means the host opted out of
	// filtering. effective is that floor after any tightening push.
	localPolicy *Policy
	effective   atomic.Pointer[Policy]

	// stop shuts the agent down from a server Command; it cancels the same
	// context SIGTERM does, so graceful drain applies either way.
	stop context.CancelFunc
}

// admitFunc is boxed so it can live in an atomic.Pointer.
type admitFunc struct{ fn func(pulse.Job) error }

// New assembles the agent.
func New(o Options) (*Agent, error) {
	if err := o.validate(); err != nil {
		return nil, err
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	if o.Metrics == nil {
		o.Metrics = pulse.NopMetrics{}
	}
	if o.Clock == nil {
		o.Clock = pulse.SystemClock
	}
	if o.Executors == nil {
		o.Executors = Executors{}
	}
	base := copyExecutors(o.Executors)
	if o.Policy != nil {
		p := o.Policy.withDefaultKinds()
		if err := p.Validate(); err != nil {
			return nil, err
		}
		o.Policy = &p
		o.Executors = p.Filter(base)
	}
	if o.Backoff == (pulse.Backoff{}) {
		o.Backoff = pulse.DefaultBackoff()
	}
	if o.HeartbeatInterval <= 0 {
		o.HeartbeatInterval = pulse.HeartbeatInterval
	}
	if o.DrainTimeout <= 0 {
		o.DrainTimeout = pulse.DrainTimeout
	}
	o.Config.Limits = o.Config.Limits.withDefaults()

	a := &Agent{opts: o, log: o.Logger, api: o.API, baseExec: base, localPolicy: o.Policy}
	if o.Policy != nil {
		pol := *o.Policy
		a.effective.Store(&pol)
	}
	cfg := o.Config
	a.cfg.Store(&cfg)
	lim := cfg.Limits
	a.lim.Store(&lim)
	return a, nil
}

// SetAdmitter installs a local policy check consulted before a job is
// accepted. pulseguard provides one; call this before Run.
//
// Returning an error from fn declines the job with AckRejected, which tells the
// server that re-dispatching to a different agent may still work but this one
// will keep refusing.
func (a *Agent) SetAdmitter(fn func(pulse.Job) error) {
	a.admit.Store(&admitFunc{fn: fn})
}

// Config returns the live configuration.
func (a *Agent) Config() RuntimeConfig { return *a.cfg.Load() }

// AppliedConfigVersion is the pushed-config version currently in effect, 0 if
// the agent has never applied one.
func (a *Agent) AppliedConfigVersion() int64 { return a.appliedConfig.Load() }

// Run is the agent's lifetime. It returns nil on shutdown, or an error it
// cannot recover from — a revoked credential with no bootstrap token to
// re-register with.
func (a *Agent) Run(ctx context.Context) error {
	ctx, a.stop = context.WithCancel(ctx)
	defer a.stop()

	cred, err := a.ensureCredential(ctx)
	if err != nil {
		return err
	}
	a.api.SetCredential(cred.Credential)

	attempt := 0
	for {
		err := a.session(ctx, cred)
		if ctx.Err() != nil {
			// Shutdown mid-session is the good path: the session already
			// drained its jobs.
			return nil
		}
		a.opts.Metrics.Set(pulse.MetricConnected, 0)
		a.opts.Metrics.Inc(pulse.MetricReconnects)

		if status.Code(err) == codes.Unauthenticated {
			// The credential was revoked (or never was valid). The only way on
			// is a fresh registration, which needs a bootstrap token.
			if a.Config().BootstrapToken == "" {
				return fmt.Errorf("pulseagent: credential rejected and no bootstrap token configured: %w", err)
			}
			a.log.Warn("credential rejected; re-registering", "error", err)
			if err := RemoveCredential(a.Config().CredentialFile); err != nil {
				a.log.Warn("remove stale credential", "error", err)
			}
			if cred, err = a.register(ctx); err != nil {
				return fmt.Errorf("pulseagent: re-registration failed: %w", err)
			}
			a.api.SetCredential(cred.Credential)
			attempt = 0
			continue
		}

		sleep := a.opts.Backoff.Delay(attempt)
		a.log.Warn("connection lost; reconnecting", "error", err, "backoff", sleep)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(sleep):
		}
		attempt++
	}
}

// ensureCredential loads the stored credential, registering on first start.
func (a *Agent) ensureCredential(ctx context.Context) (Credential, error) {
	cfg := a.Config()
	if cred, exists, err := LoadCredential(cfg.CredentialFile); err != nil {
		a.log.Warn("credential file unusable; will re-register", "error", err)
	} else if exists {
		a.log.Info("credential loaded", "agent_id", cred.AgentID, "file", cfg.CredentialFile)
		return cred, nil
	}

	if cfg.BootstrapToken == "" {
		return Credential{}, fmt.Errorf(
			"pulseagent: no credential at %s and no bootstrap token to register with", cfg.CredentialFile)
	}
	return a.register(ctx)
}

// register exchanges the bootstrap token for a fresh credential and persists
// it. The credential never appears in the log.
func (a *Agent) register(ctx context.Context) (Credential, error) {
	cfg := a.Config()
	caps := cfg.Capabilities
	if len(caps) == 0 {
		caps = a.opts.Executors.kinds()
	}
	sort.Strings(caps)

	res, err := a.api.Register(ctx, &pulsev1.RegisterRequest{
		BootstrapToken: cfg.BootstrapToken,
		Name:           cfg.Name,
		Version:        a.opts.Version,
		Labels:         cfg.Labels,
		LocalConfig:    LocalConfigToProto(a.localConfig()),
		Capabilities:   caps,
		Facts:          a.registrationFacts(),
	})
	if err != nil {
		return Credential{}, fmt.Errorf("pulseagent: register: %w", err)
	}
	cred := Credential{AgentID: res.GetAgentId(), Credential: res.GetCredential()}
	if err := SaveCredential(cfg.CredentialFile, cred); err != nil {
		return cred, err
	}
	a.log.Info("registered", "agent_id", cred.AgentID, "file", cfg.CredentialFile, "mode", "0600")
	logLabelDivergence(a.log, cfg.Labels, res.GetEffectiveLabels())
	return cred, nil
}

// registrationFacts collects the host's first facts document. A collector
// that fails is logged and omitted: enrollment must not wait on a fact the
// server can ask for again with agent.info.
func (a *Agent) registrationFacts() []byte {
	if a.opts.CollectFacts == nil {
		return nil
	}
	raw, err := a.opts.CollectFacts()
	if err != nil {
		a.log.Warn("facts collection failed; registering without them", "error", err)
		return nil
	}
	return raw
}

// localConfig reports what this agent's own configuration says, so a console
// can show the local value beside each override box.
func (a *Agent) localConfig() pulse.LocalConfig {
	cfg := a.Config()
	return pulse.LocalConfig{
		Concurrency: cfg.Limits.Concurrency,
		QPS:         cfg.Limits.QPS,
		Timeout:     cfg.Limits.Timeout,
		LogLevel:    cfg.LogLevel,
		Extra:       cfg.Extra,
	}
}

// logLabelDivergence says out loud when the server's stored labels differ from
// what this agent asked for, so whoever is reading the log on this host learns
// that the local configuration is not the last word.
//
// The local file is deliberately not rewritten: configuration files belong to
// people, not to programs.
func logLabelDivergence(log *slog.Logger, reported, effective map[string]string) {
	var changed, removed []string
	for k, v := range reported {
		got, ok := effective[k]
		switch {
		case !ok:
			removed = append(removed, k)
		case got != v:
			changed = append(changed, fmt.Sprintf("%s=%s (local %s)", k, got, v))
		}
	}
	var added []string
	for k, v := range effective {
		if _, ok := reported[k]; !ok {
			added = append(added, fmt.Sprintf("%s=%s", k, v))
		}
	}
	if len(changed) == 0 && len(removed) == 0 && len(added) == 0 {
		return
	}
	sort.Strings(changed)
	sort.Strings(removed)
	sort.Strings(added)
	log.Info("server labels differ from local config; server wins, local config left unchanged",
		"overridden", strings.Join(changed, ", "),
		"removed", strings.Join(removed, ", "),
		"added_by_server", strings.Join(added, ", "))
}

func statusFromErr(err error) string {
	if err == nil {
		return ""
	}
	return status.Code(err).String() + ": " + err.Error()
}

var errSendQueueFull = errors.New("pulseagent: send queue full")

package pulseagent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/seanly/pulse-devkit/pulse"
)

// RuntimeConfig is everything the agent needs to run. The framework does not
// parse configuration files: the host loads its own format (TOML, YAML, flags,
// environment) and fills this in.
//
// That is why RELOAD takes a callback rather than a file path — see
// Options.ReloadConfig.
type RuntimeConfig struct {
	// Name is this agent's stable identity. Re-registering the same name
	// reuses the existing server-side row.
	Name string
	// ServerAddr is informational here; the API was already dialed with it.
	// Carried so the agent can log where it is connected.
	ServerAddr string
	// BootstrapToken enrolls this agent on first start, and re-enrolls it if
	// its credential is ever revoked. Without one, a rejected credential is
	// fatal — there is no way back.
	BootstrapToken string
	// CredentialFile is where the long-lived credential is persisted, mode
	// 0600.
	CredentialFile string
	// Labels are this agent's own claim about itself. The server may override
	// them; the local value is never rewritten.
	Labels map[string]string
	// Capabilities are the job kinds this agent can execute. Defaults to the
	// registered executors' kinds when empty.
	Capabilities []string

	Limits Limits

	// LogLevel is the local level, and the value a withdrawn server override
	// falls back to.
	LogLevel string
	// Extra is reported to the server for display only: local-only switches,
	// counts of local policy entries. Never acted on.
	Extra map[string]string
}

// Limits bound how much work an agent takes on. Each can be overridden by a
// server push; an unset override falls back to the value here.
type Limits struct {
	// Concurrency caps in-flight jobs. There is deliberately no queue beyond
	// the slots themselves: a job the agent cannot start now is better served
	// by another agent than lined up behind this one's backlog.
	Concurrency int
	// QPS caps job starts per second.
	QPS int
	// Timeout is the local ceiling on one job. A server asking for longer gets
	// this instead — an agent's owner decides how long their machine may be
	// occupied.
	Timeout time.Duration
}

// Defaults applied when a field is zero.
const (
	defaultConcurrency = 8
	defaultQPS         = 20
	defaultTimeout     = 30 * time.Second
)

func (l Limits) withDefaults() Limits {
	if l.Concurrency <= 0 {
		l.Concurrency = defaultConcurrency
	}
	if l.QPS <= 0 {
		l.QPS = defaultQPS
	}
	if l.Timeout <= 0 {
		l.Timeout = defaultTimeout
	}
	return l
}

// Executor runs one job.
//
// The context's deadline is the effective timeout: the smaller of what the
// server asked for and the agent's local ceiling. An Executor may trust it
// without re-checking.
//
// Returning an error is the normal way to report failure — the framework turns
// it into a status=error result with CodeExecutorError, so an Executor does
// not write that boilerplate. Returning a Result with no error gives the
// application full control, including reporting StatusSkipped.
//
// A panic is caught and becomes CodeExecutorPanic: one bad job must not take
// the agent down with it.
//
// Implementations that reach the network and use pulseguard must call
// Guard.CheckIPs after resolving names. The framework cannot do it — it never
// sees the resolved addresses.
type Executor interface {
	Execute(ctx context.Context, job pulse.Job) (pulse.Result, error)
}

// ExecutorFunc adapts a function to Executor.
type ExecutorFunc func(ctx context.Context, job pulse.Job) (pulse.Result, error)

func (f ExecutorFunc) Execute(ctx context.Context, job pulse.Job) (pulse.Result, error) {
	return f(ctx, job)
}

// Executors maps a job kind to its executor. The empty kind is valid: a
// single-purpose agent registers one executor under "" and the server never
// sets Job.Kind.
type Executors map[string]Executor

// kinds lists the registered kinds, for the capabilities it reports at
// registration.
func (e Executors) kinds() []string {
	out := make([]string, 0, len(e))
	for k := range e {
		if k != "" {
			out = append(out, k)
		}
	}
	return out
}

// ConfigPayloadHandler consumes the application-defined part of a pushed
// AgentConfig.
//
// Returning an error rejects the ENTIRE config push, not just the payload: a
// push is all-or-nothing, so half-applied configuration never happens. The
// agent keeps running its previous config and the server sees config_version
// stay put, which is how an operator learns something is wrong.
type ConfigPayloadHandler func(payload []byte) error

// Options configures New.
type Options struct {
	// Config is the startup configuration.
	Config RuntimeConfig
	// API is the server-facing client. Required.
	API API
	// Executors handle jobs by kind. An empty map is allowed: the agent runs
	// end to end and reports CodeNoExecutor for everything, which is a useful
	// way to verify enrollment before writing any business logic.
	Executors Executors
	// Policy filters Executors and admits jobs. Nil means the host did not
	// opt in, and every registered executor stays registered — the framework's
	// own tests rely on that. A host whose configuration file has no policy
	// section should pass DefaultPolicy, so an upgrade does not suddenly
	// grant write access.
	Policy *Policy
	// Version is this build's version, reported at registration and in every
	// heartbeat. The framework does not own a version global.
	Version string
	// Logger receives framework events. Nil discards them.
	//
	// *slog.Logger rather than a bespoke interface: it is stdlib, so this
	// costs no dependency, and a host that uses something else can bridge it
	// with a slog.Handler once instead of implementing an interface here.
	Logger *slog.Logger
	// Metrics receives counters and gauges. Nil discards them.
	Metrics pulse.Metrics
	// LevelSetter applies a pushed log_level. Nil makes level pushes a no-op
	// with one log line — a deployment that does not want its level changed
	// remotely is a legitimate configuration, not an error.
	LevelSetter pulse.LevelSetter
	// ReloadConfig is called on a RELOAD command and returns the freshly read
	// configuration. Nil means RELOAD only re-applies pushed overrides without
	// re-reading anything local.
	//
	// Identity and connectivity fields in the returned config are ignored:
	// a running agent cannot change its name, server or credential file.
	ReloadConfig func() (RuntimeConfig, error)
	// OnConfigPayload handles the application part of a pushed config.
	OnConfigPayload ConfigPayloadHandler
	// Clock overrides time. Nil means pulse.SystemClock.
	Clock pulse.Clock
	// Backoff overrides the reconnect schedule. Zero means
	// pulse.DefaultBackoff.
	Backoff pulse.Backoff
	// HeartbeatInterval overrides the heartbeat cadence. Zero means
	// pulse.HeartbeatInterval. Changing it without changing the server's
	// StaleTimeout will get the agent declared offline, so this exists for
	// tests rather than for tuning.
	HeartbeatInterval time.Duration
	// OnServerContact runs after a heartbeat is written to the stream. A
	// dead-man's switch clears itself here: the agent has reached the server,
	// so a write that was waiting on that fact can stand.
	OnServerContact func()
	// CollectFacts runs at registration and its bytes are stored as the
	// agent's first facts document. Nil sends nothing. The framework does
	// not interpret the bytes.
	CollectFacts func() ([]byte, error)
	// DrainTimeout bounds graceful shutdown. Zero means pulse.DrainTimeout.
	DrainTimeout time.Duration
}

func (o *Options) validate() error {
	if o.API == nil {
		return errors.New("pulseagent: API is required")
	}
	if o.Config.Name == "" {
		return errors.New("pulseagent: Config.Name is required")
	}
	if o.Config.CredentialFile == "" {
		return errors.New("pulseagent: Config.CredentialFile is required")
	}
	return nil
}

// validatePushed rejects a config the agent cannot apply. The server validates
// the same ranges when it is saved; this is the second line, for a server that
// is older, newer, or wrong.
func validatePushed(concurrency, qps, timeoutMS int32, logLevel string) error {
	if concurrency < 0 || concurrency > 1024 {
		return fmt.Errorf("concurrency %d out of range", concurrency)
	}
	if qps < 0 || qps > 10000 {
		return fmt.Errorf("qps %d out of range", qps)
	}
	if timeoutMS != 0 && (timeoutMS < 100 || timeoutMS > 3_600_000) {
		return fmt.Errorf("timeout %dms out of range 100ms-1h", timeoutMS)
	}
	if logLevel != "" && !validLevel(logLevel) {
		return fmt.Errorf("unknown log level %q", logLevel)
	}
	return nil
}

func validLevel(l string) bool {
	switch l {
	case "debug", "info", "warn", "error":
		return true
	}
	return false
}

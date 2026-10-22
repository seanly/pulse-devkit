package pulseagent

import (
	"time"

	pulsev1 "github.com/seanly/pulse-devkit/pb/pulse/v1"
)

// applyConfig applies a server-pushed AgentConfig to the live session. Fields
// left at zero mean "no opinion": the server sends the whole config every
// time, so a cleared override arrives as 0 and restores the local value rather
// than being ignored.
//
// A push is all-or-nothing. Everything is validated first; one bad field
// discards the whole message and leaves appliedConfig where it was, so a
// console stays on "pending" and the operator can see something went wrong.
// Half a config applied is harder to debug than the old one still running.
func (a *Agent) applyConfig(cfg *pulsev1.AgentConfig, run *runner) {
	if cfg == nil {
		return
	}
	if err := validatePushed(cfg.GetConcurrency(), cfg.GetQps(), cfg.GetTimeoutMs(), cfg.GetLogLevel()); err != nil {
		a.log.Warn("discarding invalid pushed config; previous config still in effect",
			"version", cfg.GetVersion(), "error", err)
		return
	}
	nextPolicy, havePolicy, err := a.previewPolicy(cfg.GetPayload())
	if err != nil {
		a.log.Warn("discarding pushed config; policy rejected",
			"version", cfg.GetVersion(), "error", err)
		return
	}
	// The application's part of the push is validated in the same pass, and
	// its rejection discards the whole message too — a config is one unit.
	if h := a.opts.OnConfigPayload; h != nil {
		if err := h(cfg.GetPayload()); err != nil {
			a.log.Warn("config payload rejected; previous config still in effect",
				"version", cfg.GetVersion(), "error", err)
			return
		}
	}

	if havePolicy {
		pol := nextPolicy
		a.effective.Store(&pol)
		if run != nil {
			run.setExecutors(pol.Filter(a.baseExec))
		}
	}

	a.pushed.Store(cfg)
	a.applyPushed(cfg, run)
	// Recorded last: the heartbeat only claims a version once every field of
	// it is in effect.
	a.appliedConfig.Store(cfg.GetVersion())
}

// applyPushed installs an already-validated config (or nil, meaning "no server
// opinion at all") onto the live runner.
//
// Called on push, on reconnect, and after a RELOAD — RELOAD carries no
// message, so the caller passes the stored one.
func (a *Agent) applyPushed(cfg *pulsev1.AgentConfig, run *runner) {
	if run == nil {
		return
	}
	local := *a.lim.Load()

	concurrency := local.Concurrency
	if n := int(cfg.GetConcurrency()); n > 0 {
		concurrency = n
	}
	qps := local.QPS
	if n := int(cfg.GetQps()); n > 0 {
		qps = n
	}
	timeout := local.Timeout
	if ms := cfg.GetTimeoutMs(); ms > 0 {
		timeout = time.Duration(ms) * time.Millisecond
	}

	if run.gate.limitNow() != concurrency {
		run.gate.setLimit(concurrency)
	}
	run.setQPS(qps)
	run.setTimeout(timeout)

	// An override that was withdrawn has to go back to the local level, not
	// stay at the last pushed one.
	level := a.Config().LogLevel
	if lvl := cfg.GetLogLevel(); lvl != "" {
		level = lvl
	}
	if level != "" {
		a.setLevel(level)
	}

	if cfg != nil {
		a.log.Info("applied pushed config", "version", cfg.GetVersion(),
			"concurrency", concurrency, "qps", qps, "timeout", timeout.String(),
			"log_level", level)
	}
}

// setLevel applies a log level through the injected setter.
//
// No setter is not an error: a deployment that does not want the server
// adjusting its log level is a legitimate choice. Say so once at debug level
// and carry on — the rest of the config still applies.
func (a *Agent) setLevel(level string) {
	if a.opts.LevelSetter == nil {
		a.log.Debug("log level push ignored: no LevelSetter installed", "level", level)
		return
	}
	if err := a.opts.LevelSetter.SetLevel(level); err != nil {
		a.log.Warn("set log level", "level", level, "error", err)
	}
}

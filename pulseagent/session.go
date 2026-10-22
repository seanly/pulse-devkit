package pulseagent

import (
	"context"
	"time"

	pulsev1 "github.com/seanly/pulse-devkit/pb/pulse/v1"
	"github.com/seanly/pulse-devkit/pulse"
)

// session is one connection lifetime: heartbeat loop, receive loop, and the
// graceful drain when either side ends it. It returns the error that ended the
// session, for Run to classify.
func (a *Agent) session(ctx context.Context, cred Credential) error {
	stream, err := a.api.Connect(ctx)
	if err != nil {
		return err
	}
	a.opts.Metrics.Set(pulse.MetricConnected, 1)
	a.log.Info("connected", "agent_id", cred.AgentID, "server", a.Config().ServerAddr)

	// Sends from the heartbeat, from acks and from shutdown funnel through one
	// sender goroutine — grpc-go streams allow a single sending side.
	//
	// sendCh is never closed: a heartbeat mid-send when shutdown begins would
	// otherwise panic on a closed channel. Instead senderDone retires the
	// sender goroutine, and late non-blocking sends land in the buffer and are
	// dropped with it.
	sendCh := make(chan *pulsev1.AgentMessage, 64)
	sendErr := make(chan error, 1)
	senderDone := make(chan struct{})
	go func() {
		for {
			select {
			case <-senderDone:
				sendErr <- nil
				return
			default:
			}
			select {
			case msg := <-sendCh:
				if err := stream.Send(msg); err != nil {
					sendErr <- err
					return
				}
			case <-senderDone:
				sendErr <- nil
				return
			}
		}
	}()
	send := func(m *pulsev1.AgentMessage) error {
		select {
		case sendCh <- m:
			return nil
		default:
			// Never block: this is called from the receive loop, which must
			// keep reading or heartbeats stall behind it.
			return errSendQueueFull
		}
	}

	lim := *a.lim.Load()
	var admit func(pulse.Job) error
	if boxed := a.admit.Load(); boxed != nil {
		admit = boxed.fn
	}
	run := newRunner(&a.opts, lim, admit)
	a.run.Store(run)
	defer a.run.Store(nil)

	// A config pushed in an earlier session outlives that session: the server
	// will push again on connect, but applying what we already know avoids one
	// round on the old values right after a reconnect.
	a.applyPushed(a.pushed.Load(), run)

	report := func(rctx context.Context, res *pulsev1.ReportResultRequest) {
		if _, err := a.api.ReportResult(rctx, res); err != nil {
			a.opts.Metrics.Inc(pulse.MetricReportsFailed)
			a.log.Warn("report failed", "job_id", res.GetJobId(), "error", err)
		}
	}

	// Heartbeat: an immediate beat on connect (the server records one itself,
	// but an explicit beat keeps the two sides from arguing about who flipped
	// the agent online), then every interval.
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	go func() {
		beat := func() error {
			a.opts.Metrics.Inc(pulse.MetricHeartbeats)
			return send(&pulsev1.AgentMessage{Payload: &pulsev1.AgentMessage_Heartbeat{
				Heartbeat: &pulsev1.Heartbeat{
					Version:       a.opts.Version,
					RunningJobs:   run.runningJobs(),
					ConfigVersion: a.appliedConfig.Load(),
				},
			}})
		}
		if err := beat(); err != nil {
			return
		}
		ticker := time.NewTicker(a.opts.HeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				if err := beat(); err != nil {
					return
				}
			}
		}
	}()

	// Receive loop: the only goroutine that reads. Jobs dispatch without
	// blocking; commands act immediately.
	recvErr := make(chan error, 1)
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			switch payload := msg.GetPayload().(type) {
			case *pulsev1.ServerMessage_Job:
				run.dispatch(ctx, payload.Job, send, report)
			case *pulsev1.ServerMessage_Command:
				a.command(payload.Command)
			case *pulsev1.ServerMessage_Config:
				a.applyConfig(payload.Config, run)
			default:
				// A server newer than this agent: say so and carry on rather
				// than treating an unknown message as a protocol failure.
				a.log.Warn("unknown server message; ignoring", "message", msg.String())
			}
		}
	}()

	var sessionErr error
	select {
	case <-ctx.Done():
		// SIGTERM or a server-issued shutdown: stop taking new work, let
		// running jobs finish, then close the stream cleanly. Reports go out
		// on the unary RPC, which survives the stream closing. The sender is
		// retired before CloseSend so the send side stays single-goroutine
		// throughout.
		a.log.Info("shutting down; draining running jobs", "deadline", a.opts.DrainTimeout.String())
		hbCancel()
		run.drain(a.opts.DrainTimeout)
		close(senderDone)
		<-sendErr
		_ = stream.CloseSend()
		return nil
	case err := <-recvErr:
		sessionErr = err
	case err := <-sendErr:
		sessionErr = err
	}
	hbCancel()
	// If the sender is the one that failed it has already exited; otherwise
	// this retires it. Nobody touches the stream after this point.
	close(senderDone)
	a.log.Info("session ended", "error", statusFromErr(sessionErr))
	return sessionErr
}

// command handles a server Command message.
func (a *Agent) command(cmd *pulsev1.Command) {
	switch cmd.GetKind() {
	case pulsev1.Command_KIND_RELOAD:
		a.reload()
	case pulsev1.Command_KIND_SHUTDOWN:
		a.log.Info("shutdown requested by server")
		a.stop()
	default:
		a.log.Warn("unknown command; ignoring", "kind", cmd.GetKind())
	}
}

// reload re-reads local configuration and lays the pushed overrides back on
// top.
//
// That second step is not optional. Without it the agent silently falls back
// to its local limits while the server still believes its own values are in
// effect and config_version still agrees with it — a nasty ordering bug to
// chase.
func (a *Agent) reload() {
	if a.opts.ReloadConfig == nil {
		a.log.Info("reload requested; no config reader installed, re-applying pushed overrides only")
		a.applyPushed(a.pushed.Load(), a.run.Load())
		return
	}
	a.log.Info("reload requested; re-reading config")
	cfg, err := a.opts.ReloadConfig()
	if err != nil {
		a.log.Warn("reload failed; keeping current config", "error", err)
		return
	}
	// Identity and connectivity cannot change under a running agent: those
	// fields keep their current values, and only policy moves.
	cur := a.Config()
	cfg.Name = cur.Name
	cfg.ServerAddr = cur.ServerAddr
	cfg.BootstrapToken = cur.BootstrapToken
	cfg.CredentialFile = cur.CredentialFile
	cfg.Limits = cfg.Limits.withDefaults()

	a.cfg.Store(&cfg)
	lim := cfg.Limits
	a.lim.Store(&lim)
	a.log.Info("config reloaded", "concurrency", lim.Concurrency, "qps", lim.QPS,
		"timeout", lim.Timeout.String())
	a.applyPushed(a.pushed.Load(), a.run.Load())
}

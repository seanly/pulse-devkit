package pulsereg

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	pulsev1 "github.com/seanly/pulse-devkit/pb/pulse/v1"
	"github.com/seanly/pulse-devkit/pulse"
	"github.com/seanly/pulse-devkit/pulseagent"
)

// Heartbeat records liveness and what an agent reports about itself. The
// caller supplies the agent row from credential verification; the peer address
// comes from the connection, never from the payload.
func (s *Service) Heartbeat(ctx context.Context, agent pulse.Agent, hb *pulsev1.Heartbeat, peerIP string) error {
	version := agent.Version
	if v := hb.GetVersion(); v != "" {
		version = v
	}
	if err := s.agents.Heartbeat(ctx, agent.ID, version, peerIP); err != nil {
		return err
	}
	s.registry.observe(agent.ID, hb.GetLoad(), hb.GetRunningJobs(), hb.GetConfigVersion())
	return nil
}

// Runtime reports the live stream state for one agent. Everything in it is
// derived from the stream, so it empties the moment the stream goes away.
func (s *Service) Runtime(agentID string) pulse.AgentRuntime {
	return s.registry.runtime(agentID)
}

// ConnectedAgents is how many agents hold live streams. Display only: in a
// multi-instance deployment this counts only the streams on this instance.
func (s *Service) ConnectedAgents() int { return s.registry.count() }

// SendTo pushes a message down an agent's live stream.
//
// An error means the stream is gone and the work must be requeued rather than
// retried on the same handle. This is the method pulsequeue depends on, via
// pulse.Registry.
func (s *Service) SendTo(agentID string, msg *pulse.ServerMessage) error {
	wire, err := serverMessageToProto(msg)
	if err != nil {
		return err
	}
	return s.registry.send(agentID, wire)
}

var _ pulse.Registry = (*Service)(nil)

func serverMessageToProto(msg *pulse.ServerMessage) (*pulsev1.ServerMessage, error) {
	switch {
	case msg == nil:
		return nil, errors.New("pulsereg: nil server message")
	case msg.Job != nil:
		return &pulsev1.ServerMessage{Payload: &pulsev1.ServerMessage_Job{
			Job: pulseagent.JobToProto(*msg.Job),
		}}, nil
	case msg.Config != nil:
		return &pulsev1.ServerMessage{Payload: &pulsev1.ServerMessage_Config{
			Config: pulseagent.ConfigToProto(*msg.Config),
		}}, nil
	case msg.Command != nil:
		return &pulsev1.ServerMessage{Payload: &pulsev1.ServerMessage_Command{
			Command: &pulsev1.Command{
				Kind:  pulsev1.Command_Kind(msg.Command.Kind),
				JobId: msg.Command.JobID,
			},
		}}, nil
	default:
		return nil, errors.New("pulsereg: empty server message")
	}
}

// PushConfig sends an agent its current server-side configuration.
//
// An offline agent is not an error: the configuration is already stored, and
// the push on connect will deliver it. Reported through the returned bool so a
// caller can say "saved, takes effect when the agent reconnects" rather than
// guessing.
func (s *Service) PushConfig(ctx context.Context, agentID string) (delivered bool, err error) {
	if s.configs == nil {
		return false, nil
	}
	cfg, err := s.configs.GetAgentConfig(ctx, agentID)
	switch {
	case errors.Is(err, pulse.ErrNotFound):
		// No override: nothing to say. The agent's local configuration already
		// governs, and pushing an empty config would only bump its applied
		// version to a version that does not exist.
		return false, nil
	case err != nil:
		return false, err
	}
	if err := s.SendTo(agentID, &pulse.ServerMessage{Config: &cfg}); err != nil {
		return false, nil
	}
	return true, nil
}

// SendCommand asks an agent to reload, shut down, or cancel one job.
//
// Build the command with pulse.Reload, pulse.Shutdown or pulse.Cancel.
func (s *Service) SendCommand(agentID string, cmd *pulse.Command) error {
	return s.SendTo(agentID, &pulse.ServerMessage{Command: cmd})
}

// Attach registers the send side of a live stream and returns a detach func.
// The gRPC layer calls it; it is exported so an alternative transport could.
func (s *Service) Attach(agentID string, send func(*pulsev1.ServerMessage) error) (detach func()) {
	return s.registry.attach(agentID, send)
}

// SetSink installs the result handler after construction.
//
// This exists because the registry and the queue reference each other: the
// registry hands results to a Sink, and a Sink marks jobs complete in the
// queue, which pushes through the registry. Something has to be wired second,
// and a setter is less trouble than making every caller build a placeholder.
//
// Call it during setup, before serving. It is not safe to call while streams
// are live.
func (s *Service) SetSink(sink pulse.Sink) { s.sink = sink }

// RunOfflineReaper sweeps agents whose heartbeats went stale, until ctx is
// cancelled.
//
// Marking an agent offline deliberately does not touch its jobs: a lease has
// its own expiry, and a five-second network blip should not requeue work that
// is still running fine.
func (s *Service) RunOfflineReaper(ctx context.Context) {
	ticker := time.NewTicker(pulse.ReaperInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.ReapOffline(ctx)
		}
	}
}

// ReapOffline runs one staleness sweep. Exposed so tests and an operator path
// can drive it without waiting for a tick.
func (s *Service) ReapOffline(ctx context.Context) {
	ids, err := s.agents.MarkStaleAgentsOffline(ctx, pulse.StaleTimeout)
	if err != nil {
		s.log.Error("agent reaper", "error", err)
		return
	}
	for _, id := range ids {
		s.log.Warn("agent went offline", "agent_id", id)
	}
}

// streamRegistry tracks one live stream per agent plus what it last reported.
//
// Replace-on-attach keeps a reconnecting agent from accumulating handles: the
// new stream wins and the old one's detach becomes a no-op. A leaked handle
// would keep a dead agent looking dispatchable.
type streamRegistry struct {
	mu      sync.Mutex
	streams map[string]*streamEntry
}

type streamEntry struct {
	send          func(*pulsev1.ServerMessage) error
	load          float64
	runningJobs   int32
	configVersion int64
}

func (r *streamRegistry) attach(agentID string, send func(*pulsev1.ServerMessage) error) func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.streams == nil {
		r.streams = map[string]*streamEntry{}
	}
	entry := &streamEntry{send: send}
	r.streams[agentID] = entry
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		// Identity comparison, not just the key: a reconnecting agent that
		// attached a newer stream must not have it removed when this older
		// stream's deferred detach runs.
		if r.streams[agentID] == entry {
			delete(r.streams, agentID)
		}
	}
}

func (r *streamRegistry) send(agentID string, msg *pulsev1.ServerMessage) error {
	r.mu.Lock()
	entry := r.streams[agentID]
	r.mu.Unlock()
	if entry == nil {
		return fmt.Errorf("pulsereg: agent %s has no live stream", agentID)
	}
	return entry.send(msg)
}

func (r *streamRegistry) observe(agentID string, load float64, runningJobs int32, configVersion int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e := r.streams[agentID]; e != nil {
		e.load = load
		e.runningJobs = runningJobs
		e.configVersion = configVersion
	}
}

func (r *streamRegistry) runtime(agentID string) pulse.AgentRuntime {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.streams[agentID]
	if e == nil {
		return pulse.AgentRuntime{}
	}
	return pulse.AgentRuntime{
		Online:               true,
		AppliedConfigVersion: e.configVersion,
		RunningJobs:          e.runningJobs,
		Load:                 e.load,
	}
}

func (r *streamRegistry) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.streams)
}

// RenewLease extends a leased job's deadline at the agent's request.
//
// The agent sends these while a long job runs; without them a job that takes
// longer than one lease period is reclaimed and dispatched again — waste for a
// probe, corruption for a write.
//
// ErrNotFound means the lease is already gone (the reaper got there first), and
// the agent is told nothing: it will find out when its report is not accepted.
// There is nothing useful it could do with the information mid-job anyway.
func (s *Service) RenewLease(ctx context.Context, agentID, jobID string) error {
	if s.jobs == nil {
		// No job store wired: a deployment that uses the registry without the
		// queue. Renewal is meaningless rather than an error.
		return nil
	}
	if jobID == "" {
		return errors.New("pulsereg: lease renewal without a job id")
	}
	err := s.jobs.RenewLease(ctx, jobID, agentID, s.leaseFor)
	if errors.Is(err, pulse.ErrNotFound) {
		s.log.Debug("lease renewal for a job no longer leased here",
			"agent_id", agentID, "job_id", jobID)
		return nil
	}
	return err
}

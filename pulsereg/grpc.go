// The gRPC surface of the registry. Thin by design: authentication, mapping
// registry errors onto status codes, and the stream loop. Policy lives in the
// Service.
package pulsereg

import (
	"context"
	"errors"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	pulsev1 "github.com/seanly/pulse-devkit/pb/pulse/v1"
	"github.com/seanly/pulse-devkit/pulse"
	"github.com/seanly/pulse-devkit/pulseagent"
)

// credentialHeader is the metadata key credentials travel in: a bearer token
// in the authorization header, so proxies and TLS terminators see a
// conventional shape.
const credentialHeader = "authorization"

// GRPCServer implements pulsev1.AgentServiceServer.
type GRPCServer struct {
	pulsev1.UnimplementedAgentServiceServer
	svc *Service
}

// NewGRPCServer returns the gRPC-facing server. Register it with
// pulsev1.RegisterAgentServiceServer.
func NewGRPCServer(svc *Service) *GRPCServer { return &GRPCServer{svc: svc} }

// Register enrolls an agent with a bootstrap token.
func (g *GRPCServer) Register(ctx context.Context, req *pulsev1.RegisterRequest) (*pulsev1.RegisterResponse, error) {
	res, err := g.svc.Register(ctx, RegisterInput{
		BootstrapToken: req.GetBootstrapToken(),
		Name:           req.GetName(),
		Version:        req.GetVersion(),
		Labels:         req.GetLabels(),
		Capabilities:   req.GetCapabilities(),
		LocalConfig:    pulseagent.LocalConfigFromProto(req.GetLocalConfig()),
		PeerIP:         PeerIP(ctx),
	})
	if err != nil {
		return nil, mapRegistryError(err)
	}
	return &pulsev1.RegisterResponse{
		AgentId:         res.AgentID,
		Credential:      res.Credential,
		EffectiveLabels: res.EffectiveLabels,
	}, nil
}

// Connect is the agent's long-lived stream: heartbeats and acks up, jobs and
// commands down.
//
// The stream stays open for hours, so the registry handle must be dropped on
// every exit path — a leaked handle keeps a dead agent looking dispatchable.
func (g *GRPCServer) Connect(stream pulsev1.AgentService_ConnectServer) error {
	ctx := stream.Context()

	agent, err := g.authenticate(ctx)
	if err != nil {
		return err
	}
	peerIP := PeerIP(ctx)

	// The detach closure removes exactly this stream; a reconnecting agent
	// attaching a newer one leaves the old entry alone until its own deferred
	// detach runs. Deferred, so a panic in the loop below cannot leak it.
	detach := g.svc.Attach(agent.ID, stream.Send)
	defer detach()

	g.svc.log.Info("agent connected", "agent_id", agent.ID, "name", agent.Name, "remote", peerIP)

	// Arriving on the stream is itself liveness: an agent that reconnects
	// after a network blip should not wait a full heartbeat interval to be
	// considered online.
	if err := g.svc.Heartbeat(ctx, agent, &pulsev1.Heartbeat{Version: agent.Version}, peerIP); err != nil {
		return status.Error(codes.Internal, "heartbeat failed")
	}
	// Push configuration once per session. Binding delivery to the start of a
	// session removes the "how long after startup does an agent have its
	// config" window entirely — and a reconnect re-delivers for free.
	if _, err := g.svc.PushConfig(ctx, agent.ID); err != nil {
		// Storage trouble, not stream trouble: the session is fine, the agent
		// runs on its local configuration, and the next connect tries again.
		g.svc.log.Warn("push config on connect", "agent_id", agent.ID, "error", err)
	}

	for {
		msg, err := stream.Recv()
		if err != nil {
			// A clean half-close and a dropped TCP connection both land here;
			// either way the stream is over and the deferred detach runs.
			g.svc.log.Info("agent disconnected", "agent_id", agent.ID, "error", statusText(err))
			return nil
		}
		switch payload := msg.GetPayload().(type) {
		case *pulsev1.AgentMessage_Heartbeat:
			if err := g.svc.Heartbeat(ctx, agent, payload.Heartbeat, peerIP); err != nil {
				g.svc.log.Warn("heartbeat", "agent_id", agent.ID, "error", err)
			}
		case *pulsev1.AgentMessage_JobAck:
			if g.svc.sink != nil {
				ack := payload.JobAck
				// Must not block: this loop also carries heartbeats, and a
				// slow sink here would get the agent declared offline.
				g.svc.sink.Ack(agent.ID, ack.GetJobId(), ack.GetAccepted(), ack.GetReason())
			}
		case *pulsev1.AgentMessage_Lease:
			// Lease renewal for a long-running job. Extending it here is what
			// keeps a job that outlives its original lease from being
			// dispatched a second time.
			if err := g.svc.RenewLease(ctx, agent.ID, payload.Lease.GetJobId()); err != nil {
				g.svc.log.Warn("lease renewal",
					"agent_id", agent.ID, "job_id", payload.Lease.GetJobId(), "error", err)
			}
		case *pulsev1.AgentMessage_Progress:
			if g.svc.sink != nil {
				p := payload.Progress
				// Under far more pressure than Ack: a chatty job sends these
				// continuously. Same rule, more urgently — the sink must not
				// block this loop.
				g.svc.sink.Progress(agent.ID, pulse.ProgressChunk{
					JobID:  p.GetJobId(),
					Seq:    p.GetSeq(),
					Stream: p.GetStream(),
					Chunk:  p.GetChunk(),
				})
			}
		default:
			// An agent newer than this server: say so and carry on.
			g.svc.log.Warn("unknown agent message; ignoring", "agent_id", agent.ID)
		}
	}
}

// ReportResult delivers one finished job.
func (g *GRPCServer) ReportResult(ctx context.Context, req *pulsev1.ReportResultRequest) (*pulsev1.ReportResultResponse, error) {
	agent, err := g.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	if g.svc.sink == nil {
		// No sink: accept and drop. Saying accepted=true is right — the agent
		// cannot fix a server with nowhere to put results, and retrying would
		// only burn its budget.
		return &pulsev1.ReportResultResponse{Accepted: true}, nil
	}
	accepted, err := g.svc.sink.Deliver(ctx, agent.ID, pulseagent.ResultFromProto(req))
	if err != nil {
		return nil, status.Error(codes.Internal, "store result failed")
	}
	g.svc.metrics.Inc(pulse.MetricResultsStored)
	return &pulsev1.ReportResultResponse{Accepted: accepted}, nil
}

// authenticate resolves the credential in the request metadata to an agent.
func (g *GRPCServer) authenticate(ctx context.Context) (pulse.Agent, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return pulse.Agent{}, status.Error(codes.Unauthenticated, "missing metadata")
	}
	values := md.Get(credentialHeader)
	if len(values) == 0 {
		return pulse.Agent{}, status.Error(codes.Unauthenticated, "missing credential")
	}
	credential := strings.TrimSpace(strings.TrimPrefix(values[0], "Bearer "))
	if credential == "" {
		return pulse.Agent{}, status.Error(codes.Unauthenticated, "missing credential")
	}
	agent, err := g.svc.VerifyCredential(ctx, credential)
	if err != nil {
		return pulse.Agent{}, mapRegistryError(err)
	}
	return agent, nil
}

// mapRegistryError translates service errors to gRPC statuses.
//
// Every authentication failure becomes Unauthenticated with a terse message:
// an agent needs to know to re-register, and nothing more. The detail stays in
// the server's log.
func mapRegistryError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, pulse.ErrInvalidBootstrap):
		return status.Error(codes.PermissionDenied, "invalid bootstrap token")
	case errors.Is(err, ErrInvalidCredential), errors.Is(err, ErrCredentialRevoked):
		return status.Error(codes.Unauthenticated, "invalid credential")
	case errors.Is(err, ErrAgentDisabled):
		// Unauthenticated rather than PermissionDenied: the agent's correct
		// response is the same as for a revoked credential — stop and wait for
		// an operator — and the two should not need different handling.
		return status.Error(codes.Unauthenticated, "agent is disabled")
	case errors.Is(err, ErrInvalidInput):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, pulse.ErrNotFound):
		return status.Error(codes.NotFound, "not found")
	default:
		return status.Error(codes.Internal, "internal error")
	}
}

// PeerIP extracts the address the connection came from, which is the only
// source of observed IP the framework trusts.
func PeerIP(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return ""
	}
	addr := p.Addr.String()
	// Strip the port; an IPv6 address without one is returned as is.
	if i := strings.LastIndex(addr, ":"); i > 0 {
		host := addr[:i]
		return strings.Trim(host, "[]")
	}
	return addr
}

func statusText(err error) string {
	if err == nil {
		return ""
	}
	return status.Code(err).String() + ": " + err.Error()
}

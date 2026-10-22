// Package pulsereg owns an agent fleet's identity: enrollment with one-time
// bootstrap tokens, the long-lived credentials that follow, heartbeat-driven
// online state, and the registry of live streams the dispatcher pushes work
// down.
//
// The security posture in one paragraph: nobody gets a credential without an
// operator having minted a bootstrap token for exactly that purpose, and no
// credential outlives the row it belongs to — revocation is a version counter
// on the agent row, checked on every authenticated call.
package pulsereg

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/seanly/pulse-devkit/pulse"
)

// Options configures a Service.
type Options struct {
	// Agents, Tokens and Configs are the storage ports. Agents and Tokens are
	// required; Configs may be nil, in which case config push is disabled and
	// agents run purely on their local configuration.
	Agents  pulse.AgentStore
	Tokens  pulse.TokenStore
	Configs pulse.ConfigStore
	// Jobs is needed only for lease renewal: an agent asks to extend a lease,
	// and the deadline lives on the job row. Nil disables renewal, which is
	// right for a deployment that uses the registry without the queue — and
	// means long jobs there are limited to one lease period.
	Jobs pulse.JobStore
	// LeaseFor is how long one renewal extends a lease. Zero means
	// DefaultLeaseFor.
	LeaseFor time.Duration
	// MasterKey is the deployment's master key; the credential signing key is
	// derived from it, so rotating it invalidates every agent credential.
	MasterKey []byte
	// Sink receives results and acks. Nil keeps the registry inert — streams
	// connect and heartbeats flow, but nothing is stored, which is a useful
	// state for verifying enrollment before any business logic exists.
	Sink pulse.Sink
	// Logger receives framework events. Nil discards them.
	Logger *slog.Logger
	// Metrics receives counters. Nil discards them.
	Metrics pulse.Metrics
	// Clock overrides time. Nil means pulse.SystemClock.
	Clock pulse.Clock
	// NewID generates agent and token IDs. Nil uses a random 128-bit hex ID.
	NewID func() string
}

// Service is the agent registry.
type Service struct {
	agents  pulse.AgentStore
	tokens  pulse.TokenStore
	configs pulse.ConfigStore
	jobs    pulse.JobStore
	// leaseFor is how long a renewal extends a lease.
	leaseFor time.Duration
	sink     pulse.Sink
	signer   *signer
	log      *slog.Logger
	metrics  pulse.Metrics
	clock    pulse.Clock
	newID    func() string

	registry streamRegistry
}

// New validates its dependencies and returns a Service.
func New(o Options) (*Service, error) {
	if o.Agents == nil {
		return nil, errors.New("pulsereg: Agents store is required")
	}
	if o.Tokens == nil {
		return nil, errors.New("pulsereg: Tokens store is required")
	}
	sg, err := newSigner(o.MasterKey)
	if err != nil {
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
	if o.NewID == nil {
		o.NewID = randomID
	}
	leaseFor := o.LeaseFor
	if leaseFor <= 0 {
		leaseFor = DefaultLeaseFor
	}
	return &Service{
		agents:   o.Agents,
		tokens:   o.Tokens,
		configs:  o.Configs,
		jobs:     o.Jobs,
		leaseFor: leaseFor,
		sink:     o.Sink,
		signer:   sg,
		log:      o.Logger,
		metrics:  o.Metrics,
		clock:    o.Clock,
		newID:    o.NewID,
	}, nil
}

func (s *Service) now() time.Time { return s.clock.Now().UTC() }

func randomID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// A system with no entropy is not one to carry on in.
		panic("pulsereg: cannot read randomness: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

// RegisterInput is an enrollment request plus the peer address the server
// observed.
type RegisterInput struct {
	BootstrapToken string
	Name           string
	Version        string
	Labels         map[string]string
	Capabilities   []string
	LocalConfig    pulse.LocalConfig
	// PeerIP is from the connection, never from the payload.
	PeerIP string
}

// RegisterResult is everything the agent needs to persist for later calls.
type RegisterResult struct {
	AgentID         string
	Credential      string
	EffectiveLabels map[string]string
}

// Register exchanges a bootstrap token for a credential.
//
// The same name re-registers the same agent row: an agent that loses its
// credential file (or its host) and re-enrolls with a fresh token must not
// become a second row, or its history splits in two.
func (s *Service) Register(ctx context.Context, in RegisterInput) (RegisterResult, error) {
	name := NormaliseAgentName(in.Name)
	if name == "" {
		return RegisterResult{}, fmt.Errorf("%w: name is required and must be 1-64 chars of [A-Za-z0-9._-]", ErrInvalidInput)
	}
	if in.BootstrapToken == "" {
		return RegisterResult{}, fmt.Errorf("%w: bootstrap token is required", ErrInvalidInput)
	}
	if err := validateLabels(in.Labels); err != nil {
		return RegisterResult{}, err
	}

	existing, err := s.agents.GetAgentByName(ctx, name)
	if err != nil && !errors.Is(err, pulse.ErrNotFound) {
		return RegisterResult{}, err
	}
	if existing.ID != "" && existing.Status == pulse.AgentDisabled {
		// An operator disabled this agent; re-enrollment must not quietly undo
		// that decision. Enable it from the console first.
		return RegisterResult{}, fmt.Errorf("%w: %s", ErrAgentDisabled, name)
	}

	agentID := existing.ID
	if agentID == "" {
		agentID = s.newID()
	}

	agent := pulse.Agent{
		ID:   agentID,
		Name: name,
		// Reported labels are replaced, not merged: what the agent sends is
		// its whole current claim, so a key it dropped from its configuration
		// must disappear here too. This can be a clean overwrite precisely
		// because operator intent lives in LabelOverrides, which is a
		// different column and cannot be lost by it.
		ReportedLabels: copyLabels(in.Labels),
		LabelOverrides: existing.LabelOverrides,
		Capabilities:   in.Capabilities,
		Version:        strings.TrimSpace(in.Version),
		// The Register call is one RPC: online begins at Connect.
		Status:      pulse.AgentOffline,
		ObservedIP:  in.PeerIP,
		Description: existing.Description,
		// Display only: a console shows these beside the override boxes so
		// "use the agent's local value" names a number. Nothing reads them.
		LocalConfig:     in.LocalConfig,
		TokenVersion:    existing.TokenVersion,
		RegisteredAt:    existing.RegisteredAt,
		LastHeartbeatAt: existing.LastHeartbeatAt,
	}
	if agent.RegisteredAt.IsZero() {
		agent.RegisteredAt = s.now()
	}
	if agent.TokenVersion == 0 {
		agent.TokenVersion = 1
	}

	// Enroll writes the agent row and claims the token atomically: a bad token
	// must leave nothing behind, not half an agent row.
	//
	// It also merges the token's preset labels into the agent and updates it in
	// place, so the effective set reported back is the one that was stored
	// rather than a second computation that could disagree with the row.
	if _, err := s.tokens.Enroll(ctx, &agent, HashToken(in.BootstrapToken)); err != nil {
		if errors.Is(err, pulse.ErrInvalidBootstrap) {
			s.log.Warn("enrollment refused: invalid bootstrap token", "name", name, "remote", in.PeerIP)
		}
		return RegisterResult{}, err
	}
	effective := agent.Labels

	credential, err := s.signer.sign(agent.ID, agent.TokenVersion, s.now())
	if err != nil {
		return RegisterResult{}, err
	}
	s.log.Info("agent registered",
		"agent_id", agent.ID, "name", name, "version", agent.Version, "remote", in.PeerIP)
	return RegisterResult{
		AgentID:         agent.ID,
		Credential:      credential,
		EffectiveLabels: effective,
	}, nil
}

// VerifyCredential resolves a presented credential to its agent, enforcing
// revocation and the disabled flag.
//
// A valid signature is not authorisation: a signed credential from before an
// operator revoked the agent must fail just as hard as a forged one, which is
// what the token version comparison buys.
func (s *Service) VerifyCredential(ctx context.Context, credential string) (pulse.Agent, error) {
	c, err := s.signer.verify(credential)
	if err != nil {
		return pulse.Agent{}, ErrInvalidCredential
	}
	agent, err := s.agents.GetAgent(ctx, c.AgentID)
	if errors.Is(err, pulse.ErrNotFound) {
		// The agent row was deleted; its credential must not linger on.
		return pulse.Agent{}, ErrInvalidCredential
	}
	if err != nil {
		return pulse.Agent{}, err
	}
	if agent.Status == pulse.AgentDisabled {
		return agent, ErrAgentDisabled
	}
	if agent.TokenVersion != c.TokenVersion {
		return agent, ErrCredentialRevoked
	}
	return agent, nil
}

// RevokeAgent invalidates every credential issued for an agent. The agent
// re-registers if it still has a bootstrap token, and otherwise stops.
func (s *Service) RevokeAgent(ctx context.Context, agentID string) error {
	v, err := s.agents.BumpTokenVersion(ctx, agentID)
	if err != nil {
		return err
	}
	s.log.Info("agent credentials revoked", "agent_id", agentID, "token_version", v)
	return nil
}

// NormaliseAgentName keeps names usable in logs and URLs without dragging in a
// full identifier policy: visible ASCII letters, digits, dash, underscore and
// dot, 1..64 of them. Returns "" for a name that does not qualify.
func NormaliseAgentName(in string) string {
	name := strings.TrimSpace(in)
	if len(name) == 0 || len(name) > 64 {
		return ""
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '-' || r == '_' || r == '.':
		default:
			return ""
		}
	}
	return name
}

const (
	maxLabels        = 32
	maxLabelKeyLen   = 128
	maxLabelValueLen = 512
	maxCapabilityLen = 64
)

func validateLabels(labels map[string]string) error {
	if len(labels) > maxLabels {
		return fmt.Errorf("%w: at most %d labels", ErrInvalidInput, maxLabels)
	}
	for k, v := range labels {
		if len(k) > maxLabelKeyLen || len(v) > maxLabelValueLen {
			return fmt.Errorf("%w: label %q is too large", ErrInvalidInput, k)
		}
	}
	return nil
}

func copyLabels(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// mergeLabels layers maps, later arguments winning. Nil maps are fine.
func mergeLabels(layers ...map[string]string) map[string]string {
	out := map[string]string{}
	for _, layer := range layers {
		for k, v := range layer {
			out[k] = v
		}
	}
	return out
}

// DefaultLeaseFor is how long one renewal extends a lease when Options.LeaseFor
// is unset.
//
// Generous on purpose: a renewal is cheap and a prematurely expired lease is
// expensive — it means the same job runs twice. An agent renews every heartbeat
// interval, so several renewals have to go missing in a row before this runs
// out.
const DefaultLeaseFor = 5 * time.Minute

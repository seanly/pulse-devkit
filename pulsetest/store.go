package pulsetest

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/seanly/pulse-devkit/pulse"
	"github.com/seanly/pulse-devkit/pulsereg"
)

// Store is an in-memory implementation of all four storage ports.
//
// It is held to the same contract as a SQL store — the shared port test suite
// runs against both — so a test that passes here is testing real semantics,
// not a simplification.
type Store struct {
	mu     sync.Mutex
	clock  pulse.Clock
	agents map[string]*pulse.Agent
	tokens map[string]*pulse.BootstrapToken
	cfgs   map[string]*pulse.AgentConfig
	jobs   map[string]*pulse.Job
	// order preserves insertion order for jobs, so leasing is FIFO like a
	// real queue's "ORDER BY created_at, id".
	order []string

	// failNext makes the next call to any method return this error, for
	// testing error paths.
	failNext error
	// onLease, when set, runs at the start of Lease. Installing a hook that
	// leases the same job from "another process" is how the lease race is
	// tested.
	onLease func()
}

// StoreOption configures NewStore.
type StoreOption func(*Store)

// WithClock makes the store read time from c, so lease expiry can be tested
// without sleeping.
func WithClock(c pulse.Clock) StoreOption {
	return func(s *Store) { s.clock = c }
}

// NewStore returns an empty in-memory store.
func NewStore(opts ...StoreOption) *Store {
	s := &Store{
		clock:  pulse.SystemClock,
		agents: map[string]*pulse.Agent{},
		tokens: map[string]*pulse.BootstrapToken{},
		cfgs:   map[string]*pulse.AgentConfig{},
		jobs:   map[string]*pulse.Job{},
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// FailNext makes the next store call return err.
func (s *Store) FailNext(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failNext = err
}

// OnLease installs a hook called at the start of Lease, for injecting a
// competing claim.
func (s *Store) OnLease(fn func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onLease = fn
}

// taken must be called with the lock held.
func (s *Store) taken() error {
	if err := s.failNext; err != nil {
		s.failNext = nil
		return err
	}
	return nil
}

func (s *Store) now() time.Time { return s.clock.Now().UTC() }

// --- AgentStore ---

func (s *Store) GetAgent(_ context.Context, id string) (pulse.Agent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return pulse.Agent{}, err
	}
	a, ok := s.agents[id]
	if !ok {
		return pulse.Agent{}, fmt.Errorf("agent %s: %w", id, pulse.ErrNotFound)
	}
	return *a, nil
}

func (s *Store) GetAgentByName(_ context.Context, name string) (pulse.Agent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return pulse.Agent{}, err
	}
	for _, a := range s.agents {
		if a.Name == name {
			return *a, nil
		}
	}
	return pulse.Agent{}, fmt.Errorf("agent %q: %w", name, pulse.ErrNotFound)
}

func (s *Store) ListAgents(_ context.Context) ([]pulse.Agent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return nil, err
	}
	return s.listAgentsLocked(func(*pulse.Agent) bool { return true }), nil
}

func (s *Store) ListDispatchableAgents(_ context.Context) ([]pulse.Agent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return nil, err
	}
	return s.listAgentsLocked(func(a *pulse.Agent) bool {
		return a.Status == pulse.AgentOnline
	}), nil
}

func (s *Store) listAgentsLocked(keep func(*pulse.Agent) bool) []pulse.Agent {
	out := make([]pulse.Agent, 0, len(s.agents))
	for _, a := range s.agents {
		if keep(a) {
			out = append(out, *a)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (s *Store) UpdateAgentMeta(_ context.Context, a *pulse.Agent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return err
	}
	if _, ok := s.agents[a.ID]; !ok {
		return fmt.Errorf("agent %s: %w", a.ID, pulse.ErrNotFound)
	}
	stored := *a
	// Derived on write, exactly as a SQL store must: label matching is on the
	// dispatch hot path and is not recomputed per read.
	stored.Labels = pulse.EffectiveLabels(stored.ReportedLabels, stored.LabelOverrides)
	stored.UpdatedAt = s.now()
	s.agents[a.ID] = &stored
	return nil
}

func (s *Store) SetLabelOverrides(_ context.Context, agentID string, overrides map[string]*string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return err
	}
	a, ok := s.agents[agentID]
	if !ok {
		return fmt.Errorf("agent %s: %w", agentID, pulse.ErrNotFound)
	}
	a.LabelOverrides = overrides
	a.Labels = pulse.EffectiveLabels(a.ReportedLabels, overrides)
	a.UpdatedAt = s.now()
	return nil
}

func (s *Store) BumpTokenVersion(_ context.Context, agentID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return 0, err
	}
	a, ok := s.agents[agentID]
	if !ok {
		return 0, fmt.Errorf("agent %s: %w", agentID, pulse.ErrNotFound)
	}
	a.TokenVersion++
	a.UpdatedAt = s.now()
	return a.TokenVersion, nil
}

func (s *Store) SetAgentStatus(_ context.Context, agentID, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return err
	}
	a, ok := s.agents[agentID]
	if !ok {
		return fmt.Errorf("agent %s: %w", agentID, pulse.ErrNotFound)
	}
	a.Status = status
	a.UpdatedAt = s.now()
	return nil
}

func (s *Store) Heartbeat(_ context.Context, agentID, version, peerIP string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return err
	}
	a, ok := s.agents[agentID]
	if !ok {
		return fmt.Errorf("agent %s: %w", agentID, pulse.ErrNotFound)
	}
	a.LastHeartbeatAt = s.now()
	if version != "" {
		a.Version = version
	}
	if peerIP != "" {
		a.ObservedIP = peerIP
	}
	// A heartbeat from a stale agent brings it back online. A disabled agent
	// stays disabled: that is an operator's decision, not a liveness fact.
	if a.Status == pulse.AgentOffline {
		a.Status = pulse.AgentOnline
	}
	a.UpdatedAt = s.now()
	return nil
}

func (s *Store) MarkStaleAgentsOffline(_ context.Context, staleFor time.Duration) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return nil, err
	}
	cutoff := s.now().Add(-staleFor)
	var ids []string
	for id, a := range s.agents {
		if a.Status != pulse.AgentOnline {
			continue
		}
		if a.LastHeartbeatAt.Before(cutoff) {
			a.Status = pulse.AgentOffline
			a.UpdatedAt = s.now()
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids, nil
}

func (s *Store) DeleteAgent(_ context.Context, agentID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return err
	}
	if _, ok := s.agents[agentID]; !ok {
		return fmt.Errorf("agent %s: %w", agentID, pulse.ErrNotFound)
	}
	delete(s.agents, agentID)
	delete(s.cfgs, agentID)
	return nil
}

// --- TokenStore ---

func (s *Store) CreateBootstrapToken(_ context.Context, t *pulse.BootstrapToken) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return err
	}
	stored := *t
	s.tokens[t.ID] = &stored
	return nil
}

func (s *Store) ListBootstrapTokens(_ context.Context) ([]pulse.BootstrapToken, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return nil, err
	}
	out := make([]pulse.BootstrapToken, 0, len(s.tokens))
	for _, t := range s.tokens {
		out = append(out, *t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

func (s *Store) DeleteBootstrapToken(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return err
	}
	if _, ok := s.tokens[id]; !ok {
		return fmt.Errorf("token %s: %w", id, pulse.ErrNotFound)
	}
	delete(s.tokens, id)
	return nil
}

// Enroll writes the agent and claims the token under one lock, which is this
// implementation's equivalent of the SQL version's transaction: an invalid
// token must leave nothing behind.
func (s *Store) Enroll(_ context.Context, a *pulse.Agent, tokenHash string) (pulse.BootstrapToken, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return pulse.BootstrapToken{}, err
	}

	var found *pulse.BootstrapToken
	for _, t := range s.tokens {
		if t.TokenHash == tokenHash {
			found = t
			break
		}
	}
	// Unknown, used and expired are one error on purpose: an attacker probing
	// tokens learns nothing about which case they hit.
	if found == nil {
		return pulse.BootstrapToken{}, fmt.Errorf("enroll: %w", pulse.ErrInvalidBootstrap)
	}
	if err := pulsereg.TokenUsable(*found, s.now()); err != nil {
		return pulse.BootstrapToken{}, fmt.Errorf("enroll: %w", err)
	}

	// The token's presets join the reported set and win over it: an operator
	// minting a token for a purpose outranks the agent's claim about itself.
	// Merged here, inside the same critical section as the claim, because the
	// presets are not known until the token is claimed.
	if len(found.Labels) > 0 {
		merged := make(map[string]string, len(a.ReportedLabels)+len(found.Labels))
		for k, v := range a.ReportedLabels {
			merged[k] = v
		}
		for k, v := range found.Labels {
			merged[k] = v
		}
		a.ReportedLabels = merged
	}
	a.Labels = pulse.EffectiveLabels(a.ReportedLabels, a.LabelOverrides)
	a.UpdatedAt = s.now()
	if a.RegisteredAt.IsZero() {
		a.RegisteredAt = s.now()
	}
	stored := *a
	s.agents[stored.ID] = &stored

	found.UsedAt = s.now()
	found.UsedBy = stored.ID
	return *found, nil
}

// --- ConfigStore ---

func (s *Store) GetAgentConfig(_ context.Context, agentID string) (pulse.AgentConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return pulse.AgentConfig{}, err
	}
	c, ok := s.cfgs[agentID]
	if !ok {
		return pulse.AgentConfig{}, fmt.Errorf("config for %s: %w", agentID, pulse.ErrNotFound)
	}
	return *c, nil
}

func (s *Store) ListAgentConfigs(_ context.Context) (map[string]pulse.AgentConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return nil, err
	}
	out := make(map[string]pulse.AgentConfig, len(s.cfgs))
	for k, v := range s.cfgs {
		out[k] = *v
	}
	return out, nil
}

func (s *Store) SaveAgentConfig(_ context.Context, cfg *pulse.AgentConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return err
	}
	stored := *cfg
	if prev, ok := s.cfgs[cfg.AgentID]; ok {
		stored.Version = prev.Version + 1
	} else {
		stored.Version = 1
	}
	stored.UpdatedAt = s.now()
	s.cfgs[cfg.AgentID] = &stored
	// Reflect the assigned version back, as a SQL RETURNING would.
	cfg.Version = stored.Version
	cfg.UpdatedAt = stored.UpdatedAt
	return nil
}

func (s *Store) DeleteAgentConfig(_ context.Context, agentID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taken(); err != nil {
		return err
	}
	if _, ok := s.cfgs[agentID]; !ok {
		return fmt.Errorf("config for %s: %w", agentID, pulse.ErrNotFound)
	}
	delete(s.cfgs, agentID)
	return nil
}

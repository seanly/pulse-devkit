package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/seanly/pulse-devkit/pulse"
)

// agentColumns is the select list, in the order scanAgent expects.
func (s *Store) agentColumns() string {
	return `id, name, labels, reported_labels, label_overrides, capabilities,
	        version, status, description, observed_ip, token_version, local_config,
	        last_heartbeat_at, registered_at, updated_at`
}

func scanAgent(sc interface{ Scan(...any) error }) (pulse.Agent, error) {
	var (
		a                                           pulse.Agent
		labels, reported, overrides, caps, localCfg []byte
		heartbeat                                   sql.NullTime
	)
	err := sc.Scan(&a.ID, &a.Name, &labels, &reported, &overrides, &caps,
		&a.Version, &a.Status, &a.Description, &a.ObservedIP, &a.TokenVersion, &localCfg,
		&heartbeat, &a.RegisteredAt, &a.UpdatedAt)
	if err != nil {
		return a, err
	}
	a.LastHeartbeatAt = timeFrom(heartbeat)
	a.RegisteredAt = a.RegisteredAt.UTC()
	a.UpdatedAt = a.UpdatedAt.UTC()
	if err := unmarshalJSON(labels, &a.Labels); err != nil {
		return a, err
	}
	if err := unmarshalJSON(reported, &a.ReportedLabels); err != nil {
		return a, err
	}
	if err := unmarshalJSON(overrides, &a.LabelOverrides); err != nil {
		return a, err
	}
	if err := unmarshalJSON(caps, &a.Capabilities); err != nil {
		return a, err
	}
	if err := unmarshalJSON(localCfg, &a.LocalConfig); err != nil {
		return a, err
	}
	return a, nil
}

func unmarshalJSON(raw []byte, dst any) error {
	if len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("sqlstore: decode stored json: %w", err)
	}
	return nil
}

func marshalJSON(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("sqlstore: encode json: %w", err)
	}
	return raw, nil
}

// GetAgent returns one agent by ID.
func (s *Store) GetAgent(ctx context.Context, id string) (pulse.Agent, error) {
	a, err := scanAgent(s.queryRow(ctx,
		`SELECT `+s.agentColumns()+` FROM `+s.table("agents")+` WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return a, fmt.Errorf("sqlstore: agent %s: %w", id, pulse.ErrNotFound)
	}
	if err != nil {
		return a, fmt.Errorf("sqlstore: get agent: %w", err)
	}
	return a, nil
}

// GetAgentByName returns one agent by name. Registration uses this to reuse a
// row rather than duplicate an identity.
func (s *Store) GetAgentByName(ctx context.Context, name string) (pulse.Agent, error) {
	a, err := scanAgent(s.queryRow(ctx,
		`SELECT `+s.agentColumns()+` FROM `+s.table("agents")+` WHERE name = ?`, name))
	if errors.Is(err, sql.ErrNoRows) {
		return a, fmt.Errorf("sqlstore: agent %q: %w", name, pulse.ErrNotFound)
	}
	if err != nil {
		return a, fmt.Errorf("sqlstore: get agent by name: %w", err)
	}
	return a, nil
}

// ListAgents returns every agent, ordered by name for a stable console view.
func (s *Store) ListAgents(ctx context.Context) ([]pulse.Agent, error) {
	rows, err := s.query(ctx,
		`SELECT `+s.agentColumns()+` FROM `+s.table("agents")+` ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("sqlstore: list agents: %w", err)
	}
	return collect(rows, scanAgent, "list agents")
}

// ListDispatchableAgents returns agents eligible for work.
func (s *Store) ListDispatchableAgents(ctx context.Context) ([]pulse.Agent, error) {
	rows, err := s.query(ctx,
		`SELECT `+s.agentColumns()+` FROM `+s.table("agents")+`
		  WHERE status = ? ORDER BY name`, pulse.AgentOnline)
	if err != nil {
		return nil, fmt.Errorf("sqlstore: list dispatchable agents: %w", err)
	}
	return collect(rows, scanAgent, "list dispatchable agents")
}

// UpdateAgentMeta writes an existing agent's mutable fields.
func (s *Store) UpdateAgentMeta(ctx context.Context, a *pulse.Agent) error {
	_, args, err := s.agentWriteArgs(a)
	if err != nil {
		return err
	}
	err = s.execOne(ctx, `
		UPDATE `+s.table("agents")+`
		   SET name = ?, labels = ?, reported_labels = ?, label_overrides = ?,
		       capabilities = ?, version = ?, status = ?, description = ?,
		       observed_ip = ?, token_version = ?, local_config = ?,
		       last_heartbeat_at = ?, registered_at = ?, updated_at = ?
		 WHERE id = ?`, append(args, a.ID)...)
	if errors.Is(err, pulse.ErrNotFound) {
		return fmt.Errorf("sqlstore: agent %s: %w", a.ID, pulse.ErrNotFound)
	}
	if err != nil {
		return fmt.Errorf("sqlstore: update agent: %w", err)
	}
	return nil
}

// agentWriteArgs renders an agent for insert or update, computing the derived
// label set so it cannot drift from its inputs.
func (s *Store) agentWriteArgs(a *pulse.Agent) (cols string, args []any, err error) {
	// Derived on write: label matching is in the dispatch hot path and is not
	// recomputed per read.
	a.Labels = pulse.EffectiveLabels(a.ReportedLabels, a.LabelOverrides)
	a.UpdatedAt = s.now()
	if a.RegisteredAt.IsZero() {
		a.RegisteredAt = s.now()
	}
	if a.Status == "" {
		a.Status = pulse.AgentOffline
	}
	if a.TokenVersion == 0 {
		a.TokenVersion = 1
	}

	labels, err := marshalJSON(a.Labels)
	if err != nil {
		return "", nil, err
	}
	reported, err := marshalJSON(orEmptyMap(a.ReportedLabels))
	if err != nil {
		return "", nil, err
	}
	overrides, err := marshalJSON(orEmptyOverrides(a.LabelOverrides))
	if err != nil {
		return "", nil, err
	}
	caps, err := marshalJSON(orEmptySlice(a.Capabilities))
	if err != nil {
		return "", nil, err
	}
	localCfg, err := marshalJSON(a.LocalConfig)
	if err != nil {
		return "", nil, err
	}
	return s.agentColumns(), []any{
		a.Name, labels, reported, overrides, caps,
		a.Version, a.Status, a.Description, a.ObservedIP, a.TokenVersion, localCfg,
		nullTime(a.LastHeartbeatAt), a.RegisteredAt, a.UpdatedAt,
	}, nil
}

// CreateAgent inserts a new agent row. Exported beyond the port because an
// application may want to pre-provision agents.
func (s *Store) CreateAgent(ctx context.Context, a *pulse.Agent) error {
	_, args, err := s.agentWriteArgs(a)
	if err != nil {
		return err
	}
	_, err = s.exec(ctx, `
		INSERT INTO `+s.table("agents")+` (
			id, name, labels, reported_labels, label_overrides, capabilities,
			version, status, description, observed_ip, token_version, local_config,
			last_heartbeat_at, registered_at, updated_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		append([]any{a.ID}, args...)...)
	if err != nil {
		return fmt.Errorf("sqlstore: create agent: %w", err)
	}
	return nil
}

// SetLabelOverrides replaces the server-side label opinion and recomputes the
// effective set.
func (s *Store) SetLabelOverrides(ctx context.Context, agentID string, overrides map[string]*string) error {
	return s.tx(ctx, func(t *sql.Tx) error {
		var reported []byte
		err := t.QueryRowContext(ctx,
			s.rebind(`SELECT reported_labels FROM `+s.table("agents")+` WHERE id = ?`),
			agentID).Scan(&reported)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("sqlstore: agent %s: %w", agentID, pulse.ErrNotFound)
		}
		if err != nil {
			return err
		}
		var reportedMap map[string]string
		if err := unmarshalJSON(reported, &reportedMap); err != nil {
			return err
		}

		rawOverrides, err := marshalJSON(orEmptyOverrides(overrides))
		if err != nil {
			return err
		}
		labels, err := marshalJSON(pulse.EffectiveLabels(reportedMap, overrides))
		if err != nil {
			return err
		}
		_, err = t.ExecContext(ctx, s.rebind(`
			UPDATE `+s.table("agents")+`
			   SET label_overrides = ?, labels = ?, updated_at = ?
			 WHERE id = ?`), rawOverrides, labels, s.now(), agentID)
		return err
	})
}

// BumpTokenVersion revokes every credential issued for an agent.
func (s *Store) BumpTokenVersion(ctx context.Context, agentID string) (int, error) {
	var version int
	err := s.tx(ctx, func(t *sql.Tx) error {
		_, err := t.ExecContext(ctx, s.rebind(`
			UPDATE `+s.table("agents")+`
			   SET token_version = token_version + 1, updated_at = ?
			 WHERE id = ?`), s.now(), agentID)
		if err != nil {
			return err
		}
		err = t.QueryRowContext(ctx,
			s.rebind(`SELECT token_version FROM `+s.table("agents")+` WHERE id = ?`),
			agentID).Scan(&version)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("sqlstore: agent %s: %w", agentID, pulse.ErrNotFound)
		}
		return err
	})
	return version, err
}

// SetAgentStatus moves an agent between online, offline and disabled.
func (s *Store) SetAgentStatus(ctx context.Context, agentID, status string) error {
	err := s.execOne(ctx, `
		UPDATE `+s.table("agents")+` SET status = ?, updated_at = ? WHERE id = ?`,
		status, s.now(), agentID)
	if errors.Is(err, pulse.ErrNotFound) {
		return fmt.Errorf("sqlstore: agent %s: %w", agentID, pulse.ErrNotFound)
	}
	if err != nil {
		return fmt.Errorf("sqlstore: set agent status: %w", err)
	}
	return nil
}

// Heartbeat records liveness.
//
// A heartbeat from a stale agent brings it back online, but a disabled agent
// stays disabled: that is an operator's decision, not a liveness fact, and a
// heartbeat must not be able to overturn it.
func (s *Store) Heartbeat(ctx context.Context, agentID, version, peerIP string) error {
	now := s.now()
	err := s.execOne(ctx, `
		UPDATE `+s.table("agents")+`
		   SET last_heartbeat_at = ?,
		       version = CASE WHEN ? <> '' THEN ? ELSE version END,
		       observed_ip = CASE WHEN ? <> '' THEN ? ELSE observed_ip END,
		       status = CASE WHEN status = ? THEN ? ELSE status END,
		       updated_at = ?
		 WHERE id = ?`,
		now, version, version, peerIP, peerIP,
		pulse.AgentOffline, pulse.AgentOnline, now, agentID)
	if errors.Is(err, pulse.ErrNotFound) {
		return fmt.Errorf("sqlstore: agent %s: %w", agentID, pulse.ErrNotFound)
	}
	if err != nil {
		return fmt.Errorf("sqlstore: heartbeat: %w", err)
	}
	return nil
}

// MarkStaleAgentsOffline flips agents whose heartbeat went stale and returns
// their IDs.
//
// It deliberately does not touch their jobs: a lease has its own expiry, and a
// brief network blip should not requeue work that is still running fine.
func (s *Store) MarkStaleAgentsOffline(ctx context.Context, staleFor time.Duration) ([]string, error) {
	cutoff := s.now().Add(-staleFor)
	// Read then write, in a transaction: RETURNING would be neater but is not
	// available on every supported SQLite build, and the two dialects should
	// not diverge here for cosmetic reasons.
	var ids []string
	err := s.tx(ctx, func(t *sql.Tx) error {
		rows, err := t.QueryContext(ctx, s.rebind(`
			SELECT id FROM `+s.table("agents")+`
			 WHERE status = ? AND (last_heartbeat_at IS NULL OR last_heartbeat_at < ?)`),
			pulse.AgentOnline, cutoff)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		list, args := placeholders(ids)
		args = append([]any{pulse.AgentOffline, s.now()}, args...)
		_, err = t.ExecContext(ctx, s.rebind(`
			UPDATE `+s.table("agents")+` SET status = ?, updated_at = ?
			 WHERE id IN (`+list+`)`), args...)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("sqlstore: mark stale agents offline: %w", err)
	}
	return ids, nil
}

// DeleteAgent removes an agent. Its credentials stop verifying immediately,
// which falls out of GetAgent then reporting ErrNotFound.
func (s *Store) DeleteAgent(ctx context.Context, agentID string) error {
	err := s.execOne(ctx, `DELETE FROM `+s.table("agents")+` WHERE id = ?`, agentID)
	if errors.Is(err, pulse.ErrNotFound) {
		return fmt.Errorf("sqlstore: agent %s: %w", agentID, pulse.ErrNotFound)
	}
	if err != nil {
		return fmt.Errorf("sqlstore: delete agent: %w", err)
	}
	return nil
}

// collect drains rows through a scan function.
func collect[T any](rows *sql.Rows, scan func(interface{ Scan(...any) error }) (T, error), what string) ([]T, error) {
	defer rows.Close()
	var out []T
	for rows.Next() {
		v, err := scan(rows)
		if err != nil {
			return nil, fmt.Errorf("sqlstore: %s: %w", what, err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlstore: %s: %w", what, err)
	}
	return out, nil
}

func orEmptyMap(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

func orEmptyOverrides(m map[string]*string) map[string]*string {
	if m == nil {
		return map[string]*string{}
	}
	return m
}

func orEmptySlice(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

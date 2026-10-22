package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/seanly/pulse-devkit/pulse"
)

func (s *Store) configColumns() string {
	return `agent_id, concurrency, qps, timeout_ms, log_level, payload, version,
	        updated_at, updated_by`
}

func scanConfig(sc interface{ Scan(...any) error }) (pulse.AgentConfig, error) {
	var (
		c                           pulse.AgentConfig
		concurrency, qps, timeoutMS sql.NullInt64
		logLevel                    sql.NullString
	)
	if err := sc.Scan(&c.AgentID, &concurrency, &qps, &timeoutMS, &logLevel,
		&c.Payload, &c.Version, &c.UpdatedAt, &c.UpdatedBy); err != nil {
		return c, err
	}
	c.UpdatedAt = c.UpdatedAt.UTC()
	// NULL means "no opinion", which is why these are pointers: a stored zero
	// and an absent value are different instructions to the agent.
	if concurrency.Valid {
		v := int(concurrency.Int64)
		c.Concurrency = &v
	}
	if qps.Valid {
		v := int(qps.Int64)
		c.QPS = &v
	}
	if timeoutMS.Valid {
		d := time.Duration(timeoutMS.Int64) * time.Millisecond
		c.Timeout = &d
	}
	if logLevel.Valid {
		v := logLevel.String
		c.LogLevel = &v
	}
	return c, nil
}

// GetAgentConfig returns the override for one agent.
//
// ErrNotFound means the agent has no override, which is a normal state: its
// local configuration governs.
func (s *Store) GetAgentConfig(ctx context.Context, agentID string) (pulse.AgentConfig, error) {
	c, err := scanConfig(s.queryRow(ctx,
		`SELECT `+s.configColumns()+` FROM `+s.table("agent_configs")+` WHERE agent_id = ?`,
		agentID))
	if errors.Is(err, sql.ErrNoRows) {
		return c, fmt.Errorf("sqlstore: config for %s: %w", agentID, pulse.ErrNotFound)
	}
	if err != nil {
		return c, fmt.Errorf("sqlstore: get agent config: %w", err)
	}
	return c, nil
}

// ListAgentConfigs returns every override keyed by agent ID, so a dispatch
// pass reads them once rather than per agent.
func (s *Store) ListAgentConfigs(ctx context.Context) (map[string]pulse.AgentConfig, error) {
	rows, err := s.query(ctx,
		`SELECT `+s.configColumns()+` FROM `+s.table("agent_configs"))
	if err != nil {
		return nil, fmt.Errorf("sqlstore: list agent configs: %w", err)
	}
	list, err := collect(rows, scanConfig, "list agent configs")
	if err != nil {
		return nil, err
	}
	out := make(map[string]pulse.AgentConfig, len(list))
	for _, c := range list {
		out[c.AgentID] = c
	}
	return out, nil
}

// SaveAgentConfig upserts an override and advances its version.
//
// The version is assigned here rather than by the caller so it is monotonic
// per agent even when two operators save at once, and it is written back to
// cfg so the caller can report what the agent will echo.
func (s *Store) SaveAgentConfig(ctx context.Context, cfg *pulse.AgentConfig) error {
	now := s.now()
	err := s.tx(ctx, func(t *sql.Tx) error {
		var version int
		err := t.QueryRowContext(ctx,
			s.rebind(`SELECT version FROM `+s.table("agent_configs")+` WHERE agent_id = ?`),
			cfg.AgentID).Scan(&version)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			version = 1
			_, err = t.ExecContext(ctx, s.rebind(`
				INSERT INTO `+s.table("agent_configs")+`
					(agent_id, concurrency, qps, timeout_ms, log_level, payload,
					 version, updated_at, updated_by)
				VALUES (?,?,?,?,?,?,?,?,?)`),
				cfg.AgentID, intOrNull(cfg.Concurrency), intOrNull(cfg.QPS),
				durationMSOrNull(cfg.Timeout), strOrNull(cfg.LogLevel), cfg.Payload,
				version, now, cfg.UpdatedBy)
			if err != nil {
				return err
			}
		case err != nil:
			return err
		default:
			version++
			_, err = t.ExecContext(ctx, s.rebind(`
				UPDATE `+s.table("agent_configs")+`
				   SET concurrency = ?, qps = ?, timeout_ms = ?, log_level = ?,
				       payload = ?, version = ?, updated_at = ?, updated_by = ?
				 WHERE agent_id = ?`),
				intOrNull(cfg.Concurrency), intOrNull(cfg.QPS),
				durationMSOrNull(cfg.Timeout), strOrNull(cfg.LogLevel), cfg.Payload,
				version, now, cfg.UpdatedBy, cfg.AgentID)
			if err != nil {
				return err
			}
		}
		cfg.Version = version
		cfg.UpdatedAt = now
		return nil
	})
	if err != nil {
		return fmt.Errorf("sqlstore: save agent config: %w", err)
	}
	return nil
}

// DeleteAgentConfig removes an override entirely, so the next push lets the
// agent fall back to its local configuration.
func (s *Store) DeleteAgentConfig(ctx context.Context, agentID string) error {
	err := s.execOne(ctx, `DELETE FROM `+s.table("agent_configs")+` WHERE agent_id = ?`, agentID)
	if errors.Is(err, pulse.ErrNotFound) {
		return fmt.Errorf("sqlstore: config for %s: %w", agentID, pulse.ErrNotFound)
	}
	if err != nil {
		return fmt.Errorf("sqlstore: delete agent config: %w", err)
	}
	return nil
}

func intOrNull(v *int) any {
	if v == nil {
		return nil
	}
	return *v
}

func strOrNull(v *string) any {
	if v == nil {
		return nil
	}
	return *v
}

func durationMSOrNull(v *time.Duration) any {
	if v == nil {
		return nil
	}
	return v.Milliseconds()
}

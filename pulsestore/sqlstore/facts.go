package sqlstore

import (
	"context"
	"database/sql"
)

// SaveFacts replaces the facts recorded for an agent. The bytes are opaque
// to the store; the agent decided what they mean.
func (s *Store) SaveFacts(ctx context.Context, agentID string, raw []byte) error {
	if len(raw) == 0 {
		raw = []byte("{}")
	}
	q := `INSERT INTO ` + s.table("agent_facts") + ` (agent_id, facts, updated_at) VALUES (?, ?, ?)
		ON CONFLICT (agent_id) DO UPDATE SET facts = excluded.facts, updated_at = excluded.updated_at`
	_, err := s.exec(ctx, q, agentID, raw, s.now())
	return err
}

// GetFacts returns the last facts saved for an agent. A missing row is an
// empty document, not an error: an agent that has not reported facts yet
// is not a failure.
func (s *Store) GetFacts(ctx context.Context, agentID string) ([]byte, error) {
	var raw []byte
	err := s.db.QueryRowContext(ctx, s.rebind(
		`SELECT facts FROM `+s.table("agent_facts")+` WHERE agent_id = ?`), agentID).Scan(&raw)
	if err == sql.ErrNoRows {
		return []byte("{}"), nil
	}
	if err != nil {
		return nil, err
	}
	return raw, nil
}

// ListFacts returns every agent's facts, keyed by agent id.
func (s *Store) ListFacts(ctx context.Context) (map[string][]byte, error) {
	rows, err := s.query(ctx, `SELECT agent_id, facts FROM `+s.table("agent_facts"))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]byte{}
	for rows.Next() {
		var id string
		var raw []byte
		if err := rows.Scan(&id, &raw); err != nil {
			return nil, err
		}
		out[id] = append([]byte(nil), raw...)
	}
	return out, rows.Err()
}

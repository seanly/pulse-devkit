package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/seanly/pulse-devkit/pulse"
	"github.com/seanly/pulse-devkit/pulsereg"
)

func (s *Store) tokenColumns() string {
	return `id, token_hash, labels, description, expires_at, used_at, used_by,
	        created_at, created_by`
}

func scanToken(sc interface{ Scan(...any) error }) (pulse.BootstrapToken, error) {
	var (
		t             pulse.BootstrapToken
		labels        []byte
		expires, used sql.NullTime
		usedBy        sql.NullString
	)
	if err := sc.Scan(&t.ID, &t.TokenHash, &labels, &t.Description,
		&expires, &used, &usedBy, &t.CreatedAt, &t.CreatedBy); err != nil {
		return t, err
	}
	t.ExpiresAt = timeFrom(expires)
	t.UsedAt = timeFrom(used)
	t.UsedBy = stringFrom(usedBy)
	t.CreatedAt = t.CreatedAt.UTC()
	if err := unmarshalJSON(labels, &t.Labels); err != nil {
		return t, err
	}
	return t, nil
}

// CreateBootstrapToken stores a token by hash. The plaintext is never
// persisted — see pulsereg.NewBootstrapToken.
func (s *Store) CreateBootstrapToken(ctx context.Context, t *pulse.BootstrapToken) error {
	labels, err := marshalJSON(orEmptyMap(t.Labels))
	if err != nil {
		return err
	}
	if t.CreatedAt.IsZero() {
		t.CreatedAt = s.now()
	}
	_, err = s.exec(ctx, `
		INSERT INTO `+s.table("bootstrap_tokens")+`
			(id, token_hash, labels, description, expires_at, used_at, used_by,
			 created_at, created_by)
		VALUES (?,?,?,?,?,?,?,?,?)`,
		t.ID, t.TokenHash, labels, t.Description,
		nullTime(t.ExpiresAt), nullTime(t.UsedAt), nullString(t.UsedBy),
		t.CreatedAt, t.CreatedBy)
	if err != nil {
		return fmt.Errorf("sqlstore: create bootstrap token: %w", err)
	}
	return nil
}

// ListBootstrapTokens returns tokens for a console listing.
func (s *Store) ListBootstrapTokens(ctx context.Context) ([]pulse.BootstrapToken, error) {
	rows, err := s.query(ctx,
		`SELECT `+s.tokenColumns()+` FROM `+s.table("bootstrap_tokens")+` ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("sqlstore: list bootstrap tokens: %w", err)
	}
	return collect(rows, scanToken, "list bootstrap tokens")
}

// DeleteBootstrapToken removes a token.
func (s *Store) DeleteBootstrapToken(ctx context.Context, id string) error {
	err := s.execOne(ctx, `DELETE FROM `+s.table("bootstrap_tokens")+` WHERE id = ?`, id)
	if errors.Is(err, pulse.ErrNotFound) {
		return fmt.Errorf("sqlstore: token %s: %w", id, pulse.ErrNotFound)
	}
	if err != nil {
		return fmt.Errorf("sqlstore: delete bootstrap token: %w", err)
	}
	return nil
}

// Enroll writes the agent row and claims the token in one transaction.
//
// Both in one transaction because the claim records which agent used it, so
// the agent row must exist first, and because an invalid token must leave
// nothing behind — not half an agent row that a later registration would then
// find and reuse.
func (s *Store) Enroll(ctx context.Context, a *pulse.Agent, tokenHash string) (pulse.BootstrapToken, error) {
	var token pulse.BootstrapToken
	err := s.tx(ctx, func(t *sql.Tx) error {
		// Claim the token first, so its preset labels are known before the
		// agent row is written. Doing it the other way round would need a
		// second update to apply them, leaving a window where the row exists
		// with the wrong labels.
		claimed, err := s.claimTokenTx(ctx, t, tokenHash, a.ID)
		if err != nil {
			return err
		}
		token = claimed

		// The presets join the reported set and win over it: an operator
		// minting a token for a purpose outranks the agent's claim about
		// itself. Server-side overrides still beat both, which
		// agentWriteArgs applies when it derives the effective set.
		if len(claimed.Labels) > 0 {
			merged := make(map[string]string, len(a.ReportedLabels)+len(claimed.Labels))
			for k, v := range a.ReportedLabels {
				merged[k] = v
			}
			for k, v := range claimed.Labels {
				merged[k] = v
			}
			a.ReportedLabels = merged
		}

		// Does the agent row already exist? Re-enrollment under the same name
		// must reuse it rather than split the agent's history in two.
		var exists string
		err = t.QueryRowContext(ctx,
			s.rebind(`SELECT id FROM `+s.table("agents")+` WHERE id = ?`), a.ID).Scan(&exists)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return s.insertAgentTx(ctx, t, a)
		case err != nil:
			return err
		default:
			return s.updateAgentTx(ctx, t, a)
		}
	})
	if err != nil {
		return pulse.BootstrapToken{}, err
	}
	return token, nil
}

// claimTokenTx marks a token used, returning ErrInvalidBootstrap for a token
// that is unknown, already used or expired.
//
// One error for all three on purpose: an attacker probing tokens must not
// learn which case they hit.
func (s *Store) claimTokenTx(ctx context.Context, t *sql.Tx, tokenHash, agentID string) (pulse.BootstrapToken, error) {
	var token pulse.BootstrapToken
	row := t.QueryRowContext(ctx,
		s.rebind(`SELECT `+s.tokenColumns()+` FROM `+s.table("bootstrap_tokens")+` WHERE token_hash = ?`),
		tokenHash)
	token, err := scanToken(row)
	if errors.Is(err, sql.ErrNoRows) {
		return token, fmt.Errorf("sqlstore: enroll: %w", pulse.ErrInvalidBootstrap)
	}
	if err != nil {
		return token, fmt.Errorf("sqlstore: load bootstrap token: %w", err)
	}
	// TokenUsable decides "used" and "expired" identically to every other
	// store, so the rule lives in one place.
	if err := pulsereg.TokenUsable(token, s.now()); err != nil {
		return token, fmt.Errorf("sqlstore: enroll: %w", err)
	}

	now := s.now()
	// used_at IS NULL in the predicate is the real single-use guard: two
	// concurrent enrollments with the same token both pass the check above,
	// and only one of them updates a row.
	res, err := t.ExecContext(ctx, s.rebind(`
		UPDATE `+s.table("bootstrap_tokens")+`
		   SET used_at = ?, used_by = ?
		 WHERE token_hash = ? AND used_at IS NULL`),
		now, agentID, tokenHash)
	if err != nil {
		return token, fmt.Errorf("sqlstore: claim bootstrap token: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return token, err
	}
	if n == 0 {
		return token, fmt.Errorf("sqlstore: enroll: %w", pulse.ErrInvalidBootstrap)
	}
	token.UsedAt = now
	token.UsedBy = agentID
	return token, nil
}

func (s *Store) insertAgentTx(ctx context.Context, t *sql.Tx, a *pulse.Agent) error {
	_, args, err := s.agentWriteArgs(a)
	if err != nil {
		return err
	}
	_, err = t.ExecContext(ctx, s.rebind(`
		INSERT INTO `+s.table("agents")+` (
			id, name, labels, reported_labels, label_overrides, capabilities,
			version, status, description, observed_ip, token_version, local_config,
			last_heartbeat_at, registered_at, updated_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`),
		append([]any{a.ID}, args...)...)
	if err != nil {
		return fmt.Errorf("sqlstore: insert agent: %w", err)
	}
	return nil
}

func (s *Store) updateAgentTx(ctx context.Context, t *sql.Tx, a *pulse.Agent) error {
	_, args, err := s.agentWriteArgs(a)
	if err != nil {
		return err
	}
	_, err = t.ExecContext(ctx, s.rebind(`
		UPDATE `+s.table("agents")+`
		   SET name = ?, labels = ?, reported_labels = ?, label_overrides = ?,
		       capabilities = ?, version = ?, status = ?, description = ?,
		       observed_ip = ?, token_version = ?, local_config = ?,
		       last_heartbeat_at = ?, registered_at = ?, updated_at = ?
		 WHERE id = ?`), append(args, a.ID)...)
	if err != nil {
		return fmt.Errorf("sqlstore: update agent: %w", err)
	}
	return nil
}

package sqlstore

import (
	"context"
	"fmt"
	"strings"
)

// Schema returns the DDL for one dialect, as statements to run in order.
//
// The framework does not choose a migration tool. An application that already
// uses goose, atlas or its own runner feeds these in; one that has none can
// call Migrate.
//
// Statements are idempotent (IF NOT EXISTS throughout), so running them on an
// up-to-date database is a no-op. That keeps "apply the schema at startup" a
// legitimate strategy for a single-node deployment.
func Schema(d Dialect, prefix string) ([]string, error) {
	if !validPrefix.MatchString(prefix) {
		return nil, fmt.Errorf("sqlstore: table prefix %q must match [a-z0-9_]*", prefix)
	}
	var tmpl []string
	switch d {
	case Postgres:
		tmpl = postgresSchema
	case SQLite:
		tmpl = sqliteSchema
	default:
		return nil, fmt.Errorf("sqlstore: unknown dialect %q", d)
	}
	out := make([]string, 0, len(tmpl))
	for _, stmt := range tmpl {
		out = append(out, strings.ReplaceAll(stmt, "{{p}}", prefix))
	}
	return out, nil
}

// Migrate applies Schema to the store's database.
//
// Convenience for applications without a migration tool; anything with real
// migration needs should take Schema and run it through its own machinery, so
// the framework's tables are versioned alongside its own.
func (s *Store) Migrate(ctx context.Context) error {
	stmts, err := Schema(s.dialect, s.prefix)
	if err != nil {
		return err
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("sqlstore: migrate: %w\nstatement: %s", err, stmt)
		}
	}
	return nil
}

// The two schemas are kept structurally identical so one set of queries works
// against both. Where the types differ it is only in spelling: TEXT vs
// VARCHAR, TIMESTAMPTZ vs DATETIME, JSONB vs TEXT.
//
// Labels and capabilities are stored as JSON documents rather than side
// tables: nothing joins on them, they are always read with their row, and the
// effective label set is computed on write anyway.

var postgresSchema = []string{
	`CREATE TABLE IF NOT EXISTS {{p}}agents (
		id                TEXT PRIMARY KEY,
		name              TEXT NOT NULL UNIQUE,
		labels            JSONB NOT NULL DEFAULT '{}',
		reported_labels   JSONB NOT NULL DEFAULT '{}',
		label_overrides   JSONB NOT NULL DEFAULT '{}',
		capabilities      JSONB NOT NULL DEFAULT '[]',
		version           TEXT NOT NULL DEFAULT '',
		status            TEXT NOT NULL DEFAULT 'offline',
		description       TEXT NOT NULL DEFAULT '',
		observed_ip       TEXT NOT NULL DEFAULT '',
		token_version     INTEGER NOT NULL DEFAULT 1,
		local_config      JSONB NOT NULL DEFAULT '{}',
		last_heartbeat_at TIMESTAMPTZ,
		registered_at     TIMESTAMPTZ NOT NULL,
		updated_at        TIMESTAMPTZ NOT NULL
	)`,
	// Dispatch scans this every pass, so the status filter is indexed.
	`CREATE INDEX IF NOT EXISTS {{p}}agents_status_idx ON {{p}}agents (status)`,
	// The reaper orders by heartbeat age.
	`CREATE INDEX IF NOT EXISTS {{p}}agents_heartbeat_idx ON {{p}}agents (last_heartbeat_at)`,

	`CREATE TABLE IF NOT EXISTS {{p}}bootstrap_tokens (
		id          TEXT PRIMARY KEY,
		token_hash  TEXT NOT NULL UNIQUE,
		labels      JSONB NOT NULL DEFAULT '{}',
		description TEXT NOT NULL DEFAULT '',
		expires_at  TIMESTAMPTZ,
		used_at     TIMESTAMPTZ,
		used_by     TEXT,
		created_at  TIMESTAMPTZ NOT NULL,
		created_by  TEXT NOT NULL DEFAULT ''
	)`,
	// Enrollment looks a token up by hash; the plaintext is never stored.
	`CREATE INDEX IF NOT EXISTS {{p}}bootstrap_tokens_hash_idx ON {{p}}bootstrap_tokens (token_hash)`,

	`CREATE TABLE IF NOT EXISTS {{p}}agent_configs (
		agent_id    TEXT PRIMARY KEY REFERENCES {{p}}agents(id) ON DELETE CASCADE,
		concurrency INTEGER,
		qps         INTEGER,
		timeout_ms  INTEGER,
		log_level   TEXT,
		payload     BYTEA,
		version     INTEGER NOT NULL DEFAULT 1,
		updated_at  TIMESTAMPTZ NOT NULL,
		updated_by  TEXT NOT NULL DEFAULT ''
	)`,

	`CREATE TABLE IF NOT EXISTS {{p}}agent_facts (
		agent_id   TEXT PRIMARY KEY REFERENCES {{p}}agents(id) ON DELETE CASCADE,
		facts      JSONB NOT NULL DEFAULT '{}',
		updated_at TIMESTAMPTZ NOT NULL
	)`,

	`CREATE TABLE IF NOT EXISTS {{p}}jobs (
		id               TEXT PRIMARY KEY,
		group_id         TEXT,
		subject_id       TEXT NOT NULL DEFAULT '',
		kind             TEXT NOT NULL DEFAULT '',
		agent_id         TEXT NOT NULL,
		status           TEXT NOT NULL DEFAULT 'pending',
		timeout_ms       INTEGER NOT NULL DEFAULT 0,
		attempt          INTEGER NOT NULL DEFAULT 0,
		max_attempts     INTEGER NOT NULL DEFAULT 1,
		requeue_count    INTEGER NOT NULL DEFAULT 0,
		last_error       TEXT NOT NULL DEFAULT '',
		payload          BYTEA,
		lease_expires_at TIMESTAMPTZ,
		created_at       TIMESTAMPTZ NOT NULL,
		finished_at      TIMESTAMPTZ
	)`,
	// The lease query's access path: pending jobs for one agent, oldest
	// first. Without this index every dispatch pass is a table scan.
	`CREATE INDEX IF NOT EXISTS {{p}}jobs_lease_idx
		ON {{p}}jobs (agent_id, status, created_at, id)`,
	// The reaper's access path: leased jobs by lease deadline.
	`CREATE INDEX IF NOT EXISTS {{p}}jobs_lease_expiry_idx
		ON {{p}}jobs (status, lease_expires_at)`,
	// Retention sweeps by terminal state and finish time.
	`CREATE INDEX IF NOT EXISTS {{p}}jobs_finished_idx ON {{p}}jobs (status, finished_at)`,
	`CREATE INDEX IF NOT EXISTS {{p}}jobs_group_idx ON {{p}}jobs (group_id)`,
}

var sqliteSchema = []string{
	`CREATE TABLE IF NOT EXISTS {{p}}agents (
		id                TEXT PRIMARY KEY,
		name              TEXT NOT NULL UNIQUE,
		labels            TEXT NOT NULL DEFAULT '{}',
		reported_labels   TEXT NOT NULL DEFAULT '{}',
		label_overrides   TEXT NOT NULL DEFAULT '{}',
		capabilities      TEXT NOT NULL DEFAULT '[]',
		version           TEXT NOT NULL DEFAULT '',
		status            TEXT NOT NULL DEFAULT 'offline',
		description       TEXT NOT NULL DEFAULT '',
		observed_ip       TEXT NOT NULL DEFAULT '',
		token_version     INTEGER NOT NULL DEFAULT 1,
		local_config      TEXT NOT NULL DEFAULT '{}',
		last_heartbeat_at DATETIME,
		registered_at     DATETIME NOT NULL,
		updated_at        DATETIME NOT NULL
	)`,
	`CREATE INDEX IF NOT EXISTS {{p}}agents_status_idx ON {{p}}agents (status)`,
	`CREATE INDEX IF NOT EXISTS {{p}}agents_heartbeat_idx ON {{p}}agents (last_heartbeat_at)`,

	`CREATE TABLE IF NOT EXISTS {{p}}bootstrap_tokens (
		id          TEXT PRIMARY KEY,
		token_hash  TEXT NOT NULL UNIQUE,
		labels      TEXT NOT NULL DEFAULT '{}',
		description TEXT NOT NULL DEFAULT '',
		expires_at  DATETIME,
		used_at     DATETIME,
		used_by     TEXT,
		created_at  DATETIME NOT NULL,
		created_by  TEXT NOT NULL DEFAULT ''
	)`,
	`CREATE INDEX IF NOT EXISTS {{p}}bootstrap_tokens_hash_idx ON {{p}}bootstrap_tokens (token_hash)`,

	`CREATE TABLE IF NOT EXISTS {{p}}agent_configs (
		agent_id    TEXT PRIMARY KEY REFERENCES {{p}}agents(id) ON DELETE CASCADE,
		concurrency INTEGER,
		qps         INTEGER,
		timeout_ms  INTEGER,
		log_level   TEXT,
		payload     BLOB,
		version     INTEGER NOT NULL DEFAULT 1,
		updated_at  DATETIME NOT NULL,
		updated_by  TEXT NOT NULL DEFAULT ''
	)`,

	`CREATE TABLE IF NOT EXISTS {{p}}agent_facts (
		agent_id   TEXT PRIMARY KEY REFERENCES {{p}}agents(id) ON DELETE CASCADE,
		facts      TEXT NOT NULL DEFAULT '{}',
		updated_at DATETIME NOT NULL
	)`,

	`CREATE TABLE IF NOT EXISTS {{p}}jobs (
		id               TEXT PRIMARY KEY,
		group_id         TEXT,
		subject_id       TEXT NOT NULL DEFAULT '',
		kind             TEXT NOT NULL DEFAULT '',
		agent_id         TEXT NOT NULL,
		status           TEXT NOT NULL DEFAULT 'pending',
		timeout_ms       INTEGER NOT NULL DEFAULT 0,
		attempt          INTEGER NOT NULL DEFAULT 0,
		max_attempts     INTEGER NOT NULL DEFAULT 1,
		requeue_count    INTEGER NOT NULL DEFAULT 0,
		last_error       TEXT NOT NULL DEFAULT '',
		payload          BLOB,
		lease_expires_at DATETIME,
		created_at       DATETIME NOT NULL,
		finished_at      DATETIME
	)`,
	`CREATE INDEX IF NOT EXISTS {{p}}jobs_lease_idx
		ON {{p}}jobs (agent_id, status, created_at, id)`,
	`CREATE INDEX IF NOT EXISTS {{p}}jobs_lease_expiry_idx
		ON {{p}}jobs (status, lease_expires_at)`,
	`CREATE INDEX IF NOT EXISTS {{p}}jobs_finished_idx ON {{p}}jobs (status, finished_at)`,
	`CREATE INDEX IF NOT EXISTS {{p}}jobs_group_idx ON {{p}}jobs (group_id)`,
}

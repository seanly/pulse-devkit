// Package sqlstore implements pulse-devkit's storage ports over database/sql,
// against PostgreSQL and SQLite with one set of hand-written queries.
//
// The framework ships this rather than only the interfaces because the lease
// query is the hardest part of the whole system to get right — one statement,
// two dialects, and a race that does not show up until production. Leaving
// that to every downstream project would mean every project rediscovering it.
//
// Queries are written with '?' placeholders and rewritten to $1..$N for
// PostgreSQL by rebind. That is the only routine dialect difference; where a
// query genuinely cannot be shared, the split is explicit and commented at the
// call site.
//
// No driver is imported here: the application registers its own. No migration
// tool either — Migrations hands out the statements and the application runs
// them with whatever it already uses.
package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/seanly/pulse-devkit/pulse"
)

// Dialect identifies the SQL flavour in use.
type Dialect string

const (
	// Postgres is the multi-instance deployment target.
	Postgres Dialect = "postgres"
	// SQLite is the single-node target: no server to run, and a pure-Go
	// driver keeps a binary CGO-free.
	SQLite Dialect = "sqlite"
)

// DefaultTablePrefix namespaces the framework's tables so they can share a
// database with an application's own.
const DefaultTablePrefix = "pulse_"

// Store implements pulse.AgentStore, TokenStore, ConfigStore and JobStore.
type Store struct {
	db      *sql.DB
	dialect Dialect
	prefix  string
	clock   pulse.Clock
}

// Option configures New.
type Option func(*Store) error

// WithTablePrefix overrides DefaultTablePrefix.
//
// The prefix is interpolated into SQL, so it is restricted to [a-z0-9_] and
// must come from code rather than configuration — a prefix read from a config
// file would be an injection point.
func WithTablePrefix(prefix string) Option {
	return func(s *Store) error {
		if !validPrefix.MatchString(prefix) {
			return fmt.Errorf("sqlstore: table prefix %q must match [a-z0-9_]*", prefix)
		}
		s.prefix = prefix
		return nil
	}
}

// WithClock overrides the clock, so lease and staleness tests need not sleep.
func WithClock(c pulse.Clock) Option {
	return func(s *Store) error {
		if c == nil {
			return errors.New("sqlstore: clock must not be nil")
		}
		s.clock = c
		return nil
	}
}

var validPrefix = regexp.MustCompile(`^[a-z0-9_]*$`)

// New wraps an open database handle.
//
// The caller owns the handle, including its lifetime and pool settings. On
// SQLite the pool should be limited to one connection for writes; the package
// documentation of the application's driver explains why.
func New(db *sql.DB, d Dialect, opts ...Option) (*Store, error) {
	if db == nil {
		return nil, errors.New("sqlstore: db is required")
	}
	switch d {
	case Postgres, SQLite:
	default:
		return nil, fmt.Errorf("sqlstore: unknown dialect %q", d)
	}
	s := &Store{db: db, dialect: d, prefix: DefaultTablePrefix, clock: pulse.SystemClock}
	for _, o := range opts {
		if err := o(s); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Dialect reports which SQL flavour this store speaks.
func (s *Store) Dialect() Dialect { return s.dialect }

// table renders a prefixed table name. The prefix is validated at
// construction, so this cannot introduce injection.
func (s *Store) table(name string) string { return s.prefix + name }

func (s *Store) now() time.Time {
	// Truncated to microseconds: PostgreSQL stores microsecond precision and
	// SQLite round-trips through a string, so an untruncated nanosecond value
	// comes back different on one of them and timestamp comparisons start
	// failing in ways that look like clock skew.
	return s.clock.Now().UTC().Truncate(time.Microsecond)
}

// rebind rewrites '?' placeholders to $1..$N for PostgreSQL.
func (s *Store) rebind(query string) string {
	if s.dialect != Postgres {
		return query
	}
	var b strings.Builder
	b.Grow(len(query) + 8)
	n := 0
	for i := 0; i < len(query); i++ {
		if query[i] != '?' {
			b.WriteByte(query[i])
			continue
		}
		n++
		b.WriteByte('$')
		b.WriteString(strconv.Itoa(n))
	}
	return b.String()
}

func (s *Store) exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return s.db.ExecContext(ctx, s.rebind(query), args...)
}

func (s *Store) query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return s.db.QueryContext(ctx, s.rebind(query), args...)
}

func (s *Store) queryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return s.db.QueryRowContext(ctx, s.rebind(query), args...)
}

// execOne runs a statement that must affect exactly one row, reporting
// ErrNotFound when it affected none. Callers use that to detect a lost race —
// a job already leased, a report that arrived too late — rather than a fault.
func (s *Store) execOne(ctx context.Context, query string, args ...any) error {
	res, err := s.exec(ctx, query, args...)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return pulse.ErrNotFound
	}
	return nil
}

func (s *Store) execAffected(ctx context.Context, query string, args ...any) (int64, error) {
	res, err := s.exec(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// tx runs fn in a transaction, rolling back on error.
func (s *Store) tx(ctx context.Context, fn func(*sql.Tx) error) error {
	t, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(t); err != nil {
		// The rollback error is deliberately discarded: it would mask the
		// error that actually caused the rollback.
		_ = t.Rollback()
		return err
	}
	return t.Commit()
}

// placeholders renders "?, ?, ?" for a slice and returns the values as []any,
// for IN clauses.
func placeholders[T any](vals []T) (string, []any) {
	if len(vals) == 0 {
		return "", nil
	}
	args := make([]any, 0, len(vals))
	for _, v := range vals {
		args = append(args, v)
	}
	return strings.TrimSuffix(strings.Repeat("?,", len(vals)), ","), args
}

// nullString maps "" to NULL, so unique indexes treat absent values as
// distinct rather than colliding on the empty string.
func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// nullTime maps the zero time to NULL.
func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

func timeFrom(v sql.NullTime) time.Time {
	if !v.Valid {
		return time.Time{}
	}
	return v.Time.UTC()
}

func stringFrom(v sql.NullString) string {
	if !v.Valid {
		return ""
	}
	return v.String
}

package sqlstore_test

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "modernc.org/sqlite" // test-only: the library itself registers no driver

	"github.com/seanly/pulse-devkit/pulse"
	"github.com/seanly/pulse-devkit/pulsestore/porttest"
	"github.com/seanly/pulse-devkit/pulsestore/sqlstore"
	"github.com/seanly/pulse-devkit/pulsetest"
)

// openSQLite returns a fresh in-memory database with the schema applied.
func openSQLite(t *testing.T, clock pulse.Clock) *sqlstore.Store {
	t.Helper()
	// A shared cache with one connection: SQLite has a single writer, and the
	// store's lease query depends on that for its atomicity guarantee.
	db, err := sql.Open("sqlite", "file::memory:?cache=shared&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })

	s, err := sqlstore.New(db, sqlstore.SQLite, sqlstore.WithClock(clock))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	// An in-memory shared-cache database outlives one connection, so wipe the
	// tables rather than relying on a fresh file per test.
	t.Cleanup(func() {
		for _, table := range []string{"pulse_jobs", "pulse_agent_configs", "pulse_bootstrap_tokens", "pulse_agents"} {
			_, _ = db.Exec("DELETE FROM " + table)
		}
	})
	return s
}

func TestSQLiteSatisfiesThePortContract(t *testing.T) {
	porttest.Run(t, func(t *testing.T) porttest.Stores {
		clock := pulsetest.NewClock(time.Time{})
		s := openSQLite(t, clock)
		return porttest.Stores{
			Agents: s, Tokens: s, Configs: s, Jobs: s, Clock: clock,
			CreateAgent: s.CreateAgent,
		}
	})
}

// TestPostgresSatisfiesThePortContract runs the same suite against PostgreSQL.
//
// Skipped unless PULSE_TEST_POSTGRES is set, so CI needs no database by
// default. The DSN points at a database the test may freely create tables in
// and delete rows from.
func TestPostgresSatisfiesThePortContract(t *testing.T) {
	dsn := os.Getenv("PULSE_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("set PULSE_TEST_POSTGRES to a DSN to run the PostgreSQL contract tests")
	}
	porttest.Run(t, func(t *testing.T) porttest.Stores {
		clock := pulsetest.NewClock(time.Time{})
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatalf("open postgres: %v", err)
		}
		t.Cleanup(func() { db.Close() })
		s, err := sqlstore.New(db, sqlstore.Postgres, sqlstore.WithClock(clock))
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Migrate(context.Background()); err != nil {
			t.Fatalf("migrate: %v", err)
		}
		t.Cleanup(func() {
			for _, table := range []string{"pulse_jobs", "pulse_agent_configs", "pulse_bootstrap_tokens", "pulse_agents"} {
				_, _ = db.Exec("DELETE FROM " + table)
			}
		})
		return porttest.Stores{
			Agents: s, Tokens: s, Configs: s, Jobs: s, Clock: clock,
			CreateAgent: s.CreateAgent,
		}
	})
}

func TestTablePrefixIsValidated(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// The prefix is interpolated into SQL, so anything outside [a-z0-9_] is
	// refused at construction rather than becoming an injection point.
	for _, bad := range []string{"bad-prefix", "a;DROP TABLE x;--", "Upper", "with space", `quo"te`} {
		if _, err := sqlstore.New(db, sqlstore.SQLite, sqlstore.WithTablePrefix(bad)); err == nil {
			t.Errorf("prefix %q was accepted", bad)
		}
	}
	for _, ok := range []string{"", "pulse_", "ci_jobs_", "x1_"} {
		if _, err := sqlstore.New(db, sqlstore.SQLite, sqlstore.WithTablePrefix(ok)); err != nil {
			t.Errorf("prefix %q was rejected: %v", ok, err)
		}
	}
}

func TestCustomPrefixWorksEndToEnd(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()

	s, err := sqlstore.New(db, sqlstore.SQLite, sqlstore.WithTablePrefix("ci_"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate with a custom prefix: %v", err)
	}
	defer func() {
		for _, table := range []string{"ci_jobs", "ci_agent_configs", "ci_bootstrap_tokens", "ci_agents"} {
			_, _ = db.Exec("DELETE FROM " + table)
		}
	}()

	a := pulse.Agent{ID: "a1", Name: "runner-1", Status: pulse.AgentOnline}
	if err := s.CreateAgent(ctx, &a); err != nil {
		t.Fatalf("create agent: %v", err)
	}
	// The prefix exists so the framework's tables can share a database with
	// an application's own; confirm it really is the table being used.
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM ci_agents").Scan(&n); err != nil {
		t.Fatalf("query the prefixed table: %v", err)
	}
	if n != 1 {
		t.Errorf("ci_agents has %d rows, want 1", n)
	}
}

func TestUnknownDialectIsRejected(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := sqlstore.New(db, sqlstore.Dialect("mysql")); err == nil {
		t.Error("want an error for an unsupported dialect")
	}
	if _, err := sqlstore.New(nil, sqlstore.SQLite); err == nil {
		t.Error("want an error for a nil database handle")
	}
}

func TestSchemaIsIdempotent(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	s, err := sqlstore.New(db, sqlstore.SQLite)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	// "Apply the schema at startup" is a legitimate strategy for a single-node
	// deployment, which it only stays if a second run is a no-op.
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Errorf("second migrate: %v", err)
	}
}

func TestSchemaRejectsABadPrefix(t *testing.T) {
	if _, err := sqlstore.Schema(sqlstore.SQLite, "bad-prefix"); err == nil {
		t.Error("want an error for an invalid prefix")
	}
	if _, err := sqlstore.Schema(sqlstore.Dialect("mysql"), "pulse_"); err == nil {
		t.Error("want an error for an unknown dialect")
	}
	stmts, err := sqlstore.Schema(sqlstore.Postgres, "pulse_")
	if err != nil {
		t.Fatal(err)
	}
	if len(stmts) == 0 {
		t.Error("want statements for postgres")
	}
}

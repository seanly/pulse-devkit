package pulsefs

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/seanly/pulse-devkit/pulse"
	"github.com/seanly/pulse-devkit/pulseagent"
)

func allow(root string) func() interface{ CheckPath(string) error } {
	p := pulseagent.Policy{AllowedPaths: []string{root}}
	return func() interface{ CheckPath(string) error } { return p }
}

func TestPutUnchangedDoesNotTouchMtime(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bundle.crt")
	if err := os.WriteFile(path, []byte("same"), 0o644); err != nil {
		t.Fatal(err)
	}
	past := time.Now().Add(-time.Hour)
	if err := os.Chtimes(path, past, past); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	fs := New(Options{Policy: allow(dir)})
	res, err := fs.Put(context.Background(), pulse.FSPut{Path: path, Content: "same"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Changed {
		t.Fatal("identical content was reported changed")
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !after.ModTime().Equal(before.ModTime()) {
		t.Fatalf("mtime moved from %s to %s", before.ModTime(), after.ModTime())
	}
}

func TestPutModeChangeIsChanged(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bundle.crt")
	if err := os.WriteFile(path, []byte("same"), 0o644); err != nil {
		t.Fatal(err)
	}
	fs := New(Options{Policy: allow(dir)})
	res, err := fs.Put(context.Background(), pulse.FSPut{Path: path, Content: "same", Mode: "0600"})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Changed {
		t.Fatal("a mode change should be reported")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %o, want 0600", info.Mode().Perm())
	}
}

func TestPutIsAtomicOnFailure(t *testing.T) {
	dir := t.TempDir()
	// The destination is a directory, so the rename cannot replace it with a
	// partial file. The original directory must still be a directory afterwards.
	path := filepath.Join(dir, "not-a-file")
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}
	fs := New(Options{Policy: allow(dir)})
	if _, err := fs.Put(context.Background(), pulse.FSPut{Path: path, Content: "nope"}); err == nil {
		t.Fatal("writing over a directory succeeded")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Fatal("failed write replaced the directory")
	}
}

func TestGetTruncates(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "big")
	if err := os.WriteFile(path, []byte("abcdef"), 0o644); err != nil {
		t.Fatal(err)
	}
	fs := New(Options{Policy: allow(dir)})
	res, err := fs.Get(context.Background(), pulse.FSGet{Path: path, MaxBytes: 3})
	if err != nil {
		t.Fatal(err)
	}
	if res.Content != "abc" || !res.Truncated {
		t.Fatalf("get = %+v", res)
	}
}

func TestRemoveMissingIsUnchanged(t *testing.T) {
	dir := t.TempDir()
	fs := New(Options{Policy: allow(dir)})
	res, err := fs.Remove(context.Background(), pulse.FSRemove{Path: filepath.Join(dir, "gone")})
	if err != nil {
		t.Fatal(err)
	}
	if res.Changed {
		t.Fatal("removing an absent path should not look like a change")
	}
}

func TestDeadManRestoresAfterDeadlineAndSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bundle.crt")
	if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	clock := now
	sw := OpenDeadMan(filepath.Join(dir, "state", "deadman.json"), func() time.Time { return clock })
	fs := New(Options{Policy: allow(dir), Switch: sw, Now: func() time.Time { return clock }})
	if _, err := fs.Put(context.Background(), pulse.FSPut{
		Path: path, Content: "new", Backup: true, ConfirmWithin: time.Minute,
	}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "new" {
		t.Fatalf("after put: %q %v", got, err)
	}

	// A new process, same state file, after the deadline, before any heartbeat.
	clock = now.Add(2 * time.Minute)
	again := OpenDeadMan(filepath.Join(dir, "state", "deadman.json"), func() time.Time { return clock })
	if err := again.Recover(); err != nil {
		t.Fatal(err)
	}
	got, err = os.ReadFile(path)
	if err != nil || string(got) != "old" {
		t.Fatalf("after recover: %q %v", got, err)
	}
}

func TestDeadManConfirmKeepsTheWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bundle.crt")
	if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	sw := OpenDeadMan(filepath.Join(dir, "deadman.json"), time.Now)
	fs := New(Options{Policy: allow(dir), Switch: sw})
	if _, err := fs.Put(context.Background(), pulse.FSPut{
		Path: path, Content: "new", Backup: true, ConfirmWithin: time.Minute,
	}); err != nil {
		t.Fatal(err)
	}
	sw.Confirm()
	if err := sw.Recover(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "new" {
		t.Fatalf("confirm did not keep the write: %q %v", got, err)
	}
}

func TestPathOutsideAllowlistIsRefused(t *testing.T) {
	dir := t.TempDir()
	fs := New(Options{Policy: allow(dir)})
	if _, err := fs.Get(context.Background(), pulse.FSGet{Path: "/etc/passwd"}); err == nil {
		t.Fatal("a path outside the allowlist was read")
	}
}

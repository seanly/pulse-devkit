// Package pulsefs is the fs.* primitive family.
//
// It writes, reads, stats and removes files without starting a subprocess.
// Paths are refused unless the local policy already allows that prefix: a
// server that can name any path can rewrite the machine.
package pulsefs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/seanly/pulse-devkit/pulse"
	"github.com/seanly/pulse-devkit/pulseagent"
	"github.com/seanly/pulse-devkit/pulsecodec"
)

const (
	defaultGetMax = 1 << 20
	backupSuffix  = ".pulsebak"
)

// Switch arms and clears a dead man's switch. Nil means writes cannot ask
// for ConfirmWithin.
type Switch interface {
	Arm(path, backup string, within time.Duration) error
}

// Options configures the executors.
type Options struct {
	// Policy is read per operation. Nil refuses every path.
	Policy func() interface{ CheckPath(string) error }
	// Switch persists dead-man state across restarts. Nil disables ConfirmWithin.
	Switch Switch
	Now    func() time.Time
}

// FS runs the file primitives.
type FS struct{ opts Options }

// New returns executors for fs.put, fs.get, fs.stat and fs.remove.
func New(o Options) *FS {
	if o.Now == nil {
		o.Now = time.Now
	}
	return &FS{opts: o}
}

// Executors registers the four kinds.
func Executors(o Options) pulseagent.Executors {
	fs := New(o)
	return pulseagent.Executors{
		pulse.KindFSPut: pulsecodec.Typed(pulsecodec.JSON[pulse.FSPut, pulse.FSPutResult](),
			func(ctx context.Context, _ pulse.Job, in pulse.FSPut) (pulse.FSPutResult, error) {
				return fs.Put(ctx, in)
			}),
		pulse.KindFSGet: pulsecodec.Typed(pulsecodec.JSON[pulse.FSGet, pulse.FSGetResult](),
			func(ctx context.Context, _ pulse.Job, in pulse.FSGet) (pulse.FSGetResult, error) {
				return fs.Get(ctx, in)
			}),
		pulse.KindFSStat: pulsecodec.Typed(pulsecodec.JSON[pulse.FSStat, pulse.FSStatResult](),
			func(ctx context.Context, _ pulse.Job, in pulse.FSStat) (pulse.FSStatResult, error) {
				return fs.Stat(ctx, in)
			}),
		pulse.KindFSRemove: pulsecodec.Typed(pulsecodec.JSON[pulse.FSRemove, pulse.FSRemoveResult](),
			func(ctx context.Context, _ pulse.Job, in pulse.FSRemove) (pulse.FSRemoveResult, error) {
				return fs.Remove(ctx, in)
			}),
	}
}

func (f *FS) check(path string) error {
	if f.opts.Policy == nil {
		return fmt.Errorf("pulsefs: no path policy")
	}
	return f.opts.Policy().CheckPath(path)
}

// Put writes path atomically. Identical content, mode and owner leave the
// file untouched, including its mtime.
func (f *FS) Put(_ context.Context, in pulse.FSPut) (pulse.FSPutResult, error) {
	if err := f.check(in.Path); err != nil {
		return pulse.FSPutResult{}, err
	}
	if in.Blob != "" {
		return pulse.FSPutResult{}, errors.New("pulsefs: fs.put blobs are not supported; send content")
	}
	data := []byte(in.Content)
	mode, err := desiredMode(in.Path, in.Mode)
	if err != nil {
		return pulse.FSPutResult{}, err
	}
	newSum := sha(data)

	existing, statErr := os.Stat(in.Path)
	var oldSum string
	if statErr == nil && !existing.IsDir() {
		b, err := os.ReadFile(in.Path)
		if err != nil {
			return pulse.FSPutResult{}, err
		}
		oldSum = sha(b)
		sameMode := existing.Mode().Perm() == mode
		sameOwner := ownerMatches(existing, in.Owner, in.Group)
		if oldSum == newSum && sameMode && sameOwner {
			return pulse.FSPutResult{Changed: false, OldSHA256: oldSum, NewSHA256: newSum}, nil
		}
	}

	var backupPath string
	if (in.Backup || in.ConfirmWithin > 0) && statErr == nil && !existing.IsDir() {
		backupPath = in.Path + backupSuffix
		if err := f.check(backupPath); err != nil {
			return pulse.FSPutResult{}, fmt.Errorf("pulsefs: backup path: %w", err)
		}
		if err := copyFile(in.Path, backupPath); err != nil {
			return pulse.FSPutResult{}, fmt.Errorf("pulsefs: backup: %w", err)
		}
	}
	if in.ConfirmWithin > 0 {
		if f.opts.Switch == nil {
			return pulse.FSPutResult{}, errors.New("pulsefs: ConfirmWithin requires a dead-man switch")
		}
		if backupPath == "" {
			return pulse.FSPutResult{}, errors.New("pulsefs: ConfirmWithin needs an existing file to restore")
		}
		if err := f.opts.Switch.Arm(in.Path, backupPath, in.ConfirmWithin); err != nil {
			return pulse.FSPutResult{}, err
		}
	}

	if err := atomicWrite(in.Path, data, mode); err != nil {
		return pulse.FSPutResult{}, err
	}
	if err := applyOwner(in.Path, in.Owner, in.Group); err != nil {
		return pulse.FSPutResult{}, err
	}
	return pulse.FSPutResult{
		Changed: true, OldSHA256: oldSum, NewSHA256: newSum, BackupPath: backupPath,
	}, nil
}

// Get reads a file, bounded so a log cannot be used to flood the server.
func (f *FS) Get(_ context.Context, in pulse.FSGet) (pulse.FSGetResult, error) {
	if err := f.check(in.Path); err != nil {
		return pulse.FSGetResult{}, err
	}
	max := in.MaxBytes
	if max <= 0 {
		max = defaultGetMax
	}
	file, err := os.Open(in.Path)
	if err != nil {
		return pulse.FSGetResult{}, err
	}
	defer file.Close()
	buf, err := io.ReadAll(io.LimitReader(file, max+1))
	if err != nil {
		return pulse.FSGetResult{}, err
	}
	truncated := int64(len(buf)) > max
	if truncated {
		buf = buf[:max]
	}
	return pulse.FSGetResult{Content: string(buf), SHA256: sha(buf), Truncated: truncated}, nil
}

// Stat reports a path without returning its contents unless a checksum was asked for.
func (f *FS) Stat(_ context.Context, in pulse.FSStat) (pulse.FSStatResult, error) {
	if err := f.check(in.Path); err != nil {
		return pulse.FSStatResult{}, err
	}
	info, err := os.Stat(in.Path)
	if errors.Is(err, os.ErrNotExist) {
		return pulse.FSStatResult{Exists: false}, nil
	}
	if err != nil {
		return pulse.FSStatResult{}, err
	}
	res := pulse.FSStatResult{
		Exists: true, Mode: fmt.Sprintf("%04o", info.Mode().Perm()),
		Size: info.Size(), ModTime: info.ModTime().UTC(), IsDir: info.IsDir(),
	}
	if in.Checksum && !info.IsDir() {
		b, err := os.ReadFile(in.Path)
		if err != nil {
			return pulse.FSStatResult{}, err
		}
		res.SHA256 = sha(b)
	}
	return res, nil
}

// Remove deletes path. A path that is already gone reports changed=false.
func (f *FS) Remove(_ context.Context, in pulse.FSRemove) (pulse.FSRemoveResult, error) {
	if err := f.check(in.Path); err != nil {
		return pulse.FSRemoveResult{}, err
	}
	var err error
	if in.Recursive {
		err = os.RemoveAll(in.Path)
	} else {
		err = os.Remove(in.Path)
	}
	if errors.Is(err, os.ErrNotExist) {
		return pulse.FSRemoveResult{Changed: false}, nil
	}
	if err != nil {
		return pulse.FSRemoveResult{}, err
	}
	return pulse.FSRemoveResult{Changed: true}, nil
}

func desiredMode(path, mode string) (os.FileMode, error) {
	if mode == "" {
		if info, err := os.Stat(path); err == nil {
			return info.Mode().Perm(), nil
		}
		return 0o644, nil
	}
	n, err := strconv.ParseUint(mode, 8, 32)
	if err != nil {
		return 0, fmt.Errorf("pulsefs: mode %q: %w", mode, err)
	}
	return os.FileMode(n), nil
}

func ownerMatches(info os.FileInfo, owner, group string) bool {
	if owner == "" && group == "" {
		return true
	}
	uid, gid, ok := fileIDs(info)
	if !ok {
		return false
	}
	if owner != "" {
		want, err := lookupUID(owner)
		if err != nil || want != uid {
			return false
		}
	}
	if group != "" {
		want, err := lookupGID(group)
		if err != nil || want != gid {
			return false
		}
	}
	return true
}

func applyOwner(path, owner, group string) error {
	if owner == "" && group == "" {
		return nil
	}
	uid, gid := -1, -1
	if owner != "" {
		id, err := lookupUID(owner)
		if err != nil {
			return err
		}
		uid = id
	}
	if group != "" {
		id, err := lookupGID(group)
		if err != nil {
			return err
		}
		gid = id
	}
	return os.Chown(path, uid, gid)
}

func lookupUID(name string) (int, error) {
	if n, err := strconv.Atoi(name); err == nil {
		return n, nil
	}
	u, err := user.Lookup(name)
	if err != nil {
		return 0, fmt.Errorf("pulsefs: owner %q: %w", name, err)
	}
	return strconv.Atoi(u.Uid)
}

func lookupGID(name string) (int, error) {
	if n, err := strconv.Atoi(name); err == nil {
		return n, nil
	}
	g, err := user.LookupGroup(name)
	if err != nil {
		return 0, fmt.Errorf("pulsefs: group %q: %w", name, err)
	}
	return strconv.Atoi(g.Gid)
}

func fileIDs(info os.FileInfo) (uid, gid int, ok bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, false
	}
	return int(st.Uid), int(st.Gid), true
}

func atomicWrite(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".pulse-*.tmp")
	if err != nil {
		return err
	}
	name := tmp.Name()
	ok := false
	defer func() {
		if !ok {
			os.Remove(name)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	ok = true
	// Durability of the directory entry, not just the file.
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return nil
}

func copyFile(src, dst string) error {
	b, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	return atomicWrite(dst, b, info.Mode().Perm())
}

func sha(b []byte) string { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }

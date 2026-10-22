package pulsefs

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// DeadMan restores a file when the agent does not reach the server in time.
//
// The record is on disk, not in memory: a restart is the moment a bad trust
// store is most likely to show itself, and an in-memory timer would die with
// the process that needed it.
type DeadMan struct {
	mu   sync.Mutex
	path string
	now  func() time.Time
}

type armedFile struct {
	Path     string    `json:"path"`
	Backup   string    `json:"backup"`
	Deadline time.Time `json:"deadline"`
}

type armedState struct {
	Files []armedFile `json:"files"`
}

// OpenDeadMan loads state from path, which is created on the first Arm.
func OpenDeadMan(path string, now func() time.Time) *DeadMan {
	if now == nil {
		now = time.Now
	}
	return &DeadMan{path: path, now: now}
}

// Arm records that path must be restored from backup unless Confirm happens
// before the deadline.
func (d *DeadMan) Arm(path, backup string, within time.Duration) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	st, err := d.load()
	if err != nil {
		return err
	}
	st.Files = append(st.Files, armedFile{
		Path: path, Backup: backup, Deadline: d.now().Add(within),
	})
	return d.store(st)
}

// Confirm drops every armed restore. A heartbeat that reached the server is
// the signal the write did not cut the agent off.
func (d *DeadMan) Confirm() {
	d.mu.Lock()
	defer d.mu.Unlock()
	_ = d.store(armedState{})
}

// Recover restores any file whose deadline has passed. Call it at startup,
// before the first connection attempt, and periodically while disconnected.
func (d *DeadMan) Recover() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	st, err := d.load()
	if err != nil {
		return err
	}
	now := d.now()
	var keep []armedFile
	for _, f := range st.Files {
		if now.Before(f.Deadline) {
			keep = append(keep, f)
			continue
		}
		if err := restore(f.Backup, f.Path); err != nil {
			return err
		}
		_ = os.Remove(f.Backup)
	}
	return d.store(armedState{Files: keep})
}

func (d *DeadMan) load() (armedState, error) {
	b, err := os.ReadFile(d.path)
	if os.IsNotExist(err) {
		return armedState{}, nil
	}
	if err != nil {
		return armedState{}, err
	}
	var st armedState
	if err := json.Unmarshal(b, &st); err != nil {
		return armedState{}, err
	}
	return st, nil
}

func (d *DeadMan) store(st armedState) error {
	if err := os.MkdirAll(filepath.Dir(d.path), 0o700); err != nil {
		return err
	}
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return atomicWrite(d.path, b, 0o600)
}

func restore(backup, path string) error {
	b, err := os.ReadFile(backup)
	if err != nil {
		return err
	}
	info, err := os.Stat(backup)
	if err != nil {
		return err
	}
	return atomicWrite(path, b, info.Mode().Perm())
}

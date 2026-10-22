// Package pulseexec is the exec.run primitive.
//
// One job, one directory, deleted when the job ends. The program is started
// directly — there is no shell — and a cancel kills the process group, not
// only the parent. Secrets travel in the environment and are never written
// back into a log or a result.
package pulseexec

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/seanly/pulse-devkit/pulse"
	"github.com/seanly/pulse-devkit/pulseagent"
	"github.com/seanly/pulse-devkit/pulsecodec"
)

const (
	tailBytes    = 8 << 10
	captureBytes = 64 << 10
)

// Options configures the executor.
type Options struct {
	// Root is the parent of each workspace. Empty prefers a tmpfs.
	Root string
	// Now is the clock for the reported duration. Nil means time.Now.
	Now func() time.Time
}

// Runner executes exec.run jobs.
type Runner struct{ opts Options }

// New returns a runner.
func New(o Options) *Runner {
	if o.Root == "" {
		o.Root = defaultRoot()
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return &Runner{opts: o}
}

// Executors registers exec.run.
func Executors(o Options) pulseagent.Executors {
	r := New(o)
	return pulseagent.Executors{
		pulse.KindExecRun: pulsecodec.Typed(pulsecodec.JSON[pulse.ExecRun, pulse.ExecResult](),
			func(ctx context.Context, job pulse.Job, in pulse.ExecRun) (pulse.ExecResult, error) {
				return r.Run(ctx, job, in)
			}),
	}
}

// Run executes one program. The workspace is gone when this returns, including
// when the job is cancelled or the process panics.
func (r *Runner) Run(ctx context.Context, _ pulse.Job, in pulse.ExecRun) (pulse.ExecResult, error) {
	if len(in.Blobs) > 0 {
		return pulse.ExecResult{}, errors.New("pulseexec: blobs are not supported")
	}
	if len(in.Argv) == 0 {
		return pulse.ExecResult{}, errors.New("pulseexec: argv is empty")
	}
	started := r.opts.Now()
	dir, err := os.MkdirTemp(r.opts.Root, "pulse-exec-")
	if err != nil {
		return pulse.ExecResult{}, err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		os.RemoveAll(dir)
		return pulse.ExecResult{}, err
	}
	defer os.RemoveAll(dir)

	for name, file := range in.Files {
		if err := writeFile(dir, name, file); err != nil {
			return pulse.ExecResult{}, err
		}
	}

	work := dir
	if in.Dir != "" {
		work = filepath.Join(dir, in.Dir)
	}
	cmd := exec.CommandContext(ctx, in.Argv[0], in.Argv[1:]...)
	cmd.Dir = work
	cmd.Env = envList(in.Env)
	if in.Stdin != "" {
		cmd.Stdin = bytes.NewBufferString(in.Stdin)
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		// The negative pid is the group. ansible-playbook forks ssh; killing
		// only the parent leaves those children holding connections.
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
	}
	cmd.WaitDelay = time.Second

	var stdout, stderr tailWriter
	stdout.limit = tailBytes
	stderr.limit = tailBytes
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if in.StreamOutput {
		if s, ok := pulse.StreamsFrom(ctx); ok {
			if s.Stdout != nil {
				cmd.Stdout = io.MultiWriter(&stdout, s.Stdout)
			}
			if s.Stderr != nil {
				cmd.Stderr = io.MultiWriter(&stderr, s.Stderr)
			}
		}
	}

	runErr := cmd.Run()
	res := pulse.ExecResult{
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		Duration: r.opts.Now().Sub(started),
	}
	if cmd.ProcessState != nil {
		res.ExitCode = cmd.ProcessState.ExitCode()
	}
	if len(in.Capture) > 0 {
		captured, truncated, err := capture(dir, in.Capture)
		if err != nil {
			return pulse.ExecResult{}, err
		}
		res.Captured = captured
		res.Truncated = truncated
	}
	if tw, ok := streamTruncated(ctx); ok {
		res.Truncated = res.Truncated || tw
	}
	if runErr != nil && ctx.Err() != nil {
		return res, ctx.Err()
	}
	if runErr != nil {
		var exit *exec.ExitError
		if errors.As(runErr, &exit) {
			// A non-zero exit is a result, not an executor failure. The
			// caller decides whether exit 1 means the job failed.
			return res, nil
		}
		return res, runErr
	}
	return res, nil
}

func streamTruncated(ctx context.Context) (bool, bool) {
	s, ok := pulse.StreamsFrom(ctx)
	if !ok {
		return false, false
	}
	trunc := false
	if t, ok := s.Stdout.(interface{ Truncated() bool }); ok && t.Truncated() {
		trunc = true
	}
	if t, ok := s.Stderr.(interface{ Truncated() bool }); ok && t.Truncated() {
		trunc = true
	}
	return trunc, true
}

func writeFile(root, name string, file pulse.File) error {
	if filepath.IsAbs(name) || name == "" {
		return fmt.Errorf("pulseexec: file path %q must be relative", name)
	}
	clean := filepath.Clean(name)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return fmt.Errorf("pulseexec: file path %q escapes the workspace", name)
	}
	dst := filepath.Join(root, clean)
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	mode := os.FileMode(0o644)
	if file.Mode != "" {
		n, err := parseMode(file.Mode)
		if err != nil {
			return err
		}
		mode = n
	}
	return os.WriteFile(dst, []byte(file.Content), mode)
}

func parseMode(s string) (os.FileMode, error) {
	var n uint32
	if _, err := fmt.Sscanf(s, "%o", &n); err != nil {
		return 0, fmt.Errorf("pulseexec: mode %q: %w", s, err)
	}
	return os.FileMode(n), nil
}

func envList(extra map[string]string) []string {
	// Start from a small base rather than os.Environ: inheriting the agent
	// process environment would hand the program the bootstrap token if one
	// were still in the environment.
	base := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "TMPDIR=" + os.Getenv("TMPDIR")}
	for k, v := range extra {
		base = append(base, k+"="+v)
	}
	return base
}

func capture(root string, paths []string) (map[string]string, bool, error) {
	out := make(map[string]string, len(paths))
	truncated := false
	for _, p := range paths {
		if filepath.IsAbs(p) {
			return nil, false, fmt.Errorf("pulseexec: capture path %q must be relative", p)
		}
		b, err := os.ReadFile(filepath.Join(root, p))
		if err != nil {
			return nil, false, err
		}
		if len(b) > captureBytes {
			b = b[:captureBytes]
			truncated = true
		}
		out[p] = string(b)
	}
	return out, truncated, nil
}

func defaultRoot() string {
	if runtime.GOOS == "linux" {
		if fi, err := os.Stat("/dev/shm"); err == nil && fi.IsDir() {
			return "/dev/shm"
		}
	}
	return os.TempDir()
}

// tailWriter keeps the last limit bytes.
type tailWriter struct {
	buf   bytes.Buffer
	limit int
}

func (w *tailWriter) Write(p []byte) (int, error) {
	_, _ = w.buf.Write(p)
	if w.buf.Len() > w.limit {
		b := w.buf.Bytes()
		w.buf.Reset()
		_, _ = w.buf.Write(b[len(b)-w.limit:])
	}
	return len(p), nil
}

func (w *tailWriter) String() string { return w.buf.String() }

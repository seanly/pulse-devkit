package pulseexec

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/seanly/pulse-devkit/pulse"
)

func TestMain(m *testing.M) {
	switch os.Getenv("PULSEEXEC_HELPER") {
	case "parent":
		cmd := exec.Command(os.Args[0])
		cmd.Env = append(os.Environ(), "PULSEEXEC_HELPER=child")
		cmd.Stdout = os.Stdout
		if err := cmd.Start(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println(cmd.Process.Pid)
		os.Stdout.Sync()
		select {}
	case "child":
		select {}
	case "write":
		n, _ := strconv.Atoi(os.Args[2])
		if err := os.WriteFile(os.Args[1], []byte(strings.Repeat("x", n)), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	default:
		os.Exit(m.Run())
	}
}

func TestArgvIsNotAShell(t *testing.T) {
	echo := "/bin/echo"
	if _, err := os.Stat(echo); err != nil {
		t.Skip(echo)
	}
	r := New(Options{Root: t.TempDir()})
	res, err := r.Run(context.Background(), pulse.Job{}, pulse.ExecRun{
		Argv: []string{echo, "; rm -rf /"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(res.Stdout) != "; rm -rf /" {
		t.Fatalf("stdout = %q, want the argument kept literal", res.Stdout)
	}
}

func TestEnvValueIsNotInTheResult(t *testing.T) {
	r := New(Options{Root: t.TempDir()})
	secret := "sudo-secret-value"
	trueBin := "/usr/bin/true"
	if _, statErr := os.Stat(trueBin); statErr != nil {
		trueBin = "/bin/true"
	}
	res, err := r.Run(context.Background(), pulse.Job{}, pulse.ExecRun{
		Argv: []string{trueBin},
		Env:  map[string]string{"ANSIBLE_SUDO_PASS": secret},
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), secret) {
		t.Fatalf("secret leaked into the result: %s", raw)
	}
}

func TestWorkspaceIsRemoved(t *testing.T) {
	root := t.TempDir()
	r := New(Options{Root: root})
	trueBin := "/usr/bin/true"
	if _, err := os.Stat(trueBin); err != nil {
		trueBin = "/bin/true"
	}
	if _, err := r.Run(context.Background(), pulse.Job{}, pulse.ExecRun{
		Argv:  []string{trueBin},
		Files: map[string]pulse.File{"secret.key": {Content: "key", Mode: "0600"}},
	}); err != nil {
		t.Fatal(err)
	}
	left, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 0 {
		t.Fatalf("workspace left behind: %v", left)
	}
}

func TestCancelKillsTheProcessGroup(t *testing.T) {
	r := New(Options{Root: t.TempDir()})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan pulse.ExecResult, 1)
	go func() {
		res, err := r.Run(ctx, pulse.Job{}, pulse.ExecRun{
			Argv: []string{os.Args[0]},
			Env:  map[string]string{"PULSEEXEC_HELPER": "parent"},
		})
		if err != nil && ctx.Err() == nil {
			t.Errorf("run: %v", err)
		}
		done <- res
	}()
	var child int
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case res := <-done:
			t.Fatalf("process exited before cancel, stdout %q", res.Stdout)
		default:
		}
		// The pid is printed by the helper. We don't have it until stdout is
		// streamed; poll /bin/ps is heavier. Cancel after a short wait and
		// then read the pid from the result.
		time.Sleep(50 * time.Millisecond)
		break
	}
	time.Sleep(100 * time.Millisecond)
	cancel()
	res := <-done
	line := strings.TrimSpace(res.Stdout)
	if line == "" {
		t.Fatal("parent did not print the child pid")
	}
	child, _ = strconv.Atoi(strings.Fields(line)[0])
	if child == 0 {
		t.Fatalf("child pid %q", line)
	}
	time.Sleep(200 * time.Millisecond)
	if err := syscall.Kill(child, 0); err == nil {
		t.Fatalf("child %d still running after the group was cancelled", child)
	}
}

func TestCaptureTruncates(t *testing.T) {
	r := New(Options{Root: t.TempDir()})
	res, err := r.Run(context.Background(), pulse.Job{}, pulse.ExecRun{
		Argv:    []string{os.Args[0], "out.bin", "70000"},
		Env:     map[string]string{"PULSEEXEC_HELPER": "write"},
		Capture: []string{"out.bin"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Truncated || len(res.Captured["out.bin"]) != captureBytes {
		t.Fatalf("captured %d truncated %v", len(res.Captured["out.bin"]), res.Truncated)
	}
}

func TestBlobsAreRefused(t *testing.T) {
	r := New(Options{Root: t.TempDir()})
	_, err := r.Run(context.Background(), pulse.Job{}, pulse.ExecRun{
		Argv:  []string{"/bin/true"},
		Blobs: map[string]pulse.Blob{"a": {Digest: "sha256:aa"}},
	})
	if err == nil {
		t.Fatal("blobs were accepted")
	}
}

func TestFileMode(t *testing.T) {
	root := t.TempDir()
	// Keep the workspace by running a helper that copies... we remove the
	// workspace, so check the mode from inside the command via the helper.
	// Write the file and stat it before the process exits by using a helper
	// that prints the mode.
	r := New(Options{Root: root})
	res, err := r.Run(context.Background(), pulse.Job{}, pulse.ExecRun{
		Argv: []string{"/bin/sh", "-c", "stat -f %Lp secret.key"},
		Files: map[string]pulse.File{
			"secret.key": {Content: "key", Mode: "0600"},
		},
	})
	// /bin/sh is what this test uses to stat. The production admitter refuses
	// it; here we are below that layer, checking the file landed with the mode.
	if err != nil {
		// darwin stat -f, linux stat -c. Try linux form if the first failed.
		res, err = r.Run(context.Background(), pulse.Job{}, pulse.ExecRun{
			Argv:  []string{"/bin/sh", "-c", "stat -c %a secret.key"},
			Files: map[string]pulse.File{"secret.key": {Content: "key", Mode: "0600"}},
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(res.Stdout) != "600" && strings.TrimSpace(res.Stdout) != "0600" {
		t.Fatalf("mode = %q", res.Stdout)
	}
	_ = filepath.Separator
}

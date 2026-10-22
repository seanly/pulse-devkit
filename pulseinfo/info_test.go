package pulseinfo

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/seanly/pulse-devkit/pulse"
)

type denyAll struct{}

func (denyAll) CheckBin(string) error { return errDenied }

type allowPath struct{ path string }

func (a allowPath) CheckBin(path string) error {
	if path == a.path {
		return nil
	}
	return errDenied
}

var errDenied = errString("denied")

type errString string

func (e errString) Error() string { return string(e) }

func TestBinaryVersionRunsOnlyWhenAllowed(t *testing.T) {
	var ran []string
	c := New(Options{
		LookPath: func(name string) (string, error) { return "/usr/bin/" + name, nil },
		Version: func(ctx context.Context, path string) (string, error) {
			ran = append(ran, path)
			return "tool 1.2\nextra", nil
		},
		ReadOS: func() (string, string, string) { return "linux", "debian", "12" },
		Now:    func() time.Time { return time.Unix(10, 0).UTC() },
		Policy: func() BinChecker { return allowPath{path: "/usr/bin/ansible-playbook"} },
	})
	facts, err := c.Collect(context.Background(), pulse.AgentInfo{
		Binaries: []string{"ansible-playbook", "python3"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if facts.Distro != "debian" || facts.Version != "12" {
		t.Fatalf("os facts = %+v", facts)
	}
	if facts.Binaries["ansible-playbook"].Version != "tool 1.2" {
		t.Errorf("allowed binary version = %q", facts.Binaries["ansible-playbook"].Version)
	}
	if facts.Binaries["python3"].Version != "" {
		t.Errorf("disallowed binary was executed, version = %q", facts.Binaries["python3"].Version)
	}
	if len(ran) != 1 || !strings.HasSuffix(ran[0], "ansible-playbook") {
		t.Fatalf("ran %v, want only the allowlisted binary", ran)
	}
}

func TestNoPolicyDoesNotExecute(t *testing.T) {
	c := New(Options{
		LookPath: func(name string) (string, error) { return "/bin/" + name, nil },
		Version: func(context.Context, string) (string, error) {
			t.Fatal("version command ran without a policy")
			return "", nil
		},
		ReadOS: func() (string, string, string) { return "linux", "", "" },
	})
	facts, err := c.Collect(context.Background(), pulse.AgentInfo{Binaries: []string{"true"}})
	if err != nil {
		t.Fatal(err)
	}
	if facts.Binaries["true"].Path != "/bin/true" || facts.Binaries["true"].Version != "" {
		t.Fatalf("fact = %+v", facts.Binaries["true"])
	}
}

func TestFactsHaveNoVerdict(t *testing.T) {
	c := New(Options{
		ReadOS:   func() (string, string, string) { return "linux", "rhel", "8.9" },
		LookPath: func(string) (string, error) { return "", errDenied },
		Now:      func() time.Time { return time.Unix(1, 0).UTC() },
	})
	facts, err := c.Collect(context.Background(), pulse.AgentInfo{})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(facts)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "warn_days") || strings.Contains(string(raw), `"status"`) {
		t.Fatalf("facts carry a judgement: %s", raw)
	}
}

package pulseagent

import (
	"context"
	"testing"

	"github.com/seanly/pulse-devkit/pulse"
)

func TestDefaultPolicyIsReadOnly(t *testing.T) {
	p := DefaultPolicy()
	all := Executors{
		pulse.KindNetTLSProbe: ExecutorFunc(nilExec),
		pulse.KindAgentInfo:   ExecutorFunc(nilExec),
		pulse.KindFSPut:       ExecutorFunc(nilExec),
		pulse.KindExecRun:     ExecutorFunc(nilExec),
	}
	got := p.Filter(all)
	if _, ok := got[pulse.KindNetTLSProbe]; !ok {
		t.Error("net.tlsprobe should survive the default policy")
	}
	if _, ok := got[pulse.KindAgentInfo]; !ok {
		t.Error("agent.info should survive the default policy")
	}
	if _, ok := got[pulse.KindFSPut]; ok {
		t.Error("fs.put must not be registered under the default policy")
	}
	if _, ok := got[pulse.KindExecRun]; ok {
		t.Error("exec.run must not be registered under the default policy")
	}
}

func TestEmptyKindsMeanTheReadOnlyDefault(t *testing.T) {
	p := (Policy{}).withDefaultKinds()
	if !p.AllowsKind(pulse.KindNetTLSProbe) || p.AllowsKind(pulse.KindFSPut) {
		t.Fatalf("kinds = %v", p.AllowedKinds)
	}
}

func TestShellInAllowlistIsRejected(t *testing.T) {
	for _, bin := range []string{"/bin/sh", "/bin/bash", "/usr/bin/bash", "/bin/zsh"} {
		p := Policy{AllowedBins: []string{bin}}
		if err := p.Validate(); err == nil {
			t.Errorf("%s was accepted; a shell allowlist entry permits every command", bin)
		}
	}
	ok := Policy{AllowedBins: []string{"/usr/sbin/update-ca-certificates"}}
	if err := ok.Validate(); err != nil {
		t.Errorf("a concrete binary was refused: %v", err)
	}
	rel := Policy{AllowedBins: []string{"update-ca-certificates"}}
	if err := rel.Validate(); err == nil {
		t.Error("a relative allowlist entry should be refused")
	}
}

func TestTightenDropsPermissionsTheLocalPolicyDoesNotHave(t *testing.T) {
	local := Policy{
		AllowedKinds: []string{"net.*", "fs.put", "exec.run"},
		AllowedBins:  []string{"/usr/sbin/update-ca-certificates"},
		AllowedPaths: []string{"/usr/local/share/ca-certificates"},
	}
	pushed := Policy{
		AllowedKinds: []string{"net.*", "exec.run", "fs.remove"},
		AllowedBins:  []string{"/bin/true", "/usr/sbin/update-ca-certificates"},
		AllowedPaths: []string{"/etc", "/usr/local/share/ca-certificates"},
	}
	got := Tighten(local, pushed)
	if allows(got.AllowedKinds, pulse.KindFSRemove) {
		t.Errorf("fs.remove was added by a push: %v", got.AllowedKinds)
	}
	if !allows(got.AllowedKinds, pulse.KindExecRun) || !allows(got.AllowedKinds, "net.tlsprobe") {
		t.Errorf("a push should be able to keep what was already allowed: %v", got.AllowedKinds)
	}
	if len(got.AllowedBins) != 1 || got.AllowedBins[0] != "/usr/sbin/update-ca-certificates" {
		t.Errorf("bins = %v, want only the one already allowed", got.AllowedBins)
	}
	if len(got.AllowedPaths) != 1 {
		t.Errorf("paths = %v, want the local prefix only", got.AllowedPaths)
	}
}

func TestTightenOfAnEmptyPushKeepsTheLocalPolicy(t *testing.T) {
	local := DefaultPolicy()
	got := Tighten(local, Policy{})
	if len(got.AllowedKinds) != 2 {
		t.Fatalf("kinds = %v, want the local list untouched", got.AllowedKinds)
	}
}

func TestAdmitRefusesAPathOutsideTheAllowlist(t *testing.T) {
	p := Policy{AllowedPaths: []string{"/usr/local/share/ca-certificates"}}
	job := pulse.Job{Kind: pulse.KindFSPut, Payload: []byte(`{"path":"/etc/passwd","content":"x"}`)}
	if err := p.Admit(job); err == nil {
		t.Fatal("path outside the allowlist was admitted")
	}
	ok := pulse.Job{Kind: pulse.KindFSPut, Payload: []byte(`{"path":"/usr/local/share/ca-certificates/local.crt"}`)}
	if err := p.Admit(ok); err != nil {
		t.Fatal(err)
	}
}

func TestAdmitRefusesBlobsAndAShell(t *testing.T) {
	p := Policy{AllowedBins: []string{"/usr/bin/ansible-playbook"}}
	blobs := pulse.Job{Kind: pulse.KindExecRun, Payload: []byte(`{"argv":["/usr/bin/ansible-playbook"],"blobs":{"a":{"digest":"sha256:aa"}}}`)}
	if err := p.Admit(blobs); err == nil {
		t.Fatal("blobs should be refused")
	}
	shell := pulse.Job{Kind: pulse.KindExecRun, Payload: []byte(`{"argv":["/bin/sh","-c","true"]}`)}
	if err := p.Admit(shell); err == nil {
		t.Fatal("a shell argv was admitted")
	}
}

func TestCheckResolversDefaultsToDeny(t *testing.T) {
	p := Policy{}
	if err := p.CheckResolvers(nil); err != nil {
		t.Fatal(err)
	}
	if err := p.CheckResolvers([]string{"8.8.8.8"}); err == nil {
		t.Fatal("a custom resolver was allowed with an empty list")
	}
	p.AllowedResolvers = []string{"1.1.1.1"}
	if err := p.CheckResolvers([]string{"1.1.1.1"}); err != nil {
		t.Fatal(err)
	}
}

func nilExec(context.Context, pulse.Job) (pulse.Result, error) { return pulse.Result{}, nil }

func allows(patterns []string, kind string) bool {
	return kindAllowed(patterns, kind)
}

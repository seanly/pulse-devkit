package pulseagent

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/seanly/pulse-devkit/pulse"
)

// Policy is the floor an agent's owner sets, and that a server push cannot
// lower.
//
// A deployment that registers every executor and then forgets the policy file
// would hand a compromised server a shell on every host. The default is
// therefore the read-only kinds, an empty executable allowlist and an empty
// path allowlist. Opening any of those is a local edit, not a push.
type Policy struct {
	// AllowedKinds are exact kinds ("fs.put") or a family ("net.*").
	// Empty means DefaultPolicy's kinds, not "everything".
	AllowedKinds []string
	// AllowedBins are absolute paths exec.run and agent.info may execute.
	// Empty means nothing may be executed.
	AllowedBins []string
	// WorkspaceRoot is where exec.run builds its single-use directory.
	// Empty means a tmpfs when one is available, otherwise the system temp dir.
	WorkspaceRoot string
	// AllowedPaths are absolute prefixes fs.* may touch. Empty means no path.
	AllowedPaths []string
	// AllowedResolvers are the DNS servers a net.* job may name.
	// Empty means a job may not name any.
	AllowedResolvers []string
	// RunAs is informational for the host: the framework does not change
	// uid. The host process is already whoever it was started as.
	RunAs string
}

// DefaultPolicy is what an agent with no policy section is allowed to do:
// observe the network and report its own host, and nothing that writes or
// executes.
func DefaultPolicy() Policy {
	return Policy{AllowedKinds: []string{"net.*", "agent.info"}}
}

// payloadKey is the AgentConfig.payload section for a tightening push.
// It sits beside pulseguard's section; neither parses the other's bytes.
const policyPayloadKey = "policy"

type policySection struct {
	AllowedKinds      []string `json:"allowed_kinds,omitempty"`
	AllowedBins       []string `json:"allowed_bins,omitempty"`
	AllowedPaths      []string `json:"allowed_paths,omitempty"`
	AllowedResolvers  []string `json:"allowed_resolvers,omitempty"`
}

// Validate rejects a policy that would make the allowlists decorative.
//
// A shell in AllowedBins is the case that matters: exec.run does not invoke
// a shell, so the only way to get one is to name it as the program. That
// must fail at configuration time, loudly, not as a job that happens to run.
func (p Policy) Validate() error {
	for _, bin := range p.AllowedBins {
		if !filepath.IsAbs(bin) {
			return fmt.Errorf("pulseagent: exec.allowed_bins entry %q must be an absolute path", bin)
		}
		if isShell(bin) {
			return fmt.Errorf("pulseagent: exec.allowed_bins refuses %q: allowing a shell allows every command", bin)
		}
	}
	for _, path := range p.AllowedPaths {
		if !filepath.IsAbs(path) {
			return fmt.Errorf("pulseagent: fs.allowed_paths entry %q must be an absolute path", path)
		}
	}
	return nil
}

// withDefaultKinds fills the read-only kinds when the owner left the list
// empty. An explicit list, even a narrow one, is kept as written.
func (p Policy) withDefaultKinds() Policy {
	if len(p.AllowedKinds) == 0 {
		p.AllowedKinds = DefaultPolicy().AllowedKinds
	}
	return p
}

// AllowsKind reports whether kind may be executed under this policy.
func (p Policy) AllowsKind(kind string) bool {
	return kindAllowed(p.AllowedKinds, kind)
}

// Filter returns the executors this policy permits. A kind that does not
// match is absent, so a dispatched job is declined as unsupported rather
// than run and then rejected.
func (p Policy) Filter(in Executors) Executors {
	out := Executors{}
	for k, e := range in {
		if p.AllowsKind(k) {
			out[k] = e
		}
	}
	return out
}

// CheckBin reports whether path may be executed.
func (p Policy) CheckBin(path string) error {
	if path == "" {
		return fmt.Errorf("pulseagent: executable path is empty")
	}
	cleaned := filepath.Clean(path)
	if isShell(cleaned) {
		return fmt.Errorf("pulseagent: refusing shell %q", path)
	}
	if !filepath.IsAbs(cleaned) {
		return fmt.Errorf("pulseagent: executable %q is not an absolute path", path)
	}
	for _, allow := range p.AllowedBins {
		if cleaned == filepath.Clean(allow) {
			return nil
		}
	}
	return fmt.Errorf("pulseagent: %q is not in exec.allowed_bins", path)
}

// CheckPath reports whether path is under an allowed prefix.
func (p Policy) CheckPath(path string) error {
	cleaned := filepath.Clean(path)
	if !filepath.IsAbs(cleaned) {
		return fmt.Errorf("pulseagent: path %q must be absolute", path)
	}
	for _, prefix := range p.AllowedPaths {
		pref := filepath.Clean(prefix)
		if cleaned == pref || strings.HasPrefix(cleaned, pref+string(filepath.Separator)) {
			return nil
		}
	}
	if len(p.AllowedPaths) == 0 {
		return fmt.Errorf("pulseagent: fs.allowed_paths is empty, so %q is refused", path)
	}
	return fmt.Errorf("pulseagent: %q is outside fs.allowed_paths", path)
}

// CheckResolvers reports whether a job may name these DNS servers.
// An empty list is the system resolver and is always allowed.
func (p Policy) CheckResolvers(servers []string) error {
	if len(servers) == 0 {
		return nil
	}
	allowed := map[string]bool{}
	for _, s := range p.AllowedResolvers {
		allowed[s] = true
	}
	for _, s := range servers {
		if !allowed[s] {
			return fmt.Errorf("pulseagent: resolver %q is not in net.allowed_resolvers", s)
		}
	}
	return nil
}

// Admit applies the checks that can be decided from the payload alone, so a
// refusal is an ack rather than a failed attempt.
func (p Policy) Admit(job pulse.Job) error {
	switch job.Kind {
	case pulse.KindFSPut, pulse.KindFSGet, pulse.KindFSStat, pulse.KindFSRemove:
		var body struct {
			Path string `json:"path"`
		}
		if err := json.Unmarshal(job.Payload, &body); err != nil {
			return fmt.Errorf("pulseagent: fs payload: %w", err)
		}
		return p.CheckPath(body.Path)
	case pulse.KindExecRun:
		var body pulse.ExecRun
		if err := json.Unmarshal(job.Payload, &body); err != nil {
			return fmt.Errorf("pulseagent: exec payload: %w", err)
		}
		if len(body.Blobs) > 0 {
			return fmt.Errorf("pulseagent: exec.run blobs are not supported")
		}
		if len(body.Argv) == 0 {
			return fmt.Errorf("pulseagent: exec.run argv is empty")
		}
		return p.CheckBin(body.Argv[0])
	case pulse.KindAgentInfo:
		var body pulse.AgentInfo
		if err := json.Unmarshal(job.Payload, &body); err != nil {
			return fmt.Errorf("pulseagent: agent.info payload: %w", err)
		}
		for _, bin := range body.Binaries {
			if filepath.IsAbs(bin) {
				if err := p.CheckBin(bin); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Tighten returns the permissions that are in both policies.
//
// A pushed entry the local policy does not already allow is dropped. An
// empty pushed list means the server expressed no opinion and the local
// list stands. WorkspaceRoot and RunAs are never taken from a push: they
// describe the machine, which the server does not get to redefine.
func Tighten(local, pushed Policy) Policy {
	out := local
	if len(pushed.AllowedKinds) > 0 {
		out.AllowedKinds = intersectKinds(local.AllowedKinds, pushed.AllowedKinds)
	}
	if len(pushed.AllowedBins) > 0 {
		out.AllowedBins = intersectExact(local.AllowedBins, pushed.AllowedBins)
	}
	if len(pushed.AllowedPaths) > 0 {
		out.AllowedPaths = intersectExact(local.AllowedPaths, pushed.AllowedPaths)
	}
	if len(pushed.AllowedResolvers) > 0 {
		out.AllowedResolvers = intersectExact(local.AllowedResolvers, pushed.AllowedResolvers)
	}
	return out
}

// ParsePolicySection extracts a tightening push. present is false when the
// payload has no policy section, which is not an error.
func ParsePolicySection(payload []byte) (pushed Policy, present bool, err error) {
	if len(payload) == 0 {
		return Policy{}, false, nil
	}
	var sections map[string]json.RawMessage
	if err := json.Unmarshal(payload, &sections); err != nil {
		return Policy{}, false, fmt.Errorf("pulseagent: config payload is not a JSON object: %w", err)
	}
	raw, ok := sections[policyPayloadKey]
	if !ok {
		return Policy{}, false, nil
	}
	var sec policySection
	if err := json.Unmarshal(raw, &sec); err != nil {
		return Policy{}, true, fmt.Errorf("pulseagent: policy section: %w", err)
	}
	return Policy{
		AllowedKinds:     sec.AllowedKinds,
		AllowedBins:      sec.AllowedBins,
		AllowedPaths:     sec.AllowedPaths,
		AllowedResolvers: sec.AllowedResolvers,
	}, true, nil
}

func kindAllowed(patterns []string, kind string) bool {
	for _, pattern := range patterns {
		if pattern == kind {
			return true
		}
		if family, ok := strings.CutSuffix(pattern, ".*"); ok && family != "" {
			if strings.HasPrefix(kind, family+".") {
				return true
			}
		}
	}
	return false
}

// intersectKinds keeps a pushed pattern only when it cannot authorize
// anything the local patterns do not already authorize.
func intersectKinds(local, pushed []string) []string {
	out := make([]string, 0, len(pushed))
	for _, pattern := range pushed {
		if patternWithin(local, pattern) {
			out = append(out, pattern)
		}
	}
	return out
}

func patternWithin(local []string, pattern string) bool {
	if strings.HasSuffix(pattern, ".*") {
		for _, l := range local {
			if l == pattern {
				return true
			}
		}
		return false
	}
	return kindAllowed(local, pattern)
}

func intersectExact(local, pushed []string) []string {
	allow := make(map[string]bool, len(local))
	for _, s := range local {
		allow[filepath.Clean(s)] = true
		allow[s] = true
	}
	out := make([]string, 0, len(pushed))
	for _, s := range pushed {
		if allow[s] || allow[filepath.Clean(s)] {
			out = append(out, s)
		}
	}
	return out
}

func isShell(path string) bool {
	base := filepath.Base(path)
	switch base {
	case "sh", "bash", "dash", "zsh", "ksh", "csh", "tcsh", "fish":
		return true
	default:
		return false
	}
}

func copyExecutors(in Executors) Executors {
	out := make(Executors, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// previewPolicy decides what a push would do to the local floor without
// installing it. have is false when this agent has no policy at all.
//
// A payload with no policy section restores the local floor: the server
// sends its whole config every time, so absence is "no opinion", not "keep
// the previous tightening".
func (a *Agent) previewPolicy(payload []byte) (Policy, bool, error) {
	if a.localPolicy == nil {
		return Policy{}, false, nil
	}
	pushed, present, err := ParsePolicySection(payload)
	if err != nil {
		return Policy{}, false, err
	}
	if !present {
		return *a.localPolicy, true, nil
	}
	if err := pushed.Validate(); err != nil {
		return Policy{}, false, err
	}
	return Tighten(*a.localPolicy, pushed), true, nil
}

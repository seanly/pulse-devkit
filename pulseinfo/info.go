// Package pulseinfo is the agent.info primitive.
//
// It reports what the machine itself can see: operating system, addresses,
// which package manager is installed. It does not decide whether that host
// counts as production. Labels carry intent; this carries facts.
package pulseinfo

import (
	"context"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/seanly/pulse-devkit/pulse"
	"github.com/seanly/pulse-devkit/pulseagent"
	"github.com/seanly/pulse-devkit/pulsecodec"
)

// BinChecker decides whether a resolved executable may be run.
//
// Looking a name up is not executing it. Running --version is, so that step
// goes through the same allowlist as exec.run. Otherwise agent.info would be
// a way to run anything by asking for its version.
type BinChecker interface {
	CheckBin(path string) error
}

// Options configures Collect. Zero values read the real machine.
type Options struct {
	Policy   func() BinChecker
	LookPath func(string) (string, error)
	// Version runs path with --version and returns its combined output.
	// Nil means the real command, bounded by ctx.
	Version func(ctx context.Context, path string) (string, error)
	ReadOS  func() (osName, distro, version string)
	Now     func() time.Time
}

// Collector gathers host facts.
type Collector struct{ opts Options }

// New returns a collector with the real machine behind any unset option.
func New(o Options) *Collector {
	if o.LookPath == nil {
		o.LookPath = exec.LookPath
	}
	if o.Version == nil {
		o.Version = runVersion
	}
	if o.ReadOS == nil {
		o.ReadOS = readOS
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return &Collector{opts: o}
}

// Collect reports the host. Binaries and mounts are answered only when asked.
func (c *Collector) Collect(ctx context.Context, q pulse.AgentInfo) (pulse.AgentFacts, error) {
	host, _ := os.Hostname()
	osName, distro, version := c.opts.ReadOS()
	facts := pulse.AgentFacts{
		Hostname:       host,
		OS:             osName,
		Distro:         distro,
		Version:        version,
		Arch:           runtime.GOARCH,
		Kernel:         kernel(),
		Addresses:      addresses(),
		PackageManager: packageManager(c.opts.LookPath),
		CollectedAt:    c.opts.Now().UTC(),
	}
	if len(q.Binaries) > 0 {
	facts.Binaries = c.binaries(ctx, q.Binaries)
	}
	if len(q.Mounts) > 0 {
		facts.Disk = disks(q.Mounts)
	}
	return facts, nil
}

func (c *Collector) binaries(ctx context.Context, names []string) map[string]pulse.BinaryFact {
	out := make(map[string]pulse.BinaryFact, len(names))
	for _, name := range names {
		path := name
		if !strings.Contains(name, "/") {
			found, err := c.opts.LookPath(name)
			if err != nil {
				out[name] = pulse.BinaryFact{}
				continue
			}
			path = found
		}
		fact := pulse.BinaryFact{Path: path}
		// No checker, or a checker that refuses: report the path and do not
		// execute. Running --version is what the allowlist exists to gate.
		if c.opts.Policy == nil {
			out[name] = fact
			continue
		}
		if err := c.opts.Policy().CheckBin(path); err != nil {
			out[name] = fact
			continue
		}
		ver, err := c.opts.Version(ctx, path)
		if err == nil {
			fact.Version = firstLine(ver)
		}
		out[name] = fact
	}
	return out
}

func runVersion(ctx context.Context, path string) (string, error) {
	cmd := exec.CommandContext(ctx, path, "--version")
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 200 {
		s = s[:200]
	}
	return strings.TrimSpace(s)
}

func packageManager(look func(string) (string, error)) string {
	for _, name := range []string{"apk", "apt", "dnf", "yum"} {
		if _, err := look(name); err == nil {
			return name
		}
	}
	return ""
}

func addresses() []string {
	ifaces, err := net.InterfaceAddrs()
	if err != nil {
		return nil
	}
	var out []string
	for _, a := range ifaces {
		s := a.String()
		if strings.HasPrefix(s, "127.") || s == "::1/128" {
			continue
		}
		out = append(out, s)
	}
	return out
}

func kernel() string {
	out, err := exec.Command("uname", "-r").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func readOS() (osName, distro, version string) {
	osName = runtime.GOOS
	b, err := os.ReadFile("/etc/os-release")
	if err != nil {
		return osName, "", ""
	}
	vals := map[string]string{}
	for _, line := range strings.Split(string(b), "\n") {
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		vals[k] = strings.Trim(v, `"`)
	}
	id := vals["ID"]
	ver := vals["VERSION_ID"]
	if id == "" {
		id = vals["NAME"]
	}
	return osName, id, ver
}

// Executors registers agent.info.
func Executors(o Options) pulseagent.Executors {
	c := New(o)
	return pulseagent.Executors{
		pulse.KindAgentInfo: pulsecodec.Typed(pulsecodec.JSON[pulse.AgentInfo, pulse.AgentFacts](),
			func(ctx context.Context, _ pulse.Job, q pulse.AgentInfo) (pulse.AgentFacts, error) {
				return c.Collect(ctx, q)
			}),
	}
}

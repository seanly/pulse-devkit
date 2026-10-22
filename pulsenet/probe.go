// Package pulsenet is the net.* primitive family.
//
// net.tlsprobe resolves a name, handshakes TLS with every address it got back,
// and reports the certificates it saw. It does not decide what those facts
// mean. A handshake that presents an expired certificate has not failed: the
// certificate is the thing the probe exists to collect, so verification
// happens after the handshake, on the certificates that were collected, and
// the result of that verification is reported as a fact.
package pulsenet

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/seanly/pulse-devkit/pulse"
	"github.com/seanly/pulse-devkit/pulseagent"
	"github.com/seanly/pulse-devkit/pulseagent/pulseguard"
	"github.com/seanly/pulse-devkit/pulsecodec"

	"crypto/tls"
	"crypto/x509"
)

// resolver is the slice of net.Resolver a probe needs. Tests substitute a
// fake so DNS outcomes are deterministic and offline.
type resolver interface {
	LookupHost(ctx context.Context, host string) ([]string, error)
}

// Options configures New.
type Options struct {
	// Resolver resolves names when a job does not name its own servers.
	// Nil means the system resolver.
	Resolver     resolver
	ResolverName string

	// Roots anchors chain verification. Nil means the system roots.
	// The answer is reported as a fact; it is not a verdict.
	Roots *x509.CertPool

	// Guard is read on every probe, after resolution and before any
	// connection. Nil means no address policy. A Live guard is passed as
	// `func() *pulseguard.Guard { return live.Guard() }` so a pushed update
	// applies to the next probe rather than the one already in flight.
	Guard func() *pulseguard.Guard

	// AllowResolvers decides whether a job may name its own DNS servers.
	// Nil refuses every non-empty list. A server that can aim an agent at an
	// arbitrary resolver can aim it at an arbitrary address, and the agent is
	// the last place that can say no.
	AllowResolvers func(resolvers []string) error

	Logger *slog.Logger
}

// Prober runs net.tlsprobe jobs.
type Prober struct {
	opts Options
	log  *slog.Logger
}

// New assembles a prober.
func New(o Options) *Prober {
	if o.Resolver == nil {
		o.Resolver = net.DefaultResolver
	}
	if o.ResolverName == "" {
		o.ResolverName = "system"
	}
	log := o.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Prober{opts: o, log: log}
}

// Probe observes one name. The context deadline bounds the whole probe,
// including DNS.
func (p *Prober) Probe(ctx context.Context, job pulse.NetTLSProbe) *pulse.TLSObservation {
	start := time.Now()
	obs := &pulse.TLSObservation{
		Host:     job.Host,
		Resolver: p.opts.ResolverName,
		ProbedAt: start,
	}
	defer func() {
		obs.Timing.TotalMs = ms(time.Since(start))
	}()

	resv, name, err := p.resolverFor(job)
	if err != nil {
		obs.ErrorCode = pulse.CodeResolversDenied
		obs.ErrorDetail = err.Error()
		return obs
	}
	obs.Resolver = name

	dnsStart := time.Now()
	ips, err := resv.LookupHost(ctx, job.Host)
	obs.Timing.DNSMs = ms(time.Since(dnsStart))
	if err != nil {
		obs.ErrorCode = classifyDNSError(err)
		obs.ErrorDetail = fmt.Sprintf("resolve %s: %v", job.Host, err)
		return obs
	}
	if len(ips) == 0 {
		obs.ErrorCode = pulse.CodeDNSServfail
		obs.ErrorDetail = fmt.Sprintf("resolve %s: no addresses", job.Host)
		return obs
	}
	obs.ResolvedIPs = ips

	// Address rules can only be enforced here. The dispatcher saw a name;
	// the addresses exist only after this lookup. Check the full set even
	// when the job asks to dial just one of them: a name that also resolves
	// into a denied range is still that name.
	if p.opts.Guard != nil {
		if g := p.opts.Guard(); g != nil {
			addrs := make([]net.IP, 0, len(ips))
			for _, ip := range ips {
				if parsed := net.ParseIP(ip); parsed != nil {
					addrs = append(addrs, parsed)
				}
			}
			if err := g.CheckIPs(addrs); err != nil {
				obs.Refused = true
				obs.RefuseDetail = err.Error()
				p.log.Info("probe refused after resolution", "host", job.Host, "error", err)
				return obs
			}
		}
	}

	dial := ips
	if !job.ProbeAll() && len(dial) > 1 {
		dial = dial[:1]
	}

	var (
		summary *pulse.TLSCert
		tlsInfo *pulse.TLSHandshake
		now     = time.Now()
	)
	for _, ip := range dial {
		addr, cert, hs, tcpMs, tlsMs := p.probeIP(ctx, ip, job, now)
		obs.Addresses = append(obs.Addresses, addr)
		obs.Timing.TCPMs = max(obs.Timing.TCPMs, tcpMs)
		obs.Timing.TLSMs = max(obs.Timing.TLSMs, tlsMs)
		if cert != nil && (summary == nil || cert.NotAfter.Before(summary.NotAfter)) {
			summary, tlsInfo, obs.ObservedIP = cert, hs, ip
		}
	}
	obs.Cert, obs.TLS = summary, tlsInfo
	return obs
}

func (p *Prober) resolverFor(job pulse.NetTLSProbe) (resolver, string, error) {
	if len(job.Resolvers) == 0 {
		return p.opts.Resolver, p.opts.ResolverName, nil
	}
	if p.opts.AllowResolvers == nil {
		return nil, "", fmt.Errorf("custom resolvers are not allowed")
	}
	if err := p.opts.AllowResolvers(job.Resolvers); err != nil {
		return nil, "", err
	}
	return resolverDialing(job.Resolvers), strings.Join(job.Resolvers, ","), nil
}

// resolverDialing asks the named servers instead of the system resolver.
// The first server is the one contacted; a list is a preference, not a
// fan-out, because fanning a query out to every listed address is itself
// a way to use the agent as a scanner.
func resolverDialing(servers []string) *net.Resolver {
	server := servers[0]
	if _, _, err := net.SplitHostPort(server); err != nil {
		server = net.JoinHostPort(server, "53")
	}
	return &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "udp", server)
		},
	}
}

func (p *Prober) probeIP(ctx context.Context, ip string, job pulse.NetTLSProbe, now time.Time) (pulse.TLSAddress, *pulse.TLSCert, *pulse.TLSHandshake, int, int) {
	addr := pulse.TLSAddress{IP: ip}

	tcpStart := time.Now()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort(ip, strconv.Itoa(job.Port)))
	tcpMs := ms(time.Since(tcpStart))
	if err != nil {
		addr.ErrorCode = classifyTCPError(err)
		addr.ErrorDetail = err.Error()
		return addr, nil, nil, tcpMs, 0
	}
	defer conn.Close()

	serverName := job.SNI
	if serverName == "" {
		serverName = job.Host
	}
	tlsStart := time.Now()
	tc := tls.Client(conn, &tls.Config{
		ServerName: serverName,
		// The handshake must succeed for a broken certificate too, or there
		// is nothing to report. Verification happens afterwards, on the
		// certificate that was collected.
		InsecureSkipVerify: true,
		// Speak back to TLS 1.0 on purpose. A backend stuck on an old version
		// is a fact worth recording, and refusing it here would hide it.
		MinVersion: tls.VersionTLS10,
	})
	hsErr := tc.HandshakeContext(ctx)
	tlsMs := ms(time.Since(tlsStart))
	if hsErr != nil {
		code, detail := classifyHandshakeError(hsErr)
		addr.ErrorCode, addr.ErrorDetail = code, detail
		return addr, nil, nil, tcpMs, tlsMs
	}

	state := tc.ConnectionState()
	leaf := state.PeerCertificates[0]
	// Hostname is checked against the name that was resolved, not the SNI.
	// SNI is what the server was asked to present; Host is what the caller
	// claims the certificate should cover.
	seen := inspect(leaf, &state, job.Host, now, p.opts.Roots)
	addr.Handshaked = true
	addr.NotBefore = seen.notBefore
	addr.NotAfter = seen.notAfter
	addr.DaysRemaining = seen.days
	addr.ChainTrusted = seen.chainTrusted
	addr.HostnameError = seen.hostnameError
	addr.SelfSigned = seen.selfSigned
	addr.ChainError = seen.chainError
	addr.SubjectCN = seen.subjectCN
	return addr, seen.cert, seen.handshake, tcpMs, tlsMs
}

func ms(d time.Duration) int { return int(d.Milliseconds()) }

// Codec is the JSON encoding of a net.tlsprobe job and its observation.
func Codec() pulsecodec.Codec[pulse.NetTLSProbe, *pulse.TLSObservation] {
	return pulsecodec.JSON[pulse.NetTLSProbe, *pulse.TLSObservation]()
}

// Executor runs probes for pulse.KindNetTLSProbe.
//
// The handler returns the observation and a nil error even when DNS failed
// or the certificate is expired. Those are observations. An error return
// would make the framework record an executor failure, which is a different
// event: the probe did not run. What the observation means is decided by
// whoever receives it.
func Executor(o Options) pulseagent.Executor {
	p := New(o)
	return pulsecodec.Typed(Codec(), func(ctx context.Context, _ pulse.Job, payload pulse.NetTLSProbe) (*pulse.TLSObservation, error) {
		return p.Probe(ctx, payload), nil
	})
}

// Executors is the single-entry map an agent runtime takes.
func Executors(o Options) pulseagent.Executors {
	return pulseagent.Executors{pulse.KindNetTLSProbe: Executor(o)}
}

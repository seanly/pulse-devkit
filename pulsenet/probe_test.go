package pulsenet

import (
	"context"
	"encoding/json"
	"net"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/seanly/pulse-devkit/pulse"
	"github.com/seanly/pulse-devkit/pulseagent/pulseguard"
)

func TestMain(m *testing.M) {
	// A probe that leaves a goroutine behind (a hung handshake, an
	// uncancelled dial) leaks once per target per interval. goleak turns
	// that from a slow bleed into a test failure.
	goleak.VerifyTestMain(m)
}

// fakeResolver pins DNS: outcomes are deterministic and offline, and the
// multi-IP scenarios can point one name at several test listeners.
type fakeResolver struct {
	ips   []string
	err   error
	delay time.Duration
	calls int
}

func (f *fakeResolver) LookupHost(context.Context, string) ([]string, error) {
	f.calls++
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	return f.ips, f.err
}

func runProbe(t *testing.T, opts Options, fqdn, addr, sni string, timeout time.Duration) *pulse.TLSObservation {
	t.Helper()
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split %s: %v", addr, err)
	}
	portNum, _ := strconv.Atoi(port)
	if opts.Resolver == nil {
		opts.Resolver = &fakeResolver{ips: []string{"127.0.0.1"}}
	}
	p := New(opts)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return p.Probe(ctx, pulse.NetTLSProbe{Host: fqdn, Port: portNum, SNI: sni})
}

func TestProbeHealthyChain(t *testing.T) {
	ca := makeCA(t, "Test Root CA")
	leaf := makeLeaf(t, "good.example.com", []string{"good.example.com"},
		time.Now().Add(-time.Hour), time.Now().Add(30*24*time.Hour), ca)
	addr := serveTLS(t, leaf)

	obs := runProbe(t, Options{Roots: certPoolOf(ca.cert)}, "good.example.com", addr, "", 5*time.Second)

	if obs.ErrorCode != "" || obs.Refused {
		t.Fatalf("probe-level error %s refused %v; detail %s", obs.ErrorCode, obs.Refused, obs.ErrorDetail)
	}
	if obs.Cert == nil || !obs.Cert.ChainTrusted {
		t.Fatalf("chain_trusted = %v, want a trusted collected certificate", obs.Cert)
	}
	if obs.Cert.SubjectCN != "good.example.com" || obs.Cert.IssuerCN != "Test Root CA" {
		t.Errorf("cert cn/issuer = %q/%q", obs.Cert.SubjectCN, obs.Cert.IssuerCN)
	}
	if len(obs.Cert.SANs) != 1 || obs.Cert.SANs[0] != "good.example.com" {
		t.Errorf("sans = %v", obs.Cert.SANs)
	}
	if obs.Cert.FingerprintSHA256 == "" || obs.Cert.SerialNumber == "" || obs.Cert.KeyAlgorithm == "" {
		t.Errorf("fingerprint/serial/key algorithm not collected: %+v", obs.Cert)
	}
	if obs.Cert.ChainLength != 1 {
		t.Errorf("chain length = %d, want 1 (leaf only)", obs.Cert.ChainLength)
	}
	// 30 days minus an hour of skew floors to 29.
	if got := obs.Cert.DaysRemaining; got != 29 {
		t.Errorf("days_remaining = %d, want 29", got)
	}
	if ips := obs.ResolvedIPs; len(ips) != 1 || ips[0] != "127.0.0.1" {
		t.Errorf("resolved = %v", ips)
	}
	if obs.ObservedIP != "127.0.0.1" {
		t.Errorf("observed_ip = %q", obs.ObservedIP)
	}
	if obs.TLS == nil || obs.TLS.Version == "" {
		t.Error("tls handshake facts missing — negotiated version and suite are findings")
	}
	if len(obs.Addresses) != 1 || !obs.Addresses[0].Handshaked || obs.Addresses[0].HostnameError != "" {
		t.Errorf("address = %+v, want a completed handshake with a matching name", obs.Addresses)
	}
	// The observation carries facts, not a verdict. warn_days is not a field
	// an application could trip over.
	raw, err := json.Marshal(obs)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), `"warn_days"`) || strings.Contains(string(raw), `"status"`) {
		t.Errorf("observation carries a verdict field: %s", raw)
	}
}

func TestProbeExpiredCertStillCollected(t *testing.T) {
	// The reason the handshake does not verify: an expired certificate must
	// still report its full shape, not just "tls failed".
	ca := makeCA(t, "Test Root CA")
	leaf := makeLeaf(t, "expired.example.com", []string{"expired.example.com"},
		time.Now().Add(-10*24*time.Hour), time.Now().Add(-(72*time.Hour + 2*time.Hour)), ca)
	addr := serveTLS(t, leaf)

	obs := runProbe(t, Options{Roots: certPoolOf(ca.cert)}, "expired.example.com", addr, "", 5*time.Second)

	if obs.ErrorCode != "" {
		t.Fatalf("transport error %s, want the handshake to complete; detail %s", obs.ErrorCode, obs.ErrorDetail)
	}
	if obs.Cert == nil || obs.Cert.DaysRemaining >= 0 {
		t.Fatalf("days_remaining = %v, want a collected expired certificate", obs.Cert)
	}
	if obs.Cert.SubjectCN == "" || obs.Cert.IssuerCN == "" || obs.Cert.FingerprintSHA256 == "" || obs.Cert.SerialNumber == "" {
		t.Errorf("expired certificate not fully collected: %+v", obs.Cert)
	}
	if obs.Cert.NotAfter.After(time.Now()) || len(obs.Cert.SANs) != 1 {
		t.Errorf("validity/sans not collected: %+v", obs.Cert)
	}
	if len(obs.Addresses) != 1 || !obs.Addresses[0].Handshaked || obs.Addresses[0].ErrorCode != "" {
		t.Errorf("address = %+v, want a completed handshake, not a transport failure", obs.Addresses)
	}
	if obs.Addresses[0].DaysRemaining >= 0 {
		t.Errorf("per-address days = %d, want negative", obs.Addresses[0].DaysRemaining)
	}
}

func TestProbeNotYetValid(t *testing.T) {
	ca := makeCA(t, "Test Root CA")
	leaf := makeLeaf(t, "future.example.com", []string{"future.example.com"},
		time.Now().Add(24*time.Hour), time.Now().Add(10*24*time.Hour), ca)
	addr := serveTLS(t, leaf)

	obs := runProbe(t, Options{Roots: certPoolOf(ca.cert)}, "future.example.com", addr, "", 5*time.Second)

	if obs.Cert == nil || !obs.Cert.NotBefore.After(time.Now()) {
		t.Fatalf("not_before = %v, want a collected not-yet-valid certificate", obs.Cert)
	}
	if len(obs.Addresses) != 1 || !obs.Addresses[0].Handshaked {
		t.Fatalf("address = %+v, want the handshake to have completed", obs.Addresses)
	}
}

func TestProbeSelfSignedFact(t *testing.T) {
	self := selfSignedLeaf(t, "self.example.com", []string{"self.example.com"},
		time.Now().Add(-time.Hour), time.Now().Add(30*24*time.Hour))
	addr := serveTLS(t, self)

	other := makeCA(t, "Somewhere Else CA")
	obs := runProbe(t, Options{Roots: certPoolOf(other.cert)}, "self.example.com", addr, "", 5*time.Second)

	if len(obs.Addresses) != 1 || !obs.Addresses[0].SelfSigned {
		t.Fatalf("self_signed = %+v, want the leaf recorded as vouching for itself", obs.Addresses)
	}
	if obs.Addresses[0].ChainTrusted {
		t.Error("chain_trusted = true, want false against an unrelated root")
	}
}

func TestProbeHostnameMismatchFact(t *testing.T) {
	ca := makeCA(t, "Test Root CA")
	leaf := makeLeaf(t, "other.example.com", []string{"other.example.com"},
		time.Now().Add(-time.Hour), time.Now().Add(30*24*time.Hour), ca)
	addr := serveTLS(t, leaf)

	obs := runProbe(t, Options{Roots: certPoolOf(ca.cert)}, "good.example.com", addr, "", 5*time.Second)

	if len(obs.Addresses) != 1 || obs.Addresses[0].HostnameError == "" {
		t.Fatalf("hostname_error = %+v, want the name mismatch recorded", obs.Addresses)
	}
	// The chain itself is fine; the name is a separate fact.
	if !obs.Addresses[0].ChainTrusted {
		t.Error("chain_trusted = false, want true — the issuer is trusted, only the name is wrong")
	}
}

func TestProbeChainIncomplete(t *testing.T) {
	root := makeCA(t, "Test Root CA")
	inter := issueIntermediate(t, root, "Test Intermediate CA")
	leaf := makeLeaf(t, "chained.example.com", []string{"chained.example.com"},
		time.Now().Add(-time.Hour), time.Now().Add(30*24*time.Hour), inter)

	t.Run("missing intermediate", func(t *testing.T) {
		addr := serveTLS(t, leaf)
		obs := runProbe(t, Options{Roots: certPoolOf(root.cert)}, "chained.example.com", addr, "", 5*time.Second)
		if obs.Cert == nil || obs.Cert.ChainTrusted {
			t.Fatalf("chain_trusted = %v, want false when the intermediate is absent", obs.Cert)
		}
		if len(obs.Addresses) != 1 || obs.Addresses[0].ChainError == "" {
			t.Error("chain error text not recorded")
		}
	})

	t.Run("with intermediate served", func(t *testing.T) {
		addr := serveTLS(t, leaf, inter)
		obs := runProbe(t, Options{Roots: certPoolOf(root.cert)}, "chained.example.com", addr, "", 5*time.Second)
		if obs.Cert == nil || !obs.Cert.ChainTrusted || obs.Cert.ChainLength != 2 {
			t.Fatalf("chain trusted/length = %+v, want trusted with length 2", obs.Cert)
		}
	})
}

func TestProbeChainDetailCollected(t *testing.T) {
	root := makeCA(t, "Test Root CA")
	inter := issueIntermediate(t, root, "Test Intermediate CA")
	leaf := makeLeaf(t, "detail.example.com", []string{"detail.example.com", "alt.example.com"},
		time.Now().Add(-time.Hour), time.Now().Add(30*24*time.Hour), inter)

	addr := serveTLS(t, leaf, inter)
	obs := runProbe(t, Options{Roots: certPoolOf(root.cert)}, "detail.example.com", addr, "", 5*time.Second)
	if obs.Cert == nil {
		t.Fatal("no certificate collected")
	}
	chain := obs.Cert.Chain
	if len(chain) != 2 {
		t.Fatalf("chain entries = %d, want 2 (leaf + intermediate)", len(chain))
	}
	if chain[0].Position != 0 || chain[0].SubjectCN != "detail.example.com" {
		t.Errorf("chain[0] = pos %d cn %q, want the leaf first", chain[0].Position, chain[0].SubjectCN)
	}
	if chain[0].FingerprintSHA256 != obs.Cert.FingerprintSHA256 {
		t.Error("chain[0] fingerprint differs from the summary — they describe the same certificate")
	}
	if len(chain[0].SANs) != 2 {
		t.Errorf("leaf SANs = %v, want both names", chain[0].SANs)
	}
	if !slices.Equal(obs.Cert.KeyUsages, []string{"Digital Signature"}) || !slices.Equal(obs.Cert.ExtKeyUsages, []string{"TLS Web Server Authentication"}) {
		t.Errorf("leaf usages = %v / %v, want Digital Signature / TLS Web Server Authentication", obs.Cert.KeyUsages, obs.Cert.ExtKeyUsages)
	}
	if chain[1].CrossSigned {
		t.Error("issuing intermediate marked as a cross-certificate")
	}
	if !slices.Equal(chain[0].KeyUsages, obs.Cert.KeyUsages) || !slices.Equal(chain[0].ExtKeyUsages, obs.Cert.ExtKeyUsages) {
		t.Error("chain leaf usages differ from the summary")
	}
	if !slices.Equal(chain[1].KeyUsages, []string{"Digital Signature", "Certificate Sign"}) {
		t.Errorf("intermediate key usages = %v, want Digital Signature, Certificate Sign", chain[1].KeyUsages)
	}
	if len(chain[1].ExtKeyUsages) != 0 {
		t.Errorf("intermediate EKU = %v, want none", chain[1].ExtKeyUsages)
	}
	if len(chain[1].SANs) != 0 {
		t.Errorf("intermediate SANs = %v, want none carried", chain[1].SANs)
	}
	if chain[1].SubjectCN != "Test Intermediate CA" || !chain[1].IsCA {
		t.Errorf("chain[1] = %q is_ca=%v, want the intermediate CA", chain[1].SubjectCN, chain[1].IsCA)
	}
	if chain[1].IssuerCN != "Test Root CA" {
		t.Errorf("chain[1] issuer = %q, want the root", chain[1].IssuerCN)
	}
	for _, c := range chain {
		if c.SelfSigned {
			t.Errorf("chain[%d] self_signed, but the root was not presented", c.Position)
		}
	}
	for _, c := range chain {
		if c.KeyBits == 0 || c.SignatureAlgorithm == "" {
			t.Errorf("chain[%d]: key_bits = %d signature = %q, want both populated",
				c.Position, c.KeyBits, c.SignatureAlgorithm)
		}
		if c.NotAfter.IsZero() || c.NotBefore.IsZero() || c.SerialNumber == "" {
			t.Errorf("chain[%d]: validity/serial incomplete", c.Position)
		}
	}
	if chain[0].DaysRemaining != 29 {
		t.Errorf("leaf days_remaining = %d, want 29", chain[0].DaysRemaining)
	}
	if chain[1].DaysRemaining != 0 {
		t.Errorf("intermediate days_remaining = %d, want 0 (expires in 24h)", chain[1].DaysRemaining)
	}
}

func TestProbeChainMarksSelfSigned(t *testing.T) {
	leaf := selfSignedLeaf(t, "self.example.com", []string{"self.example.com"},
		time.Now().Add(-time.Hour), time.Now().Add(30*24*time.Hour))
	addr := serveTLS(t, leaf)
	obs := runProbe(t, Options{Roots: certPoolOf()}, "self.example.com", addr, "", 5*time.Second)

	if obs.Cert == nil || len(obs.Cert.Chain) != 1 {
		t.Fatalf("chain = %+v, want 1 entry", obs.Cert)
	}
	if !obs.Cert.Chain[0].SelfSigned {
		t.Error("self_signed = false on a certificate that issued itself")
	}
}

func TestProbeMultipleIPsSummaryIsSoonest(t *testing.T) {
	ca := makeCA(t, "Test Root CA")
	fresh := makeLeaf(t, "multi.example.com", []string{"multi.example.com"},
		time.Now().Add(-time.Hour), time.Now().Add(30*24*time.Hour), ca)
	stale := makeLeaf(t, "multi.example.com", []string{"multi.example.com"},
		time.Now().Add(-10*24*time.Hour), time.Now().Add(-24*time.Hour), ca)

	portProbe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := strconv.Itoa(portProbe.Addr().(*net.TCPAddr).Port)
	portProbe.Close()
	serveTLSOn(t, "127.0.0.1:"+port, stale)
	serveTLSOn(t, "[::1]:"+port, fresh)

	obs := runProbe(t, Options{
		Roots:    certPoolOf(ca.cert),
		Resolver: &fakeResolver{ips: []string{"127.0.0.1", "::1"}},
	}, "multi.example.com", "127.0.0.1:"+port, "", 5*time.Second)

	if got := len(obs.Addresses); got != 2 {
		t.Fatalf("addresses = %d, want 2", got)
	}
	if obs.Addresses[0].IP != "127.0.0.1" || obs.Addresses[0].DaysRemaining >= 0 {
		t.Errorf("addresses[0] = %+v, want the stale backend", obs.Addresses[0])
	}
	if obs.Addresses[1].IP != "::1" || obs.Addresses[1].DaysRemaining < 20 {
		t.Errorf("addresses[1] = %+v, want the fresh backend", obs.Addresses[1])
	}
	if obs.ObservedIP != "127.0.0.1" {
		t.Errorf("observed_ip = %q, want the soonest-expiring backend 127.0.0.1", obs.ObservedIP)
	}
	if obs.Cert == nil || obs.Cert.DaysRemaining >= 0 {
		t.Errorf("summary days = %v, want negative (stale backend)", obs.Cert)
	}
}

func TestProbeCheckIPsAfterResolution(t *testing.T) {
	ca := makeCA(t, "Test Root CA")
	leaf := makeLeaf(t, "blocked.example.com", []string{"blocked.example.com"},
		time.Now().Add(-time.Hour), time.Now().Add(30*24*time.Hour), ca)
	addr := serveTLS(t, leaf)

	// The default guard refuses loopback. The probe resolves to 127.0.0.1
	// and must stop there, before the handshake.
	g, err := pulseguard.New(pulseguard.Options{})
	if err != nil {
		t.Fatal(err)
	}
	obs := runProbe(t, Options{
		Roots: certPoolOf(ca.cert),
		Guard: func() *pulseguard.Guard { return g },
	}, "blocked.example.com", addr, "", 5*time.Second)

	if !obs.Refused || obs.RefuseDetail == "" {
		t.Fatalf("refused = %v detail %q, want a policy refusal after DNS", obs.Refused, obs.RefuseDetail)
	}
	if len(obs.Addresses) != 0 {
		t.Errorf("deny rule hit, but %d addresses were still probed", len(obs.Addresses))
	}
	if len(obs.ResolvedIPs) != 1 {
		t.Error("resolved addresses not reported with the refusal")
	}
}

func TestProbeDNSErrorCodes(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nxdomain", &net.DNSError{Err: "no such host", IsNotFound: true}, pulse.CodeDNSNXDomain},
		{"timeout", &net.DNSError{Err: "i/o timeout", IsTimeout: true}, pulse.CodeDNSTimeout},
		{"servfail", &net.DNSError{Err: "server misbehaving"}, pulse.CodeDNSServfail},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obs := runProbe(t, Options{Resolver: &fakeResolver{err: tc.err}},
				"gone.example.com", "127.0.0.1:1", "", time.Second)
			if obs.ErrorCode != tc.want {
				t.Fatalf("dns error %v → %s, want %s (%s)", tc.err, obs.ErrorCode, tc.want, obs.ErrorDetail)
			}
		})
	}
}

func TestProbeTCPRefused(t *testing.T) {
	obs := runProbe(t, Options{}, "refused.example.com", freeLoopbackPort(t), "", 2*time.Second)
	if len(obs.Addresses) != 1 || obs.Addresses[0].ErrorCode != pulse.CodeTCPRefused {
		t.Fatalf("address = %+v, want tcp_refused", obs.Addresses)
	}
}

func TestProbeTCPTimeout(t *testing.T) {
	ca := makeCA(t, "Test Root CA")
	addr := serveTLS(t, makeLeaf(t, "slow.example.com", []string{"slow.example.com"},
		time.Now().Add(-time.Hour), time.Now().Add(30*24*time.Hour), ca))
	obs := runProbe(t, Options{}, "slow.example.com", addr, "", time.Nanosecond)
	if len(obs.Addresses) != 1 || obs.Addresses[0].ErrorCode != pulse.CodeTCPTimeout {
		t.Fatalf("address = %+v, want tcp_timeout", obs.Addresses)
	}
}

func TestProbeHandshakeTimeout(t *testing.T) {
	obs := runProbe(t, Options{}, "blackhole.example.com", blackholeTLS(t), "", 300*time.Millisecond)
	if len(obs.Addresses) != 1 || obs.Addresses[0].ErrorCode != pulse.CodeTLSHandshake {
		t.Fatalf("address = %+v, want tls_handshake_failed", obs.Addresses)
	}
}

func TestProbeTimingRecorded(t *testing.T) {
	ca := makeCA(t, "Test Root CA")
	addr := serveTLS(t, makeLeaf(t, "timed.example.com", []string{"timed.example.com"},
		time.Now().Add(-time.Hour), time.Now().Add(30*24*time.Hour), ca))

	obs := runProbe(t, Options{Resolver: &fakeResolver{delay: 30 * time.Millisecond}},
		"timed.example.com", addr, "", 5*time.Second)

	if obs.Timing.DNSMs < 25 {
		t.Errorf("dns_ms = %d, want the resolver's 30ms delay to show up", obs.Timing.DNSMs)
	}
	if obs.Timing.TCPMs < 0 || obs.Timing.TLSMs < 0 {
		t.Errorf("tcp/tls timings = %d/%d, want recorded", obs.Timing.TCPMs, obs.Timing.TLSMs)
	}
	if obs.Timing.TotalMs < obs.Timing.DNSMs {
		t.Errorf("total_ms = %d < dns_ms = %d", obs.Timing.TotalMs, obs.Timing.DNSMs)
	}
}

func TestProbeCustomResolversRefused(t *testing.T) {
	fake := &fakeResolver{ips: []string{"127.0.0.1"}}
	p := New(Options{Resolver: fake})
	obs := p.Probe(context.Background(), pulse.NetTLSProbe{
		Host: "example.com", Port: 443, Resolvers: []string{"192.0.2.53"},
	})
	if obs.ErrorCode != pulse.CodeResolversDenied {
		t.Fatalf("error = %s, want %s (%s)", obs.ErrorCode, pulse.CodeResolversDenied, obs.ErrorDetail)
	}
	if fake.calls != 0 {
		t.Errorf("system resolver was consulted %d times; a refused list must not be used", fake.calls)
	}
	if len(obs.Addresses) != 0 {
		t.Errorf("dialed %d addresses after refusing resolvers", len(obs.Addresses))
	}
}

func TestProbeAllAddressesDefaultsToEveryAddress(t *testing.T) {
	ca := makeCA(t, "Test Root CA")
	leaf := makeLeaf(t, "one.example.com", []string{"one.example.com"},
		time.Now().Add(-time.Hour), time.Now().Add(30*24*time.Hour), ca)

	portProbe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := strconv.Itoa(portProbe.Addr().(*net.TCPAddr).Port)
	portProbe.Close()
	serveTLSOn(t, "127.0.0.1:"+port, leaf)
	serveTLSOn(t, "[::1]:"+port, leaf)

	noFlag := runProbe(t, Options{Resolver: &fakeResolver{ips: []string{"127.0.0.1", "::1"}}, Roots: certPoolOf(ca.cert)},
		"one.example.com", "127.0.0.1:"+port, "", 5*time.Second)
	if len(noFlag.Addresses) != 2 {
		t.Fatalf("default addresses = %d, want both", len(noFlag.Addresses))
	}

	onlyFirst := false
	p := New(Options{Resolver: &fakeResolver{ips: []string{"127.0.0.1", "::1"}}, Roots: certPoolOf(ca.cert)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	obs := p.Probe(ctx, pulse.NetTLSProbe{
		Host: "one.example.com", Port: atoi(port), AllAddresses: &onlyFirst,
	})
	if len(obs.Addresses) != 1 || obs.Addresses[0].IP != "127.0.0.1" {
		t.Fatalf("all_addresses=false probed %+v, want only the first address", obs.Addresses)
	}
	if len(obs.ResolvedIPs) != 2 {
		t.Errorf("resolved = %v, want both addresses recorded even when only one is dialed", obs.ResolvedIPs)
	}
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

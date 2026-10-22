package pulseguard_test

import (
	"errors"
	"net"
	"testing"

	"github.com/seanly/pulse-devkit/pulseagent/pulseguard"
)

func TestDeniesByCIDR(t *testing.T) {
	g, err := pulseguard.New(pulseguard.Options{
		Local: pulseguard.Policy{DenyCIDRs: []string{"10.0.0.0/8", "192.168.1.0/24"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	denied := []string{"10.1.2.3", "192.168.1.50"}
	for _, ip := range denied {
		if err := g.CheckIPs([]net.IP{net.ParseIP(ip)}); err == nil {
			t.Errorf("%s should be denied", ip)
		} else if !errors.Is(err, pulseguard.ErrDenied) {
			t.Errorf("%s: error does not wrap ErrDenied: %v", ip, err)
		}
	}
	if err := g.CheckIPs([]net.IP{net.ParseIP("203.0.113.5")}); err != nil {
		t.Errorf("203.0.113.5 should be allowed: %v", err)
	}
}

func TestDeniesByDomainGlob(t *testing.T) {
	g, err := pulseguard.New(pulseguard.Options{
		Local: pulseguard.Policy{DenyDomains: []string{"*.internal", "secret.example.com"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, host := range []string{"db.internal", "secret.example.com", "DB.INTERNAL"} {
		if err := g.CheckTarget(host, 443); err == nil {
			t.Errorf("%s should be denied", host)
		}
	}
	if err := g.CheckTarget("www.example.com", 443); err != nil {
		t.Errorf("www.example.com should be allowed: %v", err)
	}
}

func TestLoopbackDeniedByDefault(t *testing.T) {
	g, err := pulseguard.New(pulseguard.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, ip := range []string{"127.0.0.1", "::1"} {
		if err := g.CheckIPs([]net.IP{net.ParseIP(ip)}); err == nil {
			t.Errorf("%s should be denied without AllowLoopback", ip)
		}
	}

	allowed, err := pulseguard.New(pulseguard.Options{AllowLoopback: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := allowed.CheckIPs([]net.IP{net.ParseIP("127.0.0.1")}); err != nil {
		t.Errorf("loopback should be allowed when AllowLoopback is set: %v", err)
	}
}

func TestAlwaysDeniedAddresses(t *testing.T) {
	// These are never legitimate targets, so an operator should not have to
	// remember to list them.
	g, err := pulseguard.New(pulseguard.Options{AllowLoopback: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, ip := range []string{"0.0.0.0", "::", "ff02::1"} {
		if err := g.CheckIPs([]net.IP{net.ParseIP(ip)}); err == nil {
			t.Errorf("%s should always be denied", ip)
		}
	}
}

func TestRejectsInvalidPorts(t *testing.T) {
	g, err := pulseguard.New(pulseguard.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, port := range []int{0, -1, 70000} {
		if err := g.CheckTarget("example.com", port); err == nil {
			t.Errorf("port %d should be rejected", port)
		}
	}
}

func TestLiteralAddressInHostIsChecked(t *testing.T) {
	// A denied range must not be reachable by writing the address in the host
	// position instead of a name.
	g, err := pulseguard.New(pulseguard.Options{
		Local: pulseguard.Policy{DenyCIDRs: []string{"10.0.0.0/8"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.CheckTarget("10.1.2.3", 443); err == nil {
		t.Error("a literal address in a denied range should be refused by CheckTarget")
	}
}

func TestUnparseableRuleIsAStartupError(t *testing.T) {
	// A deny rule that silently failed to compile is worse than a loud
	// failure: the operator believes they are protected when they are not.
	if _, err := pulseguard.New(pulseguard.Options{
		Local: pulseguard.Policy{DenyCIDRs: []string{"not-a-cidr"}},
	}); err == nil {
		t.Error("want an error for an unparseable CIDR")
	}
	if _, err := pulseguard.New(pulseguard.Options{
		Local: pulseguard.Policy{DenyDomains: []string{"  "}},
	}); err == nil {
		t.Error("want an error for an empty deny domain")
	}
}

func TestPushedEntriesAddToLocalAndNeverRemoveThem(t *testing.T) {
	local := pulseguard.Options{
		Local: pulseguard.Policy{DenyCIDRs: []string{"10.0.0.0/8"}, DenyDomains: []string{"*.internal"}},
	}
	// A push that mentions neither local entry must not drop them: the local
	// list is the protection against a server-side mistake, so a push can only
	// ever narrow where the agent may go.
	g, err := pulseguard.Merge(local, pulseguard.Policy{
		DenyCIDRs: []string{"172.16.0.0/12"}, DenyDomains: []string{"*.staging"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, ip := range []string{"10.1.1.1", "172.16.5.5"} {
		if err := g.CheckIPs([]net.IP{net.ParseIP(ip)}); err == nil {
			t.Errorf("%s should be denied after the merge", ip)
		}
	}
	for _, host := range []string{"db.internal", "web.staging"} {
		if err := g.CheckTarget(host, 443); err == nil {
			t.Errorf("%s should be denied after the merge", host)
		}
	}
}

func TestPushCannotRelaxLoopback(t *testing.T) {
	// There is deliberately no field for this in Policy, so the only thing to
	// assert is that merging preserves the local decision either way.
	strict, err := pulseguard.Merge(pulseguard.Options{AllowLoopback: false}, pulseguard.Policy{})
	if err != nil {
		t.Fatal(err)
	}
	if err := strict.CheckIPs([]net.IP{net.ParseIP("127.0.0.1")}); err == nil {
		t.Error("a push must not be able to permit loopback")
	}
}

func TestPayloadRoundTripAndCoexistence(t *testing.T) {
	// The payload is a JSON object keyed by subsystem, so an application's own
	// configuration and the guard's can share it without knowing about each
	// other.
	withApp := []byte(`{"myapp":{"widget":"frobnicate"}}`)
	encoded, err := pulseguard.EncodePayload(withApp, pulseguard.Policy{
		DenyCIDRs: []string{"10.0.0.0/8"},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := pulseguard.ParsePayload(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.DenyCIDRs) != 1 || got.DenyCIDRs[0] != "10.0.0.0/8" {
		t.Errorf("round trip lost the policy: %+v", got)
	}
	// The application's section survived.
	if !containsSubstring(string(encoded), "frobnicate") {
		t.Errorf("encoding dropped the application's section: %s", encoded)
	}
}

func TestAbsentSectionIsNotAnError(t *testing.T) {
	// "The server pushed nothing for me" is normal, not a failure.
	p, err := pulseguard.ParsePayload([]byte(`{"myapp":{}}`))
	if err != nil {
		t.Errorf("an absent section should not error: %v", err)
	}
	if len(p.DenyCIDRs) != 0 || len(p.DenyDomains) != 0 {
		t.Errorf("want an empty policy, got %+v", p)
	}
	if p, err := pulseguard.ParsePayload(nil); err != nil || len(p.DenyCIDRs) != 0 {
		t.Errorf("an empty payload should yield an empty policy: %v %+v", err, p)
	}
}

func TestLiveSwapsPolicyOnPush(t *testing.T) {
	live, err := pulseguard.NewLive(pulseguard.Options{
		Local: pulseguard.Policy{DenyCIDRs: []string{"10.0.0.0/8"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := live.Guard().CheckIPs([]net.IP{net.ParseIP("172.16.1.1")}); err != nil {
		t.Fatalf("172.16.1.1 should be allowed before the push: %v", err)
	}

	payload, err := pulseguard.EncodePayload(nil, pulseguard.Policy{DenyCIDRs: []string{"172.16.0.0/12"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := live.ConfigPayloadHandler()(payload); err != nil {
		t.Fatal(err)
	}
	if err := live.Guard().CheckIPs([]net.IP{net.ParseIP("172.16.1.1")}); err == nil {
		t.Error("172.16.1.1 should be denied after the push")
	}
	// The local entry is still there.
	if err := live.Guard().CheckIPs([]net.IP{net.ParseIP("10.1.1.1")}); err == nil {
		t.Error("the local entry was dropped by the push")
	}
}

func TestBadPushedPolicyIsRejectedAndKeepsPrevious(t *testing.T) {
	live, err := pulseguard.NewLive(pulseguard.Options{
		Local: pulseguard.Policy{DenyCIDRs: []string{"10.0.0.0/8"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := pulseguard.EncodePayload(nil, pulseguard.Policy{DenyCIDRs: []string{"garbage"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := live.ConfigPayloadHandler()(payload); err == nil {
		t.Fatal("want an error for an uncompilable pushed policy")
	}
	// Rejecting the push must leave the previous policy intact, not an empty
	// one — which would be the worst possible outcome for a deny list.
	if err := live.Guard().CheckIPs([]net.IP{net.ParseIP("10.1.1.1")}); err == nil {
		t.Error("the previous policy was lost when a bad push was rejected")
	}
}

func containsSubstring(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (func() bool {
		for i := 0; i+len(needle) <= len(haystack); i++ {
			if haystack[i:i+len(needle)] == needle {
				return true
			}
		}
		return false
	})()
}

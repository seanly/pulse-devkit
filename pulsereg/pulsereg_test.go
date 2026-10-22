package pulsereg_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/seanly/pulse-devkit/pulse"
	"github.com/seanly/pulse-devkit/pulsereg"
	"github.com/seanly/pulse-devkit/pulsetest"
)

func newService(t *testing.T, mutate func(*pulsereg.Options)) (*pulsereg.Service, *pulsetest.Store, *pulsetest.Clock) {
	t.Helper()
	clock := pulsetest.NewClock(time.Time{})
	store := pulsetest.NewStore(pulsetest.WithClock(clock))
	o := pulsereg.Options{
		Agents:    store,
		Tokens:    store,
		Configs:   store,
		MasterKey: []byte("master-key-for-tests"),
		Clock:     clock,
	}
	if mutate != nil {
		mutate(&o)
	}
	svc, err := pulsereg.New(o)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return svc, store, clock
}

// mintToken creates a usable bootstrap token and returns its plaintext.
func mintToken(t *testing.T, svc *pulsereg.Service, labels map[string]string) string {
	t.Helper()
	plain, _, err := svc.NewBootstrapToken(context.Background(), pulsereg.NewBootstrapTokenInput{
		Labels: labels, Description: "test", CreatedBy: "tester",
	})
	if err != nil {
		t.Fatalf("NewBootstrapToken: %v", err)
	}
	return plain
}

func TestRegisterIssuesVerifiableCredential(t *testing.T) {
	svc, _, _ := newService(t, nil)
	ctx := context.Background()
	token := mintToken(t, svc, nil)

	res, err := svc.Register(ctx, pulsereg.RegisterInput{
		BootstrapToken: token, Name: "probe-1", Version: "1.0.0", PeerIP: "198.51.100.7",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if res.AgentID == "" || res.Credential == "" {
		t.Fatalf("incomplete result: %+v", res)
	}

	agent, err := svc.VerifyCredential(ctx, res.Credential)
	if err != nil {
		t.Fatalf("VerifyCredential: %v", err)
	}
	if agent.ID != res.AgentID {
		t.Errorf("credential resolved to %q, want %q", agent.ID, res.AgentID)
	}
	if agent.ObservedIP != "198.51.100.7" {
		t.Errorf("observed IP = %q, want the peer address", agent.ObservedIP)
	}
}

func TestTokenIsSingleUse(t *testing.T) {
	svc, _, _ := newService(t, nil)
	ctx := context.Background()
	token := mintToken(t, svc, nil)

	if _, err := svc.Register(ctx, pulsereg.RegisterInput{BootstrapToken: token, Name: "a"}); err != nil {
		t.Fatalf("first registration: %v", err)
	}
	_, err := svc.Register(ctx, pulsereg.RegisterInput{BootstrapToken: token, Name: "b"})
	if !errors.Is(err, pulse.ErrInvalidBootstrap) {
		t.Errorf("second use error = %v, want ErrInvalidBootstrap", err)
	}
}

func TestUnknownUsedAndExpiredTokensAreIndistinguishable(t *testing.T) {
	svc, _, clock := newService(t, nil)
	ctx := context.Background()

	used := mintToken(t, svc, nil)
	if _, err := svc.Register(ctx, pulsereg.RegisterInput{BootstrapToken: used, Name: "a"}); err != nil {
		t.Fatal(err)
	}

	expiring, _, err := svc.NewBootstrapToken(ctx, pulsereg.NewBootstrapTokenInput{TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Minute)

	// An attacker probing tokens must not learn which case they hit, so all
	// three produce the same error.
	cases := map[string]string{
		"unknown": "not-a-real-token",
		"used":    used,
		"expired": expiring,
	}
	var messages []string
	for name, tok := range cases {
		_, err := svc.Register(ctx, pulsereg.RegisterInput{BootstrapToken: tok, Name: "x"})
		if !errors.Is(err, pulse.ErrInvalidBootstrap) {
			t.Errorf("%s: error = %v, want ErrInvalidBootstrap", name, err)
		}
		messages = append(messages, err.Error())
	}
	for i := 1; i < len(messages); i++ {
		if messages[i] != messages[0] {
			t.Errorf("error messages differ between cases: %q vs %q", messages[0], messages[i])
		}
	}
}

func TestSameNameReusesTheSameRow(t *testing.T) {
	svc, store, _ := newService(t, nil)
	ctx := context.Background()

	first, err := svc.Register(ctx, pulsereg.RegisterInput{
		BootstrapToken: mintToken(t, svc, nil), Name: "probe-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	// Losing the credential file and re-enrolling with a fresh token must not
	// split the agent's history in two.
	second, err := svc.Register(ctx, pulsereg.RegisterInput{
		BootstrapToken: mintToken(t, svc, nil), Name: "probe-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.AgentID != second.AgentID {
		t.Errorf("re-registration created a new row: %q then %q", first.AgentID, second.AgentID)
	}
	agents, err := store.ListAgents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(agents) != 1 {
		t.Errorf("want 1 agent row, got %d", len(agents))
	}
}

func TestDisabledAgentCannotReRegister(t *testing.T) {
	svc, store, _ := newService(t, nil)
	ctx := context.Background()

	res, err := svc.Register(ctx, pulsereg.RegisterInput{
		BootstrapToken: mintToken(t, svc, nil), Name: "probe-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetAgentStatus(ctx, res.AgentID, pulse.AgentDisabled); err != nil {
		t.Fatal(err)
	}

	// Re-enrolling must not quietly undo an operator's decision.
	_, err = svc.Register(ctx, pulsereg.RegisterInput{
		BootstrapToken: mintToken(t, svc, nil), Name: "probe-1",
	})
	if !errors.Is(err, pulsereg.ErrAgentDisabled) {
		t.Errorf("error = %v, want ErrAgentDisabled", err)
	}
}

func TestBadTokenLeavesNoAgentRow(t *testing.T) {
	svc, store, _ := newService(t, nil)
	ctx := context.Background()

	_, err := svc.Register(ctx, pulsereg.RegisterInput{
		BootstrapToken: "bogus", Name: "probe-1",
	})
	if err == nil {
		t.Fatal("want an error for a bogus token")
	}
	// Enrollment is atomic: a failed claim must not leave half an agent row.
	agents, err := store.ListAgents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(agents) != 0 {
		t.Errorf("a failed enrollment left %d agent rows behind", len(agents))
	}
}

func TestReportedLabelsAreReplacedButOverridesSurvive(t *testing.T) {
	svc, store, _ := newService(t, nil)
	ctx := context.Background()

	res, err := svc.Register(ctx, pulsereg.RegisterInput{
		BootstrapToken: mintToken(t, svc, nil), Name: "probe-1",
		Labels: map[string]string{"region": "east", "tier": "gold"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// An operator overrides one label and tombstones another.
	gone := (*string)(nil)
	override := "west"
	if err := store.SetLabelOverrides(ctx, res.AgentID, map[string]*string{
		"region": &override,
		"tier":   gone,
	}); err != nil {
		t.Fatal(err)
	}

	// The agent re-registers, dropping "tier" from its own config and adding
	// "zone". Reported labels are replaced wholesale.
	res2, err := svc.Register(ctx, pulsereg.RegisterInput{
		BootstrapToken: mintToken(t, svc, nil), Name: "probe-1",
		Labels: map[string]string{"region": "east", "zone": "b"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// The override still wins, and the tombstone still suppresses.
	if got := res2.EffectiveLabels["region"]; got != "west" {
		t.Errorf("region = %q, want the operator's west to survive re-registration", got)
	}
	if _, present := res2.EffectiveLabels["tier"]; present {
		t.Error("a tombstoned label came back on re-registration")
	}
	if got := res2.EffectiveLabels["zone"]; got != "b" {
		t.Errorf("zone = %q, want the newly reported b", got)
	}

	agent, err := store.GetAgent(ctx, res.AgentID)
	if err != nil {
		t.Fatal(err)
	}
	// The dropped key is gone from the reported set too: what the agent sends
	// is its whole current claim.
	if _, present := agent.ReportedLabels["tier"]; present {
		t.Error("reported labels were merged rather than replaced")
	}
}

func TestTokenPresetLabelsJoinTheReportedSet(t *testing.T) {
	svc, _, _ := newService(t, nil)
	ctx := context.Background()
	token := mintToken(t, svc, map[string]string{"fleet": "edge"})

	res, err := svc.Register(ctx, pulsereg.RegisterInput{
		BootstrapToken: token, Name: "probe-1", Labels: map[string]string{"region": "east"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := res.EffectiveLabels["fleet"]; got != "edge" {
		t.Errorf("fleet = %q, want the token's preset", got)
	}
	if got := res.EffectiveLabels["region"]; got != "east" {
		t.Errorf("region = %q, want the agent's own label", got)
	}
}

func TestRevocationInvalidatesExistingCredentials(t *testing.T) {
	svc, _, _ := newService(t, nil)
	ctx := context.Background()

	res, err := svc.Register(ctx, pulsereg.RegisterInput{
		BootstrapToken: mintToken(t, svc, nil), Name: "probe-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.VerifyCredential(ctx, res.Credential); err != nil {
		t.Fatalf("credential should verify before revocation: %v", err)
	}

	if err := svc.RevokeAgent(ctx, res.AgentID); err != nil {
		t.Fatal(err)
	}
	// A valid signature is not authorisation: a credential from before
	// revocation must fail as hard as a forged one.
	_, err = svc.VerifyCredential(ctx, res.Credential)
	if !errors.Is(err, pulsereg.ErrCredentialRevoked) {
		t.Errorf("error = %v, want ErrCredentialRevoked", err)
	}
}

func TestDeletedAgentCredentialStopsWorking(t *testing.T) {
	svc, store, _ := newService(t, nil)
	ctx := context.Background()

	res, err := svc.Register(ctx, pulsereg.RegisterInput{
		BootstrapToken: mintToken(t, svc, nil), Name: "probe-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteAgent(ctx, res.AgentID); err != nil {
		t.Fatal(err)
	}
	_, err = svc.VerifyCredential(ctx, res.Credential)
	if !errors.Is(err, pulsereg.ErrInvalidCredential) {
		t.Errorf("error = %v, want ErrInvalidCredential for a deleted agent", err)
	}
}

func TestDisabledAgentCredentialIsRefused(t *testing.T) {
	svc, store, _ := newService(t, nil)
	ctx := context.Background()

	res, err := svc.Register(ctx, pulsereg.RegisterInput{
		BootstrapToken: mintToken(t, svc, nil), Name: "probe-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetAgentStatus(ctx, res.AgentID, pulse.AgentDisabled); err != nil {
		t.Fatal(err)
	}
	_, err = svc.VerifyCredential(ctx, res.Credential)
	if !errors.Is(err, pulsereg.ErrAgentDisabled) {
		t.Errorf("error = %v, want ErrAgentDisabled", err)
	}
}

func TestTamperedCredentialsAreRejected(t *testing.T) {
	svc, _, _ := newService(t, nil)
	ctx := context.Background()
	res, err := svc.Register(ctx, pulsereg.RegisterInput{
		BootstrapToken: mintToken(t, svc, nil), Name: "probe-1",
	})
	if err != nil {
		t.Fatal(err)
	}

	parts := strings.Split(res.Credential, ".")
	if len(parts) != 3 {
		t.Fatalf("unexpected credential shape: %q", res.Credential)
	}
	bad := map[string]string{
		"flipped signature byte": parts[0] + "." + parts[1] + "." + flipFirst(parts[2]),
		"flipped payload byte":   parts[0] + "." + flipFirst(parts[1]) + "." + parts[2],
		"no signature":           parts[0] + "." + parts[1],
		"empty":                  "",
		"garbage":                "not-a-credential",
		"wrong version":          "v9." + parts[1] + "." + parts[2],
	}
	for name, cred := range bad {
		if _, err := svc.VerifyCredential(ctx, cred); !errors.Is(err, pulsereg.ErrInvalidCredential) {
			t.Errorf("%s: error = %v, want ErrInvalidCredential", name, err)
		}
	}
}

func TestCredentialFromAnotherMasterKeyIsRejected(t *testing.T) {
	svc, store, clock := newService(t, nil)
	ctx := context.Background()
	res, err := svc.Register(ctx, pulsereg.RegisterInput{
		BootstrapToken: mintToken(t, svc, nil), Name: "probe-1",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Rotating the master key invalidates every credential, which is the
	// property that makes one secret enough for an operator to remember.
	rotated, err := pulsereg.New(pulsereg.Options{
		Agents: store, Tokens: store, Configs: store,
		MasterKey: []byte("a-different-master-key"), Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rotated.VerifyCredential(ctx, res.Credential); !errors.Is(err, pulsereg.ErrInvalidCredential) {
		t.Errorf("error = %v, want ErrInvalidCredential after key rotation", err)
	}
}

func TestBootstrapTokenPlaintextIsNotStored(t *testing.T) {
	svc, store, _ := newService(t, nil)
	ctx := context.Background()
	plain := mintToken(t, svc, nil)

	tokens, err := store.ListBootstrapTokens(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 1 {
		t.Fatalf("want 1 token, got %d", len(tokens))
	}
	// Only the hash persists, so a leaked database cannot enroll new agents.
	if tokens[0].TokenHash == plain {
		t.Error("the token is stored in plaintext")
	}
	if tokens[0].TokenHash != pulsereg.HashToken(plain) {
		t.Error("stored hash does not match the plaintext's hash")
	}
}

func TestInvalidRegistrationInput(t *testing.T) {
	svc, _, _ := newService(t, nil)
	ctx := context.Background()

	cases := []struct {
		name string
		in   pulsereg.RegisterInput
	}{
		{"no name", pulsereg.RegisterInput{BootstrapToken: "t"}},
		{"no token", pulsereg.RegisterInput{Name: "probe-1"}},
		{"name too long", pulsereg.RegisterInput{BootstrapToken: "t", Name: strings.Repeat("a", 65)}},
		{"name with spaces", pulsereg.RegisterInput{BootstrapToken: "t", Name: "probe one"}},
		{"name with slash", pulsereg.RegisterInput{BootstrapToken: "t", Name: "probe/one"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := svc.Register(ctx, tc.in); err == nil {
				t.Error("want an error")
			}
		})
	}
}

func TestTooManyLabelsRejected(t *testing.T) {
	svc, _, _ := newService(t, nil)
	ctx := context.Background()
	labels := map[string]string{}
	for i := 0; i < 40; i++ {
		labels[string(rune('a'+i%26))+strings.Repeat("x", i)] = "v"
	}
	_, err := svc.Register(ctx, pulsereg.RegisterInput{
		BootstrapToken: mintToken(t, svc, nil), Name: "probe-1", Labels: labels,
	})
	if !errors.Is(err, pulsereg.ErrInvalidInput) {
		t.Errorf("error = %v, want ErrInvalidInput", err)
	}
}

func TestOversizedLabelValueRejected(t *testing.T) {
	svc, _, _ := newService(t, nil)
	ctx := context.Background()
	_, err := svc.Register(ctx, pulsereg.RegisterInput{
		BootstrapToken: mintToken(t, svc, nil), Name: "probe-1",
		Labels: map[string]string{"k": strings.Repeat("v", 600)},
	})
	if !errors.Is(err, pulsereg.ErrInvalidInput) {
		t.Errorf("error = %v, want ErrInvalidInput", err)
	}
}

func TestNewRequiresStoresAndKey(t *testing.T) {
	store := pulsetest.NewStore()
	cases := map[string]pulsereg.Options{
		"no agents": {Tokens: store, MasterKey: []byte("k")},
		"no tokens": {Agents: store, MasterKey: []byte("k")},
		"no key":    {Agents: store, Tokens: store},
	}
	for name, o := range cases {
		if _, err := pulsereg.New(o); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

func flipFirst(s string) string {
	if s == "" {
		return s
	}
	b := []byte(s)
	if b[0] == 'A' {
		b[0] = 'B'
	} else {
		b[0] = 'A'
	}
	return string(b)
}

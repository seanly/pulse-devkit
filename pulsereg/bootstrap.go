package pulsereg

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/seanly/pulse-devkit/pulse"
)

// bootstrapTokenBytes is the entropy in a bootstrap token. 32 bytes is far
// beyond guessable and still fits on one line of a terminal.
const bootstrapTokenBytes = 32

// NewBootstrapTokenInput describes a token to mint.
type NewBootstrapTokenInput struct {
	// Labels are preset on whichever agent enrolls with this token — a claim
	// made on the agent's behalf. Server-side overrides still win over them.
	Labels map[string]string
	// Description is for the operator's own benefit ("rack 4 probes").
	Description string
	// TTL bounds how long the token may be used. Zero means no expiry, which
	// is convenient for automation and worse for security; callers that
	// care should set it.
	TTL time.Duration
	// CreatedBy records who minted it, for the audit trail.
	CreatedBy string
}

// NewBootstrapToken mints a token and stores only its hash.
//
// The plaintext is returned exactly once. It is not recoverable afterwards:
// nothing persists it, which is the property that makes a leaked database
// useless for enrolling new agents.
func (s *Service) NewBootstrapToken(ctx context.Context, in NewBootstrapTokenInput) (plaintext string, t pulse.BootstrapToken, err error) {
	raw := make([]byte, bootstrapTokenBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", t, fmt.Errorf("pulsereg: generate token: %w", err)
	}
	plaintext = base64.RawURLEncoding.EncodeToString(raw)

	t = pulse.BootstrapToken{
		ID:          s.newID(),
		TokenHash:   HashToken(plaintext),
		Labels:      in.Labels,
		Description: strings.TrimSpace(in.Description),
		CreatedAt:   s.now(),
		CreatedBy:   in.CreatedBy,
	}
	if in.TTL > 0 {
		t.ExpiresAt = s.now().Add(in.TTL)
	}
	if err := s.tokens.CreateBootstrapToken(ctx, &t); err != nil {
		return "", t, err
	}
	s.log.Info("bootstrap token created",
		"token_id", t.ID, "expires_at", t.ExpiresAt, "created_by", in.CreatedBy)
	return plaintext, t, nil
}

// ListBootstrapTokens returns the tokens, for a console listing. The plaintext
// is gone; only metadata and the hash remain.
func (s *Service) ListBootstrapTokens(ctx context.Context) ([]pulse.BootstrapToken, error) {
	return s.tokens.ListBootstrapTokens(ctx)
}

// DeleteBootstrapToken removes a token.
func (s *Service) DeleteBootstrapToken(ctx context.Context, id string) error {
	if err := s.tokens.DeleteBootstrapToken(ctx, id); err != nil {
		return err
	}
	s.log.Info("bootstrap token deleted", "token_id", id)
	return nil
}

// HashToken is what a store holds: the token itself is shown to the operator
// once and never persists anywhere.
//
// Exported because a store implementation needs the same function to look a
// token up, and two copies of a hashing rule is one too many.
func HashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// TokenUsable reports whether a stored token may still be claimed. Store
// implementations call it so "unknown", "used" and "expired" are decided the
// same way everywhere.
func TokenUsable(t pulse.BootstrapToken, now time.Time) error {
	if t.ID == "" {
		return pulse.ErrInvalidBootstrap
	}
	if !t.UsedAt.IsZero() {
		return pulse.ErrInvalidBootstrap
	}
	if !t.ExpiresAt.IsZero() && now.After(t.ExpiresAt) {
		return pulse.ErrInvalidBootstrap
	}
	return nil
}

// Errors the registry reports. The gRPC layer maps these onto status codes and
// an HTTP layer onto statuses, so they are part of the contract.
var (
	// ErrInvalidCredential is a credential that does not parse or does not
	// verify.
	ErrInvalidCredential = errors.New("pulsereg: invalid credential")
	// ErrCredentialRevoked is a well-formed credential whose agent has moved
	// on to a newer token version.
	ErrCredentialRevoked = errors.New("pulsereg: credential revoked")
	// ErrAgentDisabled is a valid credential for an operator-disabled agent.
	ErrAgentDisabled = errors.New("pulsereg: agent is disabled")
	// ErrInvalidInput is a rejected registration payload.
	ErrInvalidInput = errors.New("pulsereg: invalid input")
)

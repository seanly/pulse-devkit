// Credential signing. An agent credential is a compact HMAC-signed token
// naming the agent and the revocation counter it was issued against.
//
// Not a JWT: the credential only ever travels between an agent and its own
// server, so JWT's interoperability buys nothing, and the framework's
// dependency budget (see docs/issues/0001) does not stretch to a JWT library
// for an internal token format.
package pulsereg

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// credentialVersion prefixes every token so the format can change later
// without guessing at what an old token means.
const credentialVersion = "v1"

// signingInfo is the HKDF-ish label mixed into the master key. Deriving rather
// than using the master key directly means this key cannot be confused with
// any other key derived from the same master.
//
// Its exact bytes are load-bearing rather than cosmetic: changing them derives
// a different signing key from the same master, which invalidates every
// credential already issued and makes the whole fleet re-enroll. The /v1
// suffix exists so that a future change can be a deliberate version bump
// instead of an accident. The module rename to pulse-devkit changed this
// string while nothing was deployed — free exactly once.
const signingInfo = "pulse-devkit/agent-credential/v1"

// claims is the token's payload.
type claims struct {
	AgentID string `json:"a"`
	// TokenVersion is the agent row's revocation counter at issue time. A
	// credential whose version is behind the row's has been revoked.
	TokenVersion int `json:"v"`
	// IssuedAt is informational: expiry is not how these are retired —
	// revocation is. A long-lived agent should not have to re-enroll on a
	// timer.
	IssuedAt int64 `json:"t"`
}

// signer signs and verifies credentials.
type signer struct {
	key []byte
}

// newSigner derives the signing key from the deployment's master key.
//
// Rotating the master key invalidates every agent credential — the same
// property a credential keyring has, so an operator only has to remember one
// secret.
func newSigner(masterKey []byte) (*signer, error) {
	if len(masterKey) == 0 {
		return nil, errors.New("pulsereg: master key is required")
	}
	mac := hmac.New(sha256.New, masterKey)
	mac.Write([]byte(signingInfo))
	return &signer{key: mac.Sum(nil)}, nil
}

// sign issues a credential for one agent at one revocation version.
func (s *signer) sign(agentID string, tokenVersion int, now time.Time) (string, error) {
	if agentID == "" {
		return "", errors.New("pulsereg: agent id is required")
	}
	payload, err := json.Marshal(claims{
		AgentID: agentID, TokenVersion: tokenVersion, IssuedAt: now.Unix(),
	})
	if err != nil {
		return "", err
	}
	body := credentialVersion + "." + base64.RawURLEncoding.EncodeToString(payload)
	return body + "." + base64.RawURLEncoding.EncodeToString(s.mac(body)), nil
}

// verify checks a credential's signature and returns its claims. It does NOT
// check revocation: that needs the agent row, so it belongs in the Service.
func (s *signer) verify(credential string) (claims, error) {
	var c claims
	parts := strings.Split(credential, ".")
	if len(parts) != 3 {
		return c, errInvalidCredential
	}
	if parts[0] != credentialVersion {
		return c, errInvalidCredential
	}
	body := parts[0] + "." + parts[1]
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return c, errInvalidCredential
	}
	// Constant time: a timing oracle on the signature would let an attacker
	// forge one byte at a time.
	if subtle.ConstantTimeCompare(sig, s.mac(body)) != 1 {
		return c, errInvalidCredential
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return c, errInvalidCredential
	}
	if err := json.Unmarshal(payload, &c); err != nil {
		return c, errInvalidCredential
	}
	if c.AgentID == "" {
		return c, errInvalidCredential
	}
	return c, nil
}

func (s *signer) mac(body string) []byte {
	mac := hmac.New(sha256.New, s.key)
	mac.Write([]byte(body))
	return mac.Sum(nil)
}

var errInvalidCredential = fmt.Errorf("%w", ErrInvalidCredential)

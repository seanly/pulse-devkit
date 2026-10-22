package pulsenet

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"
)

// The tests build their own PKI so every certificate shape — expired, not yet
// valid, hostname-wrong, missing its intermediate — is one template away.

type certKey struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func newKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return k
}

// makeCA issues a self-signed CA.
func makeCA(t *testing.T, cn string) certKey {
	t.Helper()
	k := newKey(t)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn, Organization: []string{"CertPulse Test"}},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &k.PublicKey, k)
	if err != nil {
		t.Fatalf("create CA: %v", err)
	}
	return certKey{mustParseCert(t, der), k}
}

// makeLeaf issues a leaf certificate signed by issuer, covering the given SANs.
func makeLeaf(t *testing.T, cn string, sans []string, notBefore, notAfter time.Time, issuer certKey) certKey {
	t.Helper()
	k := newKey(t)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn, Organization: []string{"CertPulse Test"}},
		DNSNames:     sans,
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, issuer.cert, &k.PublicKey, issuer.key)
	if err != nil {
		t.Fatalf("create leaf: %v", err)
	}
	return certKey{mustParseCert(t, der), k}
}

func mustParseCert(t *testing.T, der []byte) *x509.Certificate {
	t.Helper()
	c, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse cert: %v", err)
	}
	return c
}

// issueIntermediate issues a CA certificate signed by parent — the middle of a
// chain, for incomplete-chain scenarios.
func issueIntermediate(t *testing.T, parent certKey, cn string) certKey {
	t.Helper()
	k := newKey(t)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: cn, Organization: []string{"CertPulse Test"}},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent.cert, &k.PublicKey, parent.key)
	if err != nil {
		t.Fatalf("create intermediate: %v", err)
	}
	return certKey{mustParseCert(t, der), k}
}

// selfSignedLeaf issues a non-CA certificate that signs itself.
func selfSignedLeaf(t *testing.T, cn string, sans []string, notBefore, notAfter time.Time) certKey {
	t.Helper()
	k := newKey(t)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn, Organization: []string{"CertPulse Test"}},
		// The issuer is deliberately identical to the subject.
		DNSNames:    sans,
		NotBefore:   notBefore,
		NotAfter:    notAfter,
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &k.PublicKey, k)
	if err != nil {
		t.Fatalf("create self-signed leaf: %v", err)
	}
	return certKey{mustParseCert(t, der), k}
}

func certPoolOf(certs ...*x509.Certificate) *x509.CertPool {
	pool := x509.NewCertPool()
	for _, c := range certs {
		pool.AddCert(c)
	}
	return pool
}

// serveTLS answers TLS handshakes with the given chain (leaf first, then any
// intermediates) on a fresh loopback listener and returns its address. The
// peer may do anything after the handshake; these servers exist to be probed,
// not to serve.
func serveTLS(t *testing.T, chain ...certKey) string {
	t.Helper()
	return serveTLSOn(t, "127.0.0.1:0", chain...)
}

// serveTLSOn is serveTLS on a caller-chosen address, for the multi-IP case
// where two listeners must share one port.
func serveTLSOn(t *testing.T, addr string, chain ...certKey) string {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen %s: %v", addr, err)
	}
	t.Cleanup(func() { ln.Close() })

	raw := make([][]byte, len(chain))
	for i, c := range chain {
		raw[i] = c.cert.Raw
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{{Certificate: raw, PrivateKey: chain[0].key}}}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				tc := tls.Server(conn, cfg)
				// A handshake the client can complete is all the prober needs;
				// it never sends application data.
				_ = tc.Handshake()
			}()
		}
	}()
	return ln.Addr().String()
}

// blackholeTLS accepts connections and never answers the handshake — the
// shape of a port that is open but broken, for timeout classification.
func blackholeTLS(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				// Hold the connection open in silence until the test ends.
				buf := make([]byte, 1)
				for {
					if _, err := conn.Read(buf); err != nil {
						return
					}
				}
			}()
		}
	}()
	return ln.Addr().String()
}

// freeLoopbackPort finds a port nothing is bound to, for refused-connection
// tests. There is an inherent race between closing and rebinding, but no
// listener means the next dial is all but guaranteed refused.
func freeLoopbackPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

package pulsenet

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"math"
	"net"
	"strings"
	"time"

	"github.com/seanly/pulse-devkit/pulse"
)

// daysRemaining is whole days from now until expiry, floored. Expired
// certificates are negative on purpose, and floor keeps "expired two hours
// ago" at -1 instead of a misleading 0.
func daysRemaining(notAfter, now time.Time) int {
	return int(math.Floor(notAfter.Sub(now).Hours() / 24))
}

// isSelfSigned reports whether the certificate vouches for itself.
// CheckSignature is used directly rather than CheckSignatureFrom, which
// additionally demands a CA key usage a self-signed server leaf deliberately
// does not carry.
func isSelfSigned(leaf *x509.Certificate) bool {
	if !bytes.Equal(leaf.RawSubject, leaf.RawIssuer) {
		return false
	}
	return leaf.CheckSignature(leaf.SignatureAlgorithm, leaf.RawTBSCertificate, leaf.Signature) == nil
}

// inspected is one handshake's certificate, plus the separate facts a later
// verdict needs. Hostname and chain trust are not collapsed: each is its own
// fact, and one must not hide the other.
type inspected struct {
	cert          *pulse.TLSCert
	handshake     *pulse.TLSHandshake
	notBefore     time.Time
	notAfter      time.Time
	days          int
	chainTrusted  bool
	hostnameError string
	selfSigned    bool
	chainError    string
	subjectCN     string
}

func inspect(leaf *x509.Certificate, state *tls.ConnectionState, host string, now time.Time, roots *x509.CertPool) inspected {
	intermediates := x509.NewCertPool()
	for _, c := range state.PeerCertificates[1:] {
		intermediates.AddCert(c)
	}
	verifyOpts := x509.VerifyOptions{Intermediates: intermediates}
	if roots != nil {
		verifyOpts.Roots = roots
	}
	_, chainErr := leaf.Verify(verifyOpts)
	hostErr := leaf.VerifyHostname(host)

	sum := sha256.Sum256(leaf.Raw)
	cert := &pulse.TLSCert{
		SubjectCN:          leaf.Subject.CommonName,
		SubjectOrg:         strings.Join(leaf.Subject.Organization, ", "),
		SANs:               leaf.DNSNames,
		IPSANs:             ipStrings(leaf.IPAddresses),
		IssuerCN:           leaf.Issuer.CommonName,
		IssuerOrg:          strings.Join(leaf.Issuer.Organization, ", "),
		SerialNumber:       leaf.SerialNumber.String(),
		NotBefore:          leaf.NotBefore,
		NotAfter:           leaf.NotAfter,
		DaysRemaining:      daysRemaining(leaf.NotAfter, now),
		FingerprintSHA256:  hex.EncodeToString(sum[:]),
		KeyAlgorithm:       leaf.PublicKeyAlgorithm.String(),
		KeyBits:            keyBits(leaf),
		SignatureAlgorithm: leaf.SignatureAlgorithm.String(),
		KeyUsages:          keyUsageNamesOf(leaf),
		ExtKeyUsages:       extKeyUsageNamesOf(leaf),
		IsCA:               leaf.IsCA,
		ChainLength:        len(state.PeerCertificates),
		ChainTrusted:       chainErr == nil,
		OCSPStapled:        len(state.OCSPResponse) > 0,
		Chain:              chainView(state.PeerCertificates, now),
	}
	if hostErr != nil {
		cert.VerifyError = hostErr.Error()
	} else if chainErr != nil {
		cert.VerifyError = chainErr.Error()
	}

	out := inspected{
		cert:         cert,
		notBefore:    leaf.NotBefore,
		notAfter:     leaf.NotAfter,
		days:         cert.DaysRemaining,
		chainTrusted: chainErr == nil,
		selfSigned:   isSelfSigned(leaf),
		subjectCN:    leaf.Subject.CommonName,
		handshake: &pulse.TLSHandshake{
			Version:     tls.VersionName(state.Version),
			CipherSuite: tls.CipherSuiteName(state.CipherSuite),
			ALPN:        state.NegotiatedProtocol,
		},
	}
	if hostErr != nil {
		out.hostnameError = hostErr.Error()
	}
	if chainErr != nil {
		out.chainError = chainErr.Error()
	}
	return out
}

func chainView(certs []*x509.Certificate, now time.Time) []pulse.TLSChainCert {
	out := make([]pulse.TLSChainCert, 0, len(certs))
	for i, c := range certs {
		sum := sha256.Sum256(c.Raw)
		entry := pulse.TLSChainCert{
			Position:           i,
			SubjectCN:          c.Subject.CommonName,
			SubjectOrg:         strings.Join(c.Subject.Organization, ", "),
			SubjectOU:          strings.Join(c.Subject.OrganizationalUnit, ", "),
			IssuerCN:           c.Issuer.CommonName,
			IssuerOrg:          strings.Join(c.Issuer.Organization, ", "),
			IssuerOU:           strings.Join(c.Issuer.OrganizationalUnit, ", "),
			SerialNumber:       c.SerialNumber.String(),
			NotBefore:          c.NotBefore,
			NotAfter:           c.NotAfter,
			DaysRemaining:      daysRemaining(c.NotAfter, now),
			FingerprintSHA256:  hex.EncodeToString(sum[:]),
			KeyAlgorithm:       c.PublicKeyAlgorithm.String(),
			KeyBits:            keyBits(c),
			SignatureAlgorithm: c.SignatureAlgorithm.String(),
			KeyUsages:          keyUsageNamesOf(c),
			ExtKeyUsages:       extKeyUsageNamesOf(c),
			IsCA:               c.IsCA,
			SelfSigned:         c.Subject.String() == c.Issuer.String(),
			CrossSigned:        crossSigned(c),
		}
		if i == 0 {
			entry.SANs = c.DNSNames
			entry.IPSANs = ipStrings(c.IPAddresses)
		}
		out = append(out, entry)
	}
	return out
}

func keyBits(c *x509.Certificate) int {
	switch pub := c.PublicKey.(type) {
	case *rsa.PublicKey:
		return pub.N.BitLen()
	case *ecdsa.PublicKey:
		return pub.Curve.Params().BitSize
	default:
		return 0
	}
}

func ipStrings(ips []net.IP) []string {
	if len(ips) == 0 {
		return nil
	}
	out := make([]string, 0, len(ips))
	for _, ip := range ips {
		out = append(out, ip.String())
	}
	return out
}

// crossSigned is the stand-in used for the chain panel: a CA that is not
// self-signed, whose subject name contains "root". It is right for the
// cross-signed roots operators actually upload, and wrong outside that shape.
// It is a label on the document, not a trust decision.
func crossSigned(c *x509.Certificate) bool {
	if c == nil || !c.IsCA || c.Subject.String() == c.Issuer.String() {
		return false
	}
	if strings.Contains(strings.ToLower(c.Subject.CommonName), "root") {
		return true
	}
	for _, ou := range c.Subject.OrganizationalUnit {
		if strings.Contains(strings.ToLower(ou), "root") {
			return true
		}
	}
	return false
}

var keyUsageBits = []struct {
	bit  x509.KeyUsage
	name string
}{
	{x509.KeyUsageDigitalSignature, "Digital Signature"},
	{x509.KeyUsageContentCommitment, "Non Repudiation"},
	{x509.KeyUsageKeyEncipherment, "Key Encipherment"},
	{x509.KeyUsageDataEncipherment, "Data Encipherment"},
	{x509.KeyUsageKeyAgreement, "Key Agreement"},
	{x509.KeyUsageCertSign, "Certificate Sign"},
	{x509.KeyUsageCRLSign, "CRL Sign"},
	{x509.KeyUsageEncipherOnly, "Encipher Only"},
	{x509.KeyUsageDecipherOnly, "Decipher Only"},
}

func keyUsageNamesOf(c *x509.Certificate) []string {
	out := make([]string, 0)
	if c == nil {
		return out
	}
	for _, item := range keyUsageBits {
		if c.KeyUsage&item.bit != 0 {
			out = append(out, item.name)
		}
	}
	return out
}

var extKeyUsageNames = map[x509.ExtKeyUsage]string{
	x509.ExtKeyUsageAny:                            "Any Extended Key Usage",
	x509.ExtKeyUsageServerAuth:                     "TLS Web Server Authentication",
	x509.ExtKeyUsageClientAuth:                     "TLS Web Client Authentication",
	x509.ExtKeyUsageCodeSigning:                    "Code Signing",
	x509.ExtKeyUsageEmailProtection:                "E-mail Protection",
	x509.ExtKeyUsageIPSECEndSystem:                 "IPSec End System",
	x509.ExtKeyUsageIPSECTunnel:                    "IPSec Tunnel",
	x509.ExtKeyUsageIPSECUser:                      "IPSec User",
	x509.ExtKeyUsageTimeStamping:                   "Time Stamping",
	x509.ExtKeyUsageOCSPSigning:                    "OCSP Signing",
	x509.ExtKeyUsageMicrosoftServerGatedCrypto:     "Microsoft Server Gated Crypto",
	x509.ExtKeyUsageNetscapeServerGatedCrypto:      "Netscape Server Gated Crypto",
	x509.ExtKeyUsageMicrosoftCommercialCodeSigning: "Microsoft Commercial Code Signing",
	x509.ExtKeyUsageMicrosoftKernelCodeSigning:     "Microsoft Kernel Code Signing",
}

func extKeyUsageNamesOf(c *x509.Certificate) []string {
	out := make([]string, 0)
	if c == nil {
		return out
	}
	for _, u := range c.ExtKeyUsage {
		if name, ok := extKeyUsageNames[u]; ok {
			out = append(out, name)
			continue
		}
		out = append(out, fmt.Sprintf("EKU %d", int(u)))
	}
	for _, oid := range c.UnknownExtKeyUsage {
		out = append(out, oid.String())
	}
	return out
}

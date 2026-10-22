package pulse

import "time"

// Transport error codes for net.tlsprobe. They name what the probe saw —
// the resolver failed, the port refused, the handshake did not complete —
// and carry no opinion about whether that is an alert. An application that
// stores them as a result status is making its own decision.
const (
	CodeDNSNXDomain    = "dns_nxdomain"
	CodeDNSTimeout     = "dns_timeout"
	CodeDNSServfail    = "dns_servfail"
	CodeTCPRefused     = "tcp_refused"
	CodeTCPTimeout     = "tcp_timeout"
	CodeTCPUnreachable = "tcp_unreachable"
	CodeTLSHandshake   = "tls_handshake_failed"
	CodeTLSVersion     = "tls_version_unsupported"

	// CodeResolversDenied is a probe that named its own DNS servers when the
	// agent is not allowed to. A server that can aim an agent at an arbitrary
	// resolver can aim it at an arbitrary address; refusing is the safe default
	// until a local policy says otherwise.
	CodeResolversDenied = "resolvers_denied"
)

// NetTLSProbe asks an agent to observe the TLS certificate served for a name.
//
// It describes the target and nothing else. How many days left counts as a
// warning, and whether an untrusted chain is a problem, are decisions the
// application makes from the observation — two deployments can disagree about
// both and still share this probe.
type NetTLSProbe struct {
	// Host is the name to resolve and, unless SNI is set, to send in the handshake.
	Host string `json:"host"`
	Port int    `json:"port"`
	// SNI overrides the name sent in the handshake. Empty means Host.
	SNI string `json:"sni,omitempty"`

	// AllAddresses probes every address the name resolved to, rather than the
	// first. Nil means yes: one connection lands on one backend, and a stale
	// certificate behind a healthy sibling is the case this probe exists for.
	AllAddresses *bool `json:"all_addresses,omitempty"`

	// Resolvers overrides which DNS servers to ask. Empty means the system
	// resolver. An agent refuses a non-empty list unless its own policy allows
	// it — see CodeResolversDenied.
	Resolvers []string `json:"resolvers,omitempty"`
}

// ProbeAll reports whether every resolved address should be contacted.
// A missing AllAddresses is yes, matching the reason the probe exists.
func (p NetTLSProbe) ProbeAll() bool {
	if p.AllAddresses == nil {
		return true
	}
	return *p.AllAddresses
}

// TLSObservation is what net.tlsprobe reports: facts about one name, from one
// vantage point, at one moment. It has no warning threshold and no verdict.
// "Twenty days left" is here; "twenty days is too few" is not.
type TLSObservation struct {
	// Host is the name that was resolved and checked against the certificate.
	Host string `json:"host,omitempty"`

	// ErrorCode is set when the probe stopped before any address was contacted:
	// DNS failed, or the job named resolvers the agent will not use. Per-address
	// failures live on Addresses instead, because one bad backend is not the
	// same fact as "the name did not resolve".
	ErrorCode   string `json:"error_code,omitempty"`
	ErrorDetail string `json:"error_detail,omitempty"`

	// Refused is set when local policy declined the resolved addresses and
	// nothing was contacted. The agent is the only one who knows that; the
	// application decides how to record it.
	Refused      bool   `json:"refused,omitempty"`
	RefuseDetail string `json:"refuse_detail,omitempty"`

	ResolvedIPs []string `json:"resolved_ips,omitempty"`
	Resolver    string   `json:"resolver,omitempty"`
	// ObservedIP is the address the summary certificate came from.
	ObservedIP string `json:"observed_ip,omitempty"`

	Addresses []TLSAddress `json:"addresses,omitempty"`
	// Cert is the soonest-expiring certificate seen. A monitor exists to catch
	// that one, and it is chosen here because "which certificate expires first"
	// is a fact, not a policy.
	Cert   *TLSCert      `json:"cert,omitempty"`
	TLS    *TLSHandshake `json:"tls,omitempty"`
	Timing TLSTiming     `json:"timing"`

	// ProbedAt is when the probe ran, not when the report was delivered.
	// Verdicts that depend on the clock — expired, not yet valid — must use
	// this, or a report that sat in a queue would be judged against a later day.
	ProbedAt time.Time `json:"probed_at"`
}

// TLSAddress is one resolved address. A transport failure and a collected
// certificate are different shapes: ErrorCode is set only when the handshake
// did not complete, and the certificate facts are meaningful only when
// Handshaked is set.
type TLSAddress struct {
	IP          string `json:"ip"`
	ErrorCode   string `json:"error_code,omitempty"`
	ErrorDetail string `json:"error_detail,omitempty"`

	Handshaked    bool      `json:"handshaked,omitempty"`
	NotBefore     time.Time `json:"not_before,omitzero"`
	NotAfter      time.Time `json:"not_after,omitzero"`
	DaysRemaining int       `json:"days_remaining,omitempty"`
	// ChainTrusted is the verifier's answer against the agent's roots. It is
	// not a verdict: an application may treat an untrusted internal chain as
	// normal.
	ChainTrusted bool `json:"chain_trusted,omitempty"`
	// HostnameError is VerifyHostname's message, empty when the name matched.
	HostnameError string `json:"hostname_error,omitempty"`
	// SelfSigned reports that the leaf vouches for itself. Whether that is a
	// problem is the application's decision; the fact is not.
	SelfSigned bool   `json:"self_signed,omitempty"`
	ChainError string `json:"chain_error,omitempty"`
	SubjectCN  string `json:"subject_cn,omitempty"`
}

// TLSCert is the certificate document collected from one handshake. The field
// names are the stored shape as well as the wire shape: an application can
// keep the document without re-deriving it, and a renamed Go field must not
// silently change either.
type TLSCert struct {
	SubjectCN          string    `json:"subject_cn"`
	SubjectOrg         string    `json:"subject_org,omitempty"`
	SANs               []string  `json:"sans,omitempty"`
	IPSANs             []string  `json:"ip_sans,omitempty"`
	IssuerCN           string    `json:"issuer_cn"`
	IssuerOrg          string    `json:"issuer_org,omitempty"`
	SerialNumber       string    `json:"serial_number"`
	NotBefore          time.Time `json:"not_before"`
	NotAfter           time.Time `json:"not_after"`
	DaysRemaining      int       `json:"days_remaining"`
	FingerprintSHA256  string    `json:"fingerprint_sha256"`
	KeyAlgorithm       string    `json:"key_algorithm"`
	KeyBits            int       `json:"key_bits,omitempty"`
	SignatureAlgorithm string    `json:"signature_algorithm,omitempty"`
	// KeyUsages and ExtKeyUsages are always present on a newly collected
	// certificate. An empty slice means the extension is absent, which is a
	// different fact from a document that never had the field.
	KeyUsages    []string       `json:"key_usages"`
	ExtKeyUsages []string       `json:"ext_key_usages"`
	IsCA         bool           `json:"is_ca"`
	ChainLength  int            `json:"chain_length"`
	ChainTrusted bool           `json:"chain_trusted"`
	VerifyError  string         `json:"verify_error,omitempty"`
	OCSPStapled  bool           `json:"ocsp_stapled"`
	Chain        []TLSChainCert `json:"chain,omitempty"`
}

// TLSChainCert is one certificate the server presented, leaf first.
type TLSChainCert struct {
	Position int `json:"position"`

	SubjectCN  string `json:"subject_cn,omitempty"`
	SubjectOrg string `json:"subject_org,omitempty"`
	SubjectOU  string `json:"subject_ou,omitempty"`
	IssuerCN   string `json:"issuer_cn,omitempty"`
	IssuerOrg  string `json:"issuer_org,omitempty"`
	IssuerOU   string `json:"issuer_ou,omitempty"`

	SerialNumber  string    `json:"serial_number"`
	NotBefore     time.Time `json:"not_before"`
	NotAfter      time.Time `json:"not_after"`
	DaysRemaining int       `json:"days_remaining"`

	FingerprintSHA256  string   `json:"fingerprint_sha256"`
	KeyAlgorithm       string   `json:"key_algorithm"`
	KeyBits            int      `json:"key_bits,omitempty"`
	SignatureAlgorithm string   `json:"signature_algorithm,omitempty"`
	KeyUsages          []string `json:"key_usages"`
	ExtKeyUsages       []string `json:"ext_key_usages"`

	IsCA        bool `json:"is_ca"`
	SelfSigned  bool `json:"self_signed,omitempty"`
	CrossSigned bool `json:"cross_signed,omitempty"`

	SANs   []string `json:"sans,omitempty"`
	IPSANs []string `json:"ip_sans,omitempty"`
}

// TLSHandshake is the version and suite the server actually negotiated.
type TLSHandshake struct {
	Version     string `json:"version"`
	CipherSuite string `json:"cipher_suite"`
	ALPN        string `json:"alpn,omitempty"`
}

// TLSTiming breaks one probe into stages, in milliseconds. DNS, TCP and TLS
// are the slowest address — the worst path a client at this vantage point
// would experience — and total covers the whole probe.
type TLSTiming struct {
	DNSMs   int `json:"dns_ms"`
	TCPMs   int `json:"tcp_ms"`
	TLSMs   int `json:"tls_ms"`
	TotalMs int `json:"total_ms"`
}

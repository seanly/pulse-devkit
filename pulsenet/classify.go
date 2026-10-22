package pulsenet

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"syscall"

	"github.com/seanly/pulse-devkit/pulse"
)

// classifyDNSError maps resolver failures. NXDOMAIN means the name does not
// exist; timeout and servfail are the resolver's problem. The distinction is
// a fact about the failure, which is why it lives with the probe rather than
// with whoever later decides whether to page a human.
func classifyDNSError(err error) string {
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		switch {
		case dnsErr.IsNotFound:
			return pulse.CodeDNSNXDomain
		case dnsErr.IsTimeout:
			return pulse.CodeDNSTimeout
		default:
			return pulse.CodeDNSServfail
		}
	}
	if errors.Is(err, context.DeadlineExceeded) || os.IsTimeout(err) {
		return pulse.CodeDNSTimeout
	}
	return pulse.CodeDNSServfail
}

// classifyTCPError maps connection failures.
func classifyTCPError(err error) string {
	if errors.Is(err, context.DeadlineExceeded) || os.IsTimeout(err) {
		return pulse.CodeTCPTimeout
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case syscall.ECONNREFUSED:
			return pulse.CodeTCPRefused
		case syscall.EHOSTUNREACH, syscall.ENETUNREACH, syscall.ENETDOWN:
			return pulse.CodeTCPUnreachable
		}
	}
	// A TCP reset, an empty reply from a broken middlebox: the host was
	// reached or it was not, and unreachable is the honest default.
	return pulse.CodeTCPUnreachable
}

// classifyHandshakeError splits "no common TLS version" from every other
// handshake failure. The version case is a fact about the server's
// configuration; folding it into a generic handshake error would hide it.
func classifyHandshakeError(err error) (code, detail string) {
	msg := err.Error()
	if strings.Contains(msg, "no supported versions") ||
		strings.Contains(msg, "protocol version") ||
		strings.Contains(msg, "unsupported version") {
		return pulse.CodeTLSVersion, msg
	}
	return pulse.CodeTLSHandshake, msg
}

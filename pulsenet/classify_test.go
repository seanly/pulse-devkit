package pulsenet

import (
	"context"
	"errors"
	"net"
	"os"
	"syscall"
	"testing"

	"github.com/seanly/pulse-devkit/pulse"
)

func TestClassifyHandshakeError(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{errors.New("tls: no supported versions satisfy MinVersion 0x0301"), pulse.CodeTLSVersion},
		{errors.New("tls: server selected unsupported protocol version 300"), pulse.CodeTLSVersion},
		{errors.New("tls: handshake failure"), pulse.CodeTLSHandshake},
		{errors.New("remote error: tls: bad certificate"), pulse.CodeTLSHandshake},
	}
	for _, tc := range cases {
		if code, _ := classifyHandshakeError(tc.err); code != tc.want {
			t.Errorf("classifyHandshakeError(%q) = %s, want %s", tc.err, code, tc.want)
		}
	}
}

func TestClassifyTCPError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"refused", os.NewSyscallError("connect", syscall.ECONNREFUSED), pulse.CodeTCPRefused},
		{"host unreachable", os.NewSyscallError("connect", syscall.EHOSTUNREACH), pulse.CodeTCPUnreachable},
		{"net unreachable", os.NewSyscallError("connect", syscall.ENETUNREACH), pulse.CodeTCPUnreachable},
		{"deadline", &net.OpError{Op: "dial", Err: os.ErrDeadlineExceeded}, pulse.CodeTCPTimeout},
		{"ctx deadline", context.DeadlineExceeded, pulse.CodeTCPTimeout},
		{"anything else", errors.New("weird middlebox"), pulse.CodeTCPUnreachable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyTCPError(tc.err); got != tc.want {
				t.Errorf("classifyTCPError(%v) = %s, want %s", tc.err, got, tc.want)
			}
		})
	}
}

func TestClassifyDNSErrorFallback(t *testing.T) {
	// A non-DNSError timeout still lands on dns_timeout; anything unknown is
	// servfail — the resolver's problem, not the domain's.
	if got := classifyDNSError(context.DeadlineExceeded); got != pulse.CodeDNSTimeout {
		t.Errorf("deadline = %s, want dns_timeout", got)
	}
	if got := classifyDNSError(errors.New("socket went away")); got != pulse.CodeDNSServfail {
		t.Errorf("unknown = %s, want dns_servfail", got)
	}
}

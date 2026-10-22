// Package pulsetest provides fakes for testing against pulse-devkit: an in-memory
// store, a fake agent API, a recording registry and a manual clock.
//
// It exists because the bad paths are the ones worth testing and the hardest
// to reach. A fake that can only succeed is not much use: these can break a
// stream mid-session, reject a credential, fill a send queue and lose a lease
// race. Every downstream project needs those, and without them here each one
// writes its own — inconsistently, and usually without the failure injection.
package pulsetest

import (
	"context"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pulsev1 "github.com/seanly/pulse-devkit/pb/pulse/v1"
	"github.com/seanly/pulse-devkit/pulseagent"
)

// API is a fake pulseagent.API. It records what the agent sent and lets a test
// push messages down as if from a server.
type API struct {
	mu sync.Mutex

	// AgentID and Credential are what Register hands back.
	agentID    string
	credential string
	// effectiveLabels is what Register reports the server stored.
	effectiveLabels map[string]string

	credentialSet string
	registers     []*pulsev1.RegisterRequest
	results       []*pulsev1.ReportResultRequest

	// rejectCredential makes Connect fail Unauthenticated, for testing the
	// re-registration path.
	rejectCredential bool
	// registerErr makes Register fail.
	registerErr error
	// reportErr makes ReportResult fail.
	reportErr error

	streams []*FakeStream
	// onStream is notified with each stream as it is created, so a test can
	// grab one without polling.
	onStream chan *FakeStream
}

// NewAPI returns a fake API that registers agents as "agent-1".
func NewAPI() *API {
	return &API{
		agentID:    "agent-1",
		credential: "credential-1",
		onStream:   make(chan *FakeStream, 8),
	}
}

// WithIdentity sets what Register returns.
func (a *API) WithIdentity(agentID, credential string) *API {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.agentID, a.credential = agentID, credential
	return a
}

// WithEffectiveLabels sets the labels Register reports as stored.
func (a *API) WithEffectiveLabels(l map[string]string) *API {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.effectiveLabels = l
	return a
}

// RejectCredential makes the next Connect fail with Unauthenticated, as a
// server does when an operator revoked the agent.
func (a *API) RejectCredential(reject bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.rejectCredential = reject
}

// FailRegister makes Register return err.
func (a *API) FailRegister(err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.registerErr = err
}

// FailReport makes ReportResult return err.
func (a *API) FailReport(err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.reportErr = err
}

func (a *API) SetCredential(c string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.credentialSet = c
}

// CredentialInUse reports the credential the agent most recently installed.
func (a *API) CredentialInUse() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.credentialSet
}

func (a *API) Register(_ context.Context, in *pulsev1.RegisterRequest) (*pulsev1.RegisterResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.registers = append(a.registers, in)
	if a.registerErr != nil {
		return nil, a.registerErr
	}
	return &pulsev1.RegisterResponse{
		AgentId:         a.agentID,
		Credential:      a.credential,
		EffectiveLabels: a.effectiveLabels,
	}, nil
}

// Registrations returns every Register call, in order.
func (a *API) Registrations() []*pulsev1.RegisterRequest {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]*pulsev1.RegisterRequest{}, a.registers...)
}

func (a *API) Connect(ctx context.Context) (pulseagent.Stream, error) {
	a.mu.Lock()
	reject := a.rejectCredential
	a.mu.Unlock()
	if reject {
		return nil, status.Error(codes.Unauthenticated, "credential revoked")
	}
	s := newFakeStream(ctx)
	a.mu.Lock()
	a.streams = append(a.streams, s)
	a.mu.Unlock()
	select {
	case a.onStream <- s:
	default:
	}
	return s, nil
}

// WaitStream blocks until the agent opens its next stream.
func (a *API) WaitStream(ctx context.Context) (*FakeStream, error) {
	select {
	case s := <-a.onStream:
		return s, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (a *API) ReportResult(_ context.Context, in *pulsev1.ReportResultRequest) (*pulsev1.ReportResultResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.reportErr != nil {
		return nil, a.reportErr
	}
	a.results = append(a.results, in)
	return &pulsev1.ReportResultResponse{Accepted: true}, nil
}

// Results returns every reported result, in arrival order.
func (a *API) Results() []*pulsev1.ReportResultRequest {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]*pulsev1.ReportResultRequest{}, a.results...)
}

func (a *API) Close() error { return nil }

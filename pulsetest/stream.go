package pulsetest

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	pulsev1 "github.com/seanly/pulse-devkit/pb/pulse/v1"
)

// FakeStream is a bidirectional stream a test drives from the server's side.
type FakeStream struct {
	ctx context.Context

	mu       sync.Mutex
	sent     []*pulsev1.AgentMessage
	sendErr  error
	closed   bool
	sendHook func(*pulsev1.AgentMessage)

	down chan *pulsev1.ServerMessage
	// broken carries the error that ends Recv, for simulating a dropped
	// connection.
	broken   chan error
	breakOne sync.Once
}

func newFakeStream(ctx context.Context) *FakeStream {
	return &FakeStream{
		ctx:    ctx,
		down:   make(chan *pulsev1.ServerMessage, 32),
		broken: make(chan error, 1),
	}
}

// Push queues a message for the agent to receive.
func (s *FakeStream) Push(msg *pulsev1.ServerMessage) { s.down <- msg }

// PushJob queues a job.
func (s *FakeStream) PushJob(j *pulsev1.Job) {
	s.Push(&pulsev1.ServerMessage{Payload: &pulsev1.ServerMessage_Job{Job: j}})
}

// PushConfig queues a config.
func (s *FakeStream) PushConfig(c *pulsev1.AgentConfig) {
	s.Push(&pulsev1.ServerMessage{Payload: &pulsev1.ServerMessage_Config{Config: c}})
}

// PushCommand queues a command.
func (s *FakeStream) PushCommand(k pulsev1.Command_Kind) {
	s.Push(&pulsev1.ServerMessage{Payload: &pulsev1.ServerMessage_Command{
		Command: &pulsev1.Command{Kind: k},
	}})
}

// Break ends Recv with err, simulating a dropped connection. Safe to call
// more than once; only the first takes effect.
func (s *FakeStream) Break(err error) {
	if err == nil {
		err = io.EOF
	}
	s.breakOne.Do(func() { s.broken <- err })
}

// FailSend makes every subsequent Send return err, simulating a send side that
// died before the receive side noticed.
func (s *FakeStream) FailSend(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sendErr = err
}

// OnSend installs a hook called for each message the agent sends, before it is
// recorded. Useful for reacting to an ack without polling.
func (s *FakeStream) OnSend(fn func(*pulsev1.AgentMessage)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sendHook = fn
}

func (s *FakeStream) Recv() (*pulsev1.ServerMessage, error) {
	select {
	case msg := <-s.down:
		return msg, nil
	case err := <-s.broken:
		return nil, err
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (s *FakeStream) Send(m *pulsev1.AgentMessage) error {
	s.mu.Lock()
	err := s.sendErr
	hook := s.sendHook
	if err == nil {
		s.sent = append(s.sent, m)
	}
	s.mu.Unlock()
	if err != nil {
		return err
	}
	if hook != nil {
		hook(m)
	}
	return nil
}

func (s *FakeStream) CloseSend() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// CloseSendCalled reports whether the agent closed its send side, which is the
// marker of a clean shutdown rather than a dropped connection.
func (s *FakeStream) CloseSendCalled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// Sent returns every message the agent sent, in order.
func (s *FakeStream) Sent() []*pulsev1.AgentMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*pulsev1.AgentMessage{}, s.sent...)
}

// Heartbeats returns just the heartbeats.
func (s *FakeStream) Heartbeats() []*pulsev1.Heartbeat {
	var out []*pulsev1.Heartbeat
	for _, m := range s.Sent() {
		if hb := m.GetHeartbeat(); hb != nil {
			out = append(out, hb)
		}
	}
	return out
}

// Acks returns just the job acks.
func (s *FakeStream) Acks() []*pulsev1.JobAck {
	var out []*pulsev1.JobAck
	for _, m := range s.Sent() {
		if ack := m.GetJobAck(); ack != nil {
			out = append(out, ack)
		}
	}
	return out
}

// WaitAcks blocks until at least n acks have arrived, or the deadline passes.
//
// Polling with a deadline rather than a channel because a test usually wants
// "n acks, whatever order they came in", and the agent acks from several code
// paths.
func (s *FakeStream) WaitAcks(n int, within time.Duration) ([]*pulsev1.JobAck, error) {
	deadline := time.Now().Add(within)
	for {
		acks := s.Acks()
		if len(acks) >= n {
			return acks, nil
		}
		if time.Now().After(deadline) {
			return acks, errors.New("pulsetest: timed out waiting for acks")
		}
		time.Sleep(time.Millisecond)
	}
}

// WaitHeartbeats blocks until at least n heartbeats have arrived.
func (s *FakeStream) WaitHeartbeats(n int, within time.Duration) ([]*pulsev1.Heartbeat, error) {
	deadline := time.Now().Add(within)
	for {
		hbs := s.Heartbeats()
		if len(hbs) >= n {
			return hbs, nil
		}
		if time.Now().After(deadline) {
			return hbs, errors.New("pulsetest: timed out waiting for heartbeats")
		}
		time.Sleep(time.Millisecond)
	}
}

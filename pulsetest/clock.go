package pulsetest

import (
	"sync"
	"time"

	"github.com/seanly/pulse-devkit/pulse"
)

// Clock is a manually advanced pulse.Clock.
//
// Lease expiry, heartbeat staleness and backoff all depend on time. Tests must
// not sleep through them — nobody should wait 45 seconds to check a staleness
// rule — so every framework component takes a Clock and tests advance it.
type Clock struct {
	mu  sync.Mutex
	now time.Time
}

// NewClock starts a clock at the given instant. A zero start uses a fixed
// arbitrary time, so test output is stable.
func NewClock(start time.Time) *Clock {
	if start.IsZero() {
		start = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	return &Clock{now: start}
}

func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Set moves the clock to an absolute instant.
func (c *Clock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

var _ pulse.Clock = (*Clock)(nil)

// LevelSetter records log levels pushed by a server.
type LevelSetter struct {
	mu     sync.Mutex
	levels []string
	err    error
}

// NewLevelSetter returns a recording LevelSetter.
func NewLevelSetter() *LevelSetter { return &LevelSetter{} }

// FailWith makes SetLevel return err, for testing that a bad level does not
// take the rest of a config push down with it.
func (l *LevelSetter) FailWith(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.err = err
}

func (l *LevelSetter) SetLevel(level string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return l.err
	}
	l.levels = append(l.levels, level)
	return nil
}

// Levels returns every level applied, in order.
func (l *LevelSetter) Levels() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string{}, l.levels...)
}

// Last returns the most recently applied level, empty if none.
func (l *LevelSetter) Last() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.levels) == 0 {
		return ""
	}
	return l.levels[len(l.levels)-1]
}

var _ pulse.LevelSetter = (*LevelSetter)(nil)

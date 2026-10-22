package pulse

import (
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// This file holds the seams that replace package-level globals. A library
// cannot own the process's logger, version string or metrics registry — its
// host does — so each is an interface with a usable no-op default.

// LevelSetter changes the host's log level. The framework calls it when the
// server pushes a log_level override.
//
// A nil LevelSetter is not an error: a deployment that does not want the
// server adjusting its log level is a legitimate choice. The framework logs
// that it ignored the push and carries on.
type LevelSetter interface {
	// SetLevel accepts one of debug|info|warn|error. An unknown level must
	// return an error rather than silently picking something.
	SetLevel(level string) error
}

// Metrics is the framework's counter and gauge sink. Implementations must be
// safe for concurrent use.
//
// Deliberately minimal: pulse-devkit does not choose a metrics library for its
// host. Adapting this to prometheus, otel or statsd is a dozen lines.
type Metrics interface {
	// Inc adds one to a named counter.
	Inc(name string)
	// Add adds delta to a named counter.
	Add(name string, delta float64)
	// Set writes a named gauge.
	Set(name string, value float64)
}

// Metric names the framework emits. Listed here so a host can register them
// up front and so the names cannot drift between emission sites.
const (
	MetricJobsReceived   = "pulse_agent_jobs_received_total"
	MetricJobsExecuted   = "pulse_agent_jobs_executed_total"
	MetricJobsRejected   = "pulse_agent_jobs_rejected_total"
	MetricJobsPanicked   = "pulse_agent_jobs_panicked_total"
	MetricReportsFailed  = "pulse_agent_reports_failed_total"
	MetricHeartbeats     = "pulse_agent_heartbeats_total"
	MetricReconnects     = "pulse_agent_reconnects_total"
	MetricConnected      = "pulse_agent_connected"
	MetricRunningJobs    = "pulse_agent_running_jobs"
	MetricJobsDispatched = "pulse_server_jobs_dispatched_total"
	MetricJobsRequeued   = "pulse_server_jobs_requeued_total"
	MetricLeasesExpired  = "pulse_server_leases_expired_total"
	MetricResultsStored  = "pulse_server_results_stored_total"
)

// NopMetrics discards everything. Used when Options.Metrics is nil.
type NopMetrics struct{}

func (NopMetrics) Inc(string)          {}
func (NopMetrics) Add(string, float64) {}
func (NopMetrics) Set(string, float64) {}

// MemMetrics is a concurrent in-memory Metrics, useful in tests and as a
// starting point for a host that has no metrics system yet.
type MemMetrics struct {
	mu       sync.Mutex
	counters map[string]float64
	gauges   map[string]float64
}

// NewMemMetrics returns an empty MemMetrics.
func NewMemMetrics() *MemMetrics {
	return &MemMetrics{counters: map[string]float64{}, gauges: map[string]float64{}}
}

func (m *MemMetrics) Inc(name string) { m.Add(name, 1) }

func (m *MemMetrics) Add(name string, delta float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters[name] += delta
}

func (m *MemMetrics) Set(name string, value float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gauges[name] = value
}

// Counter reports a counter's current value.
func (m *MemMetrics) Counter(name string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counters[name]
}

// Gauge reports a gauge's current value.
func (m *MemMetrics) Gauge(name string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.gauges[name]
}

// Snapshot copies every counter and gauge, for a test assertion or a debug
// endpoint.
func (m *MemMetrics) Snapshot() (counters, gauges map[string]float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	counters = make(map[string]float64, len(m.counters))
	for k, v := range m.counters {
		counters[k] = v
	}
	gauges = make(map[string]float64, len(m.gauges))
	for k, v := range m.gauges {
		gauges[k] = v
	}
	return counters, gauges
}

// Clock is the framework's view of time. Every component that waits, expires
// or ages something takes one, so tests never sleep: a 45-second staleness
// test is not something anyone should wait for.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
}

// ClockFunc adapts a function to Clock.
type ClockFunc func() time.Time

func (f ClockFunc) Now() time.Time { return f() }

// SystemClock is time.Now.
var SystemClock Clock = ClockFunc(time.Now)

// Backoff computes reconnect delays: exponential growth with jitter, so a
// fleet that lost its server at the same instant does not stampede it on
// return.
//
// Defaults match the protocol: start at 1s, double per failure, cap at 60s,
// jitter every delay by ±20%.
type Backoff struct {
	Base   time.Duration
	Cap    time.Duration
	Jitter float64

	// rand is lazily seeded; swap it in tests for a deterministic source.
	Rand *rand.Rand
}

// DefaultBackoff is the protocol's schedule.
func DefaultBackoff() Backoff {
	return Backoff{Base: time.Second, Cap: 60 * time.Second, Jitter: 0.2}
}

// Delay returns how long to wait before attempt n, counting from zero: attempt
// 0 waits Base, attempt 1 waits 2×Base, and so on.
//
// The cap applies before jitter so the spread stays around the cap rather than
// above it.
func (b Backoff) Delay(attempt int) time.Duration {
	base := b.Base
	if base <= 0 {
		base = time.Second
	}
	max := b.Cap
	if max <= 0 {
		max = 60 * time.Second
	}
	d := base
	for i := 0; i < attempt && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	j := b.Jitter
	if j <= 0 {
		return d
	}
	spread := float64(d) * j
	r := b.Rand
	if r == nil {
		r = globalRand()
	}
	return time.Duration(float64(d) - spread + r.Float64()*2*spread)
}

// globalRand lazily seeds one shared source. math/rand/v2's top-level
// functions are already concurrency-safe, but Backoff holds a *rand.Rand so a
// test can replace it, and a nil one has to come from somewhere.
var (
	sharedRand     atomic.Pointer[rand.Rand]
	sharedRandOnce sync.Once
)

func globalRand() *rand.Rand {
	sharedRandOnce.Do(func() {
		sharedRand.Store(rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0)))
	})
	return sharedRand.Load()
}

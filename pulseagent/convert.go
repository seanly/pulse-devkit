// Conversions between the wire types and pulse's domain types. Kept in one
// file so the mapping is auditable in a single read: a field that silently
// stops being carried is the kind of bug that only shows up in production.
package pulseagent

import (
	"time"

	pulsev1 "github.com/seanly/pulse-devkit/pb/pulse/v1"
	"github.com/seanly/pulse-devkit/pulse"
)

func jobFromProto(m *pulsev1.Job) pulse.Job {
	return pulse.Job{
		ID:        m.GetJobId(),
		GroupID:   m.GetGroupId(),
		SubjectID: m.GetSubjectId(),
		Kind:      m.GetKind(),
		Timeout:   time.Duration(m.GetTimeoutMs()) * time.Millisecond,
		Attempt:   int(m.GetAttempt()),
		Payload:   m.GetPayload(),
	}
}

func resultToProto(r pulse.Result) *pulsev1.ReportResultRequest {
	out := &pulsev1.ReportResultRequest{
		JobId:       r.JobID,
		GroupId:     r.GroupID,
		SubjectId:   r.SubjectID,
		Kind:        r.Kind,
		Status:      r.Status,
		ErrorCode:   r.ErrorCode,
		ErrorDetail: r.ErrorDetail,
		DurationMs:  r.Duration.Milliseconds(),
		Payload:     r.Payload,
	}
	if !r.ExecutedAt.IsZero() {
		out.ExecutedAtUnix = r.ExecutedAt.Unix()
	}
	return out
}

// ResultFromProto converts an inbound report. Used by the control plane, which
// is why it is exported from here: the mapping belongs next to its inverse, so
// the two cannot drift apart.
func ResultFromProto(m *pulsev1.ReportResultRequest) pulse.Result {
	r := pulse.Result{
		JobID:       m.GetJobId(),
		GroupID:     m.GetGroupId(),
		SubjectID:   m.GetSubjectId(),
		Kind:        m.GetKind(),
		Status:      m.GetStatus(),
		ErrorCode:   m.GetErrorCode(),
		ErrorDetail: m.GetErrorDetail(),
		Duration:    time.Duration(m.GetDurationMs()) * time.Millisecond,
		Payload:     m.GetPayload(),
	}
	if ts := m.GetExecutedAtUnix(); ts != 0 {
		r.ExecutedAt = time.Unix(ts, 0).UTC()
	}
	return r
}

// JobToProto renders a domain job for the wire. The control plane uses it when
// pushing dispatch messages.
func JobToProto(j pulse.Job) *pulsev1.Job {
	return &pulsev1.Job{
		JobId:     j.ID,
		GroupId:   j.GroupID,
		SubjectId: j.SubjectID,
		Kind:      j.Kind,
		TimeoutMs: int32(j.Timeout.Milliseconds()),
		Attempt:   int32(j.Attempt),
		Payload:   j.Payload,
	}
}

// ConfigToProto renders a stored override for the wire.
//
// A nil override field travels as its zero value, which the agent reads as "no
// opinion" and answers by falling back to its local value. That is how
// clearing an override takes effect without a reconnect.
func ConfigToProto(c pulse.AgentConfig) *pulsev1.AgentConfig {
	out := &pulsev1.AgentConfig{
		Version: int64(c.Version),
		Payload: c.Payload,
	}
	if c.Concurrency != nil {
		out.Concurrency = int32(*c.Concurrency)
	}
	if c.QPS != nil {
		out.Qps = int32(*c.QPS)
	}
	if c.Timeout != nil {
		out.TimeoutMs = int32(c.Timeout.Milliseconds())
	}
	if c.LogLevel != nil {
		out.LogLevel = *c.LogLevel
	}
	return out
}

// LocalConfigToProto reports an agent's own settings for display.
func LocalConfigToProto(c pulse.LocalConfig) *pulsev1.LocalConfig {
	return &pulsev1.LocalConfig{
		Concurrency: int32(c.Concurrency),
		Qps:         int32(c.QPS),
		TimeoutMs:   int32(c.Timeout.Milliseconds()),
		LogLevel:    c.LogLevel,
		Extra:       c.Extra,
	}
}

// LocalConfigFromProto is the inverse, for the control plane storing what an
// agent reported.
func LocalConfigFromProto(m *pulsev1.LocalConfig) pulse.LocalConfig {
	return pulse.LocalConfig{
		Concurrency: int(m.GetConcurrency()),
		QPS:         int(m.GetQps()),
		Timeout:     time.Duration(m.GetTimeoutMs()) * time.Millisecond,
		LogLevel:    m.GetLogLevel(),
		Extra:       m.GetExtra(),
	}
}

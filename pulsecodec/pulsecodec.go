// Package pulsecodec gives back the type safety that the opaque payload in
// the wire protocol trades away.
//
// The framework moves []byte because it must not know what the work is (see
// docs/design.md §2). Applications do know, and should not hand-roll
// marshalling in every executor. This package is the adapter between the two:
// generics live out here at the edge, and the core stays non-generic.
//
// Why not make Executor itself generic: type parameters would spread from
// Executor through the runner, the Agent, the API and Options — every type in
// the core would carry parameters that the framework never looks at.
package pulsecodec

import (
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/seanly/pulse-devkit/pulse"
	"github.com/seanly/pulse-devkit/pulseagent"
)

// Codec encodes and decodes one application's job and result payloads.
type Codec[J, R any] interface {
	DecodeJob([]byte) (J, error)
	EncodeJob(J) ([]byte, error)
	DecodeResult([]byte) (R, error)
	EncodeResult(R) ([]byte, error)
}

// Handler is an executor written in application types. The pulse.Job is passed
// alongside the decoded payload because the envelope still carries useful
// context — attempt number, subject ID, the deadline on the context.
type Handler[J, R any] func(ctx context.Context, job pulse.Job, payload J) (R, error)

// Typed wraps a typed handler as a pulseagent.Executor.
//
// A payload that does not decode becomes a status=error result with
// CodePayloadDecode and the handler is never called: a decode failure is a
// protocol mismatch, which the handler could do nothing about anyway.
func Typed[J, R any](c Codec[J, R], fn Handler[J, R]) pulseagent.Executor {
	return pulseagent.ExecutorFunc(func(ctx context.Context, job pulse.Job) (pulse.Result, error) {
		in, err := c.DecodeJob(job.Payload)
		if err != nil {
			return pulse.Result{
				Status:      pulse.StatusError,
				ErrorCode:   pulse.CodePayloadDecode,
				ErrorDetail: err.Error(),
			}, nil
		}
		out, err := fn(ctx, job, in)
		if err != nil {
			// Let the runner classify this: it knows whether the deadline
			// fired, which decides between a timeout and a plain error.
			return pulse.Result{}, err
		}
		raw, encErr := c.EncodeResult(out)
		if encErr != nil {
			// The work succeeded but its result cannot be sent. Reporting an
			// error is honest; silently dropping the payload is not.
			return pulse.Result{
				Status:      pulse.StatusError,
				ErrorCode:   pulse.CodePayloadDecode,
				ErrorDetail: "encode result: " + encErr.Error(),
			}, nil
		}
		return pulse.Result{Status: pulse.StatusOK, Payload: raw}, nil
	})
}

// TypedResult is like Typed but lets the handler return a full pulse.Result,
// for work that needs to report StatusSkipped or StatusWarning.
//
// The handler sets the status and any error code; this only fills in the
// encoded payload.
func TypedResult[J, R any](c Codec[J, R], fn func(ctx context.Context, job pulse.Job, payload J) (pulse.Result, R, error)) pulseagent.Executor {
	return pulseagent.ExecutorFunc(func(ctx context.Context, job pulse.Job) (pulse.Result, error) {
		in, err := c.DecodeJob(job.Payload)
		if err != nil {
			return pulse.Result{
				Status:      pulse.StatusError,
				ErrorCode:   pulse.CodePayloadDecode,
				ErrorDetail: err.Error(),
			}, nil
		}
		res, out, err := fn(ctx, job, in)
		if err != nil {
			return res, err
		}
		raw, encErr := c.EncodeResult(out)
		if encErr != nil {
			return pulse.Result{
				Status:      pulse.StatusError,
				ErrorCode:   pulse.CodePayloadDecode,
				ErrorDetail: "encode result: " + encErr.Error(),
			}, nil
		}
		res.Payload = raw
		if res.Status == "" {
			res.Status = pulse.StatusOK
		}
		return res, nil
	})
}

// JSONCodec marshals payloads as JSON. No extra dependency, and payloads stay
// readable in logs — the right default until payload size or rate says
// otherwise.
type JSONCodec[J, R any] struct{}

// JSON returns a JSON codec.
func JSON[J, R any]() Codec[J, R] { return JSONCodec[J, R]{} }

func (JSONCodec[J, R]) DecodeJob(raw []byte) (J, error) {
	var out J
	if len(raw) == 0 {
		// An empty payload decodes to the zero value rather than an error:
		// plenty of job kinds need no parameters at all.
		return out, nil
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, fmt.Errorf("pulsecodec: decode job payload: %w", err)
	}
	return out, nil
}

func (JSONCodec[J, R]) EncodeJob(in J) ([]byte, error) {
	raw, err := json.Marshal(in)
	if err != nil {
		return nil, fmt.Errorf("pulsecodec: encode job payload: %w", err)
	}
	return raw, nil
}

func (JSONCodec[J, R]) DecodeResult(raw []byte) (R, error) {
	var out R
	if len(raw) == 0 {
		return out, nil
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, fmt.Errorf("pulsecodec: decode result payload: %w", err)
	}
	return out, nil
}

func (JSONCodec[J, R]) EncodeResult(in R) ([]byte, error) {
	raw, err := json.Marshal(in)
	if err != nil {
		return nil, fmt.Errorf("pulsecodec: encode result payload: %w", err)
	}
	return raw, nil
}

// ProtoCodec marshals protobuf messages, for payloads large or frequent
// enough that JSON's size and parse cost matter. It uses the protobuf
// dependency the framework already has.
//
// J and R must be pointer types implementing proto.Message (*MyJob, not
// MyJob).
type ProtoCodec[J, R proto.Message] struct {
	// newJob and newResult allocate a message to unmarshal into. Needed
	// because a generic function cannot allocate the concrete type behind a
	// pointer-shaped type parameter.
	newJob    func() J
	newResult func() R
}

// Proto returns a protobuf codec. The two constructors allocate empty
// messages, usually as `func() *MyJob { return &MyJob{} }`.
func Proto[J, R proto.Message](newJob func() J, newResult func() R) Codec[J, R] {
	return ProtoCodec[J, R]{newJob: newJob, newResult: newResult}
}

func (c ProtoCodec[J, R]) DecodeJob(raw []byte) (J, error) {
	msg := c.newJob()
	if len(raw) == 0 {
		return msg, nil
	}
	if err := proto.Unmarshal(raw, msg); err != nil {
		return msg, fmt.Errorf("pulsecodec: decode job payload: %w", err)
	}
	return msg, nil
}

func (c ProtoCodec[J, R]) EncodeJob(in J) ([]byte, error) {
	raw, err := proto.Marshal(in)
	if err != nil {
		return nil, fmt.Errorf("pulsecodec: encode job payload: %w", err)
	}
	return raw, nil
}

func (c ProtoCodec[J, R]) DecodeResult(raw []byte) (R, error) {
	msg := c.newResult()
	if len(raw) == 0 {
		return msg, nil
	}
	if err := proto.Unmarshal(raw, msg); err != nil {
		return msg, fmt.Errorf("pulsecodec: decode result payload: %w", err)
	}
	return msg, nil
}

func (c ProtoCodec[J, R]) EncodeResult(in R) ([]byte, error) {
	raw, err := proto.Marshal(in)
	if err != nil {
		return nil, fmt.Errorf("pulsecodec: encode result payload: %w", err)
	}
	return raw, nil
}

// EncodeJobPayload is the server-side half: it renders a typed job payload for
// enqueueing. Symmetric with Typed, so both ends of a kind use one codec.
func EncodeJobPayload[J, R any](c Codec[J, R], in J) ([]byte, error) {
	return c.EncodeJob(in)
}

// DecodeResultPayload is the server-side half for reading what came back.
func DecodeResultPayload[J, R any](c Codec[J, R], raw []byte) (R, error) {
	return c.DecodeResult(raw)
}

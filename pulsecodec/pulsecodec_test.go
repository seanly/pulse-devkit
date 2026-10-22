package pulsecodec_test

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/seanly/pulse-devkit/pulse"
	"github.com/seanly/pulse-devkit/pulsecodec"
)

type probeJob struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

type probeResult struct {
	DaysRemaining int  `json:"days_remaining"`
	Trusted       bool `json:"trusted"`
}

func TestTypedRoundTrip(t *testing.T) {
	codec := pulsecodec.JSON[probeJob, probeResult]()
	exec := pulsecodec.Typed(codec, func(_ context.Context, job pulse.Job, p probeJob) (probeResult, error) {
		if p.Host != "example.com" || p.Port != 443 {
			t.Errorf("decoded payload = %+v, want example.com:443", p)
		}
		// The envelope is still available alongside the decoded payload.
		if job.Attempt != 2 {
			t.Errorf("job.Attempt = %d, want 2", job.Attempt)
		}
		return probeResult{DaysRemaining: 30, Trusted: true}, nil
	})

	raw, err := codec.EncodeJob(probeJob{Host: "example.com", Port: 443})
	if err != nil {
		t.Fatal(err)
	}
	res, err := exec.Execute(context.Background(), pulse.Job{
		ID: "j1", Attempt: 2, Payload: raw,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Status != pulse.StatusOK {
		t.Errorf("status = %q, want ok", res.Status)
	}
	out, err := codec.DecodeResult(res.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if out.DaysRemaining != 30 || !out.Trusted {
		t.Errorf("result payload = %+v, want 30 days and trusted", out)
	}
}

func TestUndecodablePayloadNeverReachesTheHandler(t *testing.T) {
	called := false
	exec := pulsecodec.Typed(pulsecodec.JSON[probeJob, probeResult](),
		func(context.Context, pulse.Job, probeJob) (probeResult, error) {
			called = true
			return probeResult{}, nil
		})

	res, err := exec.Execute(context.Background(), pulse.Job{
		ID: "j1", Payload: []byte("{not json"),
	})
	if err != nil {
		t.Errorf("Execute returned an error; the failure should be in the result: %v", err)
	}
	// A decode failure is a protocol mismatch. The handler could do nothing
	// about it, so it is not asked to.
	if called {
		t.Error("the handler ran on an undecodable payload")
	}
	if res.ErrorCode != pulse.CodePayloadDecode {
		t.Errorf("error code = %q, want %q", res.ErrorCode, pulse.CodePayloadDecode)
	}
	if res.Status != pulse.StatusError {
		t.Errorf("status = %q, want error", res.Status)
	}
}

func TestEmptyPayloadDecodesToZeroValue(t *testing.T) {
	// Plenty of job kinds take no parameters; that must not be an error.
	exec := pulsecodec.Typed(pulsecodec.JSON[probeJob, probeResult](),
		func(_ context.Context, _ pulse.Job, p probeJob) (probeResult, error) {
			if p.Host != "" || p.Port != 0 {
				t.Errorf("want the zero value, got %+v", p)
			}
			return probeResult{}, nil
		})
	res, err := exec.Execute(context.Background(), pulse.Job{ID: "j1"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != pulse.StatusOK {
		t.Errorf("status = %q, want ok", res.Status)
	}
}

func TestHandlerErrorIsLeftForTheRunnerToClassify(t *testing.T) {
	want := errors.New("the probe failed")
	exec := pulsecodec.Typed(pulsecodec.JSON[probeJob, probeResult](),
		func(context.Context, pulse.Job, probeJob) (probeResult, error) {
			return probeResult{}, want
		})
	// Returned as an error, not pre-classified: only the runner knows whether
	// the deadline fired, which decides timeout versus plain failure.
	_, err := exec.Execute(context.Background(), pulse.Job{ID: "j1"})
	if !errors.Is(err, want) {
		t.Errorf("error = %v, want the handler's error", err)
	}
}

func TestTypedResultAllowsSkipped(t *testing.T) {
	codec := pulsecodec.JSON[probeJob, probeResult]()
	exec := pulsecodec.TypedResult(codec,
		func(context.Context, pulse.Job, probeJob) (pulse.Result, probeResult, error) {
			return pulse.Result{Status: pulse.StatusSkipped, ErrorCode: "out_of_scope"},
				probeResult{}, nil
		})
	res, err := exec.Execute(context.Background(), pulse.Job{ID: "j1"})
	if err != nil {
		t.Fatal(err)
	}
	// An application that wants full control of the status gets it.
	if res.Status != pulse.StatusSkipped {
		t.Errorf("status = %q, want skipped", res.Status)
	}
	if res.ErrorCode != "out_of_scope" {
		t.Errorf("error code = %q, want the application's own", res.ErrorCode)
	}
	if res.Payload == nil {
		t.Error("want the encoded payload filled in")
	}
}

// unencodable fails to marshal, to exercise the encode-failure path.
type unencodable struct {
	Ch chan int `json:"ch"`
}

func TestUnencodableResultIsReportedNotDropped(t *testing.T) {
	exec := pulsecodec.Typed(pulsecodec.JSON[probeJob, unencodable](),
		func(context.Context, pulse.Job, probeJob) (unencodable, error) {
			return unencodable{Ch: make(chan int)}, nil
		})
	res, err := exec.Execute(context.Background(), pulse.Job{ID: "j1"})
	if err != nil {
		t.Fatal(err)
	}
	// The work succeeded but its result cannot be sent. Saying so is honest;
	// silently sending an empty payload would not be.
	if res.Status != pulse.StatusError {
		t.Errorf("status = %q, want error", res.Status)
	}
	if res.ErrorCode != pulse.CodePayloadDecode {
		t.Errorf("error code = %q, want %q", res.ErrorCode, pulse.CodePayloadDecode)
	}
}

func TestProtoCodecRoundTrip(t *testing.T) {
	// A well-known type as a stand-in payload: the point is the codec, not the
	// message shape.
	codec := pulsecodec.Proto(
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func() *wrapperspb.Int32Value { return &wrapperspb.Int32Value{} },
	)
	raw, err := codec.EncodeJob(wrapperspb.String("hello"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := codec.DecodeJob(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.GetValue() != "hello" {
		t.Errorf("job round trip gave %q, want hello", got.GetValue())
	}

	rawRes, err := codec.EncodeResult(wrapperspb.Int32(42))
	if err != nil {
		t.Fatal(err)
	}
	gotRes, err := codec.DecodeResult(rawRes)
	if err != nil {
		t.Fatal(err)
	}
	if gotRes.GetValue() != 42 {
		t.Errorf("result round trip gave %d, want 42", gotRes.GetValue())
	}
}

func TestProtoCodecRejectsGarbage(t *testing.T) {
	codec := pulsecodec.Proto(
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func() *wrapperspb.Int32Value { return &wrapperspb.Int32Value{} },
	)
	// Field 1 declared as a varint but carrying a truncated length prefix.
	if _, err := codec.DecodeJob([]byte{0x0a, 0xff}); err == nil {
		t.Error("want an error for an undecodable protobuf payload")
	}
}

package pulse

import (
	"context"
	"io"
)

// Streams is where a running job may write stdout and stderr while it runs.
//
// An executor that does not stream ignores them. One that does must treat a
// short or failed write as "the server did not get this chunk", not as a
// failure of the job: the process's own output is not the agent's to reject.
type Streams struct {
	Stdout io.Writer
	Stderr io.Writer
}

type streamsKey struct{}

// ContextWithStreams attaches output streams to a job's context.
func ContextWithStreams(ctx context.Context, s Streams) context.Context {
	return context.WithValue(ctx, streamsKey{}, s)
}

// StreamsFrom returns the streams a runner attached, if any.
func StreamsFrom(ctx context.Context) (Streams, bool) {
	s, ok := ctx.Value(streamsKey{}).(Streams)
	return s, ok
}

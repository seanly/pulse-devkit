// The server-facing API the agent talks to, as a narrow interface so the whole
// agent loop can be tested against fakes that speak the same vocabulary as the
// generated client.
package pulseagent

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	pulsev1 "github.com/seanly/pulse-devkit/pb/pulse/v1"
)

// API is the AgentService surface the agent uses. Method shapes mirror the
// generated client one-to-one so the gRPC implementation is a thin adapter.
type API interface {
	// SetCredential installs the bearer credential used by Connect and
	// ReportResult. Register does not use it — the bootstrap token is that
	// call's authentication.
	SetCredential(credential string)
	Register(ctx context.Context, in *pulsev1.RegisterRequest) (*pulsev1.RegisterResponse, error)
	Connect(ctx context.Context) (Stream, error)
	ReportResult(ctx context.Context, in *pulsev1.ReportResultRequest) (*pulsev1.ReportResultResponse, error)
	// Close tears down the underlying transport.
	Close() error
}

// Stream is one live connection: server messages in, agent messages out.
type Stream interface {
	Recv() (*pulsev1.ServerMessage, error)
	Send(*pulsev1.AgentMessage) error
	CloseSend() error
}

// grpcAPI adapts the generated client to API. The credential travels in the
// authorization metadata on every call.
type grpcAPI struct {
	conn       *grpc.ClientConn
	client     pulsev1.AgentServiceClient
	credential string
}

// DialOptions configures NewGRPCAPI.
type DialOptions struct {
	// Addr is the server's gRPC address.
	Addr string
	// TransportCredentials secures the connection. Nil means plaintext, which
	// is appropriate only on a trusted network: the credential is signed, so
	// an eavesdropper cannot forge one, but everything on the stream is
	// readable. Production deployments should set this.
	TransportCredentials grpc.DialOption
	// Extra options are appended, for keepalives, interceptors and the like.
	Extra []grpc.DialOption
}

// NewGRPCAPI builds a client. The dial is lazy in grpc-go, so a server that is
// down at agent start is a reconnect rather than a startup failure.
func NewGRPCAPI(o DialOptions) (API, error) {
	opts := []grpc.DialOption{}
	if o.TransportCredentials != nil {
		opts = append(opts, o.TransportCredentials)
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	opts = append(opts, o.Extra...)
	conn, err := grpc.NewClient(o.Addr, opts...)
	if err != nil {
		return nil, err
	}
	return &grpcAPI{conn: conn, client: pulsev1.NewAgentServiceClient(conn)}, nil
}

func (g *grpcAPI) SetCredential(credential string) { g.credential = credential }

func (g *grpcAPI) auth(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, credentialHeader, "Bearer "+g.credential)
}

// credentialHeader is the metadata key credentials travel in: a bearer token
// in the authorization header, so proxies and TLS terminators see a
// conventional shape.
const credentialHeader = "authorization"

// Register needs no credential — the bootstrap token is the authentication.
func (g *grpcAPI) Register(ctx context.Context, in *pulsev1.RegisterRequest) (*pulsev1.RegisterResponse, error) {
	return g.client.Register(ctx, in)
}

func (g *grpcAPI) Connect(ctx context.Context) (Stream, error) {
	cs, err := g.client.Connect(g.auth(ctx))
	if err != nil {
		return nil, err
	}
	return &grpcStream{cs: cs}, nil
}

func (g *grpcAPI) ReportResult(ctx context.Context, in *pulsev1.ReportResultRequest) (*pulsev1.ReportResultResponse, error) {
	return g.client.ReportResult(g.auth(ctx), in)
}

// Close tears down the underlying connection.
func (g *grpcAPI) Close() error { return g.conn.Close() }

// grpcStream wraps the generated stream. Send is NOT safe for concurrent use —
// grpc-go allows one sending goroutine — which is why the agent funnels every
// send through a single sender goroutine; see session in agent.go.
type grpcStream struct {
	cs pulsev1.AgentService_ConnectClient
}

func (s *grpcStream) Recv() (*pulsev1.ServerMessage, error) { return s.cs.Recv() }

func (s *grpcStream) Send(m *pulsev1.AgentMessage) error { return s.cs.Send(m) }

func (s *grpcStream) CloseSend() error { return s.cs.CloseSend() }

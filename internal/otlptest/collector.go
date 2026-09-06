// Package otlptest is an in-process OTLP/gRPC trace collector for tests.
package otlptest

import (
	"context"
	"net"
	"sync"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
)

// Collector records span names exported over OTLP/gRPC.
type Collector struct {
	coltracepb.UnimplementedTraceServiceServer

	mu     sync.Mutex
	names  []string
	lis    net.Listener
	server *grpc.Server
}

// Start listens on 127.0.0.1 (ephemeral port) and serves TraceService/Export.
func Start() (*Collector, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	c := &Collector{
		lis:    lis,
		server: grpc.NewServer(),
	}
	coltracepb.RegisterTraceServiceServer(c.server, c)
	go func() {
		_ = c.server.Serve(lis)
	}()
	return c, nil
}

// Endpoint is host:port, with no scheme.
func (c *Collector) Endpoint() string {
	return c.lis.Addr().String()
}

// HTTPEndpoint is the value for OTEL_EXPORTER_OTLP_ENDPOINT (http implies insecure).
func (c *Collector) HTTPEndpoint() string {
	return "http://" + c.Endpoint()
}

// Export implements the OTLP trace collector.
func (c *Collector) Export(_ context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, rs := range req.GetResourceSpans() {
		for _, ss := range rs.GetScopeSpans() {
			for _, sp := range ss.GetSpans() {
				c.names = append(c.names, sp.GetName())
			}
		}
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

// SpanNames returns a copy of received span names, in arrival order.
func (c *Collector) SpanNames() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.names))
	copy(out, c.names)
	return out
}

// HasSpan reports whether a span with this name was received.
func (c *Collector) HasSpan(name string) bool {
	for _, n := range c.SpanNames() {
		if n == name {
			return true
		}
	}
	return false
}

// Stop shuts down the gRPC server.
func (c *Collector) Stop() {
	if c.server != nil {
		c.server.GracefulStop()
	}
}

//go:build unit_test

package main

import (
	"context"
	"testing"
	"time"

	"github.com/taekjaskyli/kafka-canary/internal/otlptest"
)

func TestInitTracerProviderOff(t *testing.T) {
	if tp := initTracerProvider(false); tp != nil {
		t.Fatal("expected nil tracer provider when tracing is off")
	}
}

func TestInitTracerProviderOTLP(t *testing.T) {
	col, err := otlptest.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(col.Stop)

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", col.HTTPEndpoint())
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "true")

	tp := initTracerProvider(true)
	if tp == nil {
		t.Fatal("expected tracer provider for otlp")
	}
	ctx := context.Background()
	t.Cleanup(func() { _ = tp.Shutdown(ctx) })

	_, span := tp.Tracer("kafka-canary-test").Start(ctx, "otlp-unit-span")
	span.End()
	if err := tp.ForceFlush(ctx); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if col.HasSpan("otlp-unit-span") {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("OTLP collector did not receive span; got %v", col.SpanNames())
}

package tracing

import (
	"context"
	"github.com/3lvia/kunde-extensions-lib-go/pkg/sfkafkalib/consumerLogger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func ConfigureTracing(ctx context.Context, r *resource.Resource, env string) (func(), error) {
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		return nil, err
	}

	var prc sdktrace.TracerProviderOption
	switch env {
	case consumerLogger.Local:
		prc = sdktrace.WithSyncer(exporter)
	default:
		prc = sdktrace.WithBatcher(exporter)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(r),
		prc,
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return func() {
		exporter.Shutdown(ctx)
	}, nil
}

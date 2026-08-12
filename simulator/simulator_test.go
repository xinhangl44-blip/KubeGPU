package simulator

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func initTracer(ctx context.Context) (*sdktrace.TracerProvider, error) {
	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithInsecure(), otlptracehttp.WithEndpoint("localhost:4318"))
	if err != nil {
		return nil, err
	}
	res, err := resource.New(ctx, resource.WithAttributes(attribute.String("service.name", "simulator-test-service")))
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithBatcher(exporter), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	return tp, nil
}

func TestSimulatorSchedule(t *testing.T) {
	ctx := context.Background()
	tp, err := initTracer(ctx)
	if err != nil {
		t.Fatalf("failed to initialize tracer: %v", err)
	}
	defer func() {
		if err := tp.Shutdown(ctx); err != nil {
			t.Errorf("failed to shutdown tracer provider: %v", err)
		}
	}()
	mockNodes := []nodeState{
		{
			Name: "node-gpu-pool-1",
			GPUs: []gpuState{
				{ID: 0, UUID: "gpu-uuid-aaaa", TotalVRAM: 32 * 1024 * 1024 * 1024, UsedVRAM: 4 * 1024 * 1024 * 1024},
				{ID: 1, UUID: "gpu-uuid-bbbb", TotalVRAM: 32 * 1024 * 1024 * 1024, UsedVRAM: 16 * 1024 * 1024 * 1024},
			},
		},
		{
			Name: "node-gpu-pool-2",
			GPUs: []gpuState{
				{ID: 0, UUID: "gpu-uuid-cccc", TotalVRAM: 16 * 1024 * 1024 * 1024, UsedVRAM: 0},
			},
		},
	}
	mockJobs := []jobState{
		{Name: "llama-70b-train", GangSize: 2, VRAMPerGPU: 12 * 1024 * 1024 * 1024, Priority: 100, SubmittedAt: time.Now()},
		{Name: "sdxl-inference", GangSize: 1, VRAMPerGPU: 20 * 1024 * 1024 * 1024, Priority: 50, SubmittedAt: time.Now().Add(-5 * time.Minute)},
		{Name: "deadlock-job-1", GangSize: 1, VRAMPerGPU: 30 * 1024 * 1024 * 1024, Priority: 10, SubmittedAt: time.Now()},
		{Name: "deadlock-job-2", GangSize: 1, VRAMPerGPU: 30 * 1024 * 1024 * 1024, Priority: 10, SubmittedAt: time.Now()},
	}
	sim := &Simulator{
		nodes:       mockNodes,
		pendingVRAM: make(map[string]map[string]int64),
	}
	results := sim.schedule(ctx, mockJobs)
	if len(results) != 4 {
		t.Errorf("expected 4 job results, got %d", len(results))
	}
	if results[0].Outcome != OutcomeScheduled {
		t.Errorf("expected first job scheduled, got %s", results[0].Outcome)
	}
	time.Sleep(2 * time.Second)
}

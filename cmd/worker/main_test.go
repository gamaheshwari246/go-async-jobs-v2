package main

import (
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestWorkerConfig(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	config := workerConfig(7, logger, func(error) {})
	if config.Concurrency != 7 {
		t.Fatalf("unexpected concurrency %d", config.Concurrency)
	}
	if config.DelayedTaskCheckInterval != time.Second {
		t.Fatalf("retry polling must be one second, got %v", config.DelayedTaskCheckInterval)
	}
	if config.ShutdownTimeout != 330*time.Second {
		t.Fatalf("shutdown timeout mismatch %v", config.ShutdownTimeout)
	}
	if config.HealthCheckInterval != 5*time.Second || config.HealthCheckFunc == nil {
		t.Fatalf("worker Redis health monitor not configured")
	}
}
func TestWorkerHealthAddress(t *testing.T) {
	t.Setenv("WORKER_HEALTH_ADDR", "")
	if got := workerHealthAddress(); got != ":8081" {
		t.Fatal(got)
	}
	t.Setenv("WORKER_HEALTH_ADDR", ":19123")
	if got := workerHealthAddress(); got != ":19123" {
		t.Fatal(got)
	}
}

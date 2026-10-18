package metrics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hibiken/asynq"
)

func TestDurationBucketsAscending(t *testing.T) {
	if len(bounds) < 3 {
		t.Fatal("histogram buckets missing")
	}
	for i := 1; i < len(bounds); i++ {
		if bounds[i] <= bounds[i-1] {
			t.Fatal("bucket bounds must be strictly increasing")
		}
	}
}
func TestMetricsMiddlewarePreservesHandlerFailure(t *testing.T) {
	c := &Collector{}
	want := errors.New("original failure")
	h := c.Middleware()(asynq.HandlerFunc(func(ctx context.Context, task *asynq.Task) error { return want }))
	err := h.ProcessTask(context.Background(), asynq.NewTask("demo.sleep", nil))
	if !errors.Is(err, want) {
		t.Fatalf("middleware changed error %v", err)
	}
}
func TestParseIntMissingAndInvalid(t *testing.T) {
	if parseInt(map[string]string{}, "count") != 0 || parseInt(map[string]string{"count": "oops"}, "count") != 0 || parseInt(map[string]string{"count": "42"}, "count") != 42 {
		t.Fatal("parseInt did not handle counters")
	}
	_ = time.Second
}

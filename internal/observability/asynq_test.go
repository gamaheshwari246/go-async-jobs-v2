package observability

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/hibiken/asynq"
)

func TestTaskMiddlewareLogsSuccess(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	handler := TaskMiddleware(logger)(asynq.HandlerFunc(func(ctx context.Context, task *asynq.Task) error {
		return nil
	}))
	if err := handler.ProcessTask(context.Background(), asynq.NewTask("demo.sleep", nil)); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	for _, event := range []string{"job_started", "job_succeeded"} {
		if !strings.Contains(text, event) {
			t.Fatalf("missing event %q in log: %s", event, text)
		}
	}
}

func TestTaskMiddlewarePreservesFailure(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	expected := errors.New("transient failure")
	handler := TaskMiddleware(logger)(asynq.HandlerFunc(func(ctx context.Context, task *asynq.Task) error {
		return expected
	}))
	err := handler.ProcessTask(context.Background(), asynq.NewTask("demo.sleep", nil))
	if !errors.Is(err, expected) {
		t.Fatalf("handler error must be returned unchanged, got %v", err)
	}
	if !strings.Contains(output.String(), "job_failed") {
		t.Fatalf("missing failure event: %s", output.String())
	}
}

package httpapi

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hibiken/asynq"
)

func TestRequestLoggerCapturesStatusAndAvoidsBody(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	handler := requestLogger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}), logger)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader("secret-payload"))
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected 202 got %d", recorder.Code)
	}
	logged := output.String()
	if !strings.Contains(logged, "http_request_finished") || !strings.Contains(logged, "\"status\":202") {
		t.Fatalf("missing request log fields: %s", logged)
	}
	if strings.Contains(logged, "secret-payload") {
		t.Fatal("HTTP request body must not appear in structured logs")
	}
}

func TestStateNamesAndAttemptCounts(t *testing.T) {
	cases := []struct {
		state        asynq.TaskState
		retried      int
		wantName     string
		wantAttempts int
	}{
		{asynq.TaskStatePending, 0, "queued", 0},
		{asynq.TaskStateActive, 0, "running", 1},
		{asynq.TaskStateRetry, 1, "failed", 1},
		{asynq.TaskStateCompleted, 2, "succeeded", 3},
		{asynq.TaskStateArchived, 1, "dead_lettered", 2},
		{asynq.TaskStateScheduled, 0, "scheduled", 0},
	}
	for _, tc := range cases {
		info := &asynq.TaskInfo{State: tc.state, Retried: tc.retried}
		if got := stateName(info); got != tc.wantName {
			t.Errorf("state %v name: got %q want %q", tc.state, got, tc.wantName)
		}
		if got := attempts(info); got != tc.wantAttempts {
			t.Errorf("state %v attempts: got %d want %d", tc.state, got, tc.wantAttempts)
		}
	}
}

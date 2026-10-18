package health

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWorkerReadiness(t *testing.T) {
	monitor := &WorkerReadiness{}
	handler := monitor.Routes()
	check := func(path string, want int) {
		t.Helper()
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != want {
			t.Fatalf("%s returned %d; want %d", path, response.Code, want)
		}
	}
	check("/healthz", http.StatusOK)
	check("/readyz", http.StatusServiceUnavailable)
	monitor.Observe(nil)
	check("/readyz", http.StatusOK)
	monitor.Observe(errors.New("redis disconnected"))
	check("/readyz", http.StatusServiceUnavailable)
	monitor.Observe(nil)
	monitor.lastSuccess.Store(time.Now().Add(-maxHealthAge - time.Second).UnixNano())
	check("/readyz", http.StatusServiceUnavailable)
}

package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"example.com/go-async-jobs/internal/operations"
	"github.com/redis/go-redis/v9"
)

func TestReadinessFailsWhenRedisUnavailable(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: 0})
	defer client.Close()
	handler := (&Server{Gate: operations.Gate{Redis: client}}).Routes()
	check := func(path string, want int) {
		t.Helper()
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != want {
			t.Fatalf("%s: got %d, want %d", path, response.Code, want)
		}
	}
	check("/healthz", http.StatusOK)
	check("/readyz", http.StatusServiceUnavailable)
}

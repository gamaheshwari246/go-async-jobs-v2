// Package health provides a lightweight worker liveness/readiness monitor.
// Readiness reflects recent Asynq Redis connectivity, not job throughput.
package health

import (
	"net/http"
	"sync/atomic"
	"time"
)

const maxHealthAge = 20 * time.Second

type WorkerReadiness struct {
	lastSuccess  atomic.Int64
	redisHealthy atomic.Bool
}

// Observe receives Asynq's periodic Redis ping result.
func (h *WorkerReadiness) Observe(err error) {
	if err != nil {
		h.redisHealthy.Store(false)
		return
	}
	h.lastSuccess.Store(time.Now().UnixNano())
	h.redisHealthy.Store(true)
}

func (h *WorkerReadiness) Ready() bool {
	last := h.lastSuccess.Load()
	return last != 0 && h.redisHealthy.Load() &&
		time.Since(time.Unix(0, last)) <= maxHealthAge
}

func (h *WorkerReadiness) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("alive\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if !h.Ready() {
			http.Error(w, "worker not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	return mux
}

package httpapi

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"example.com/go-async-jobs/internal/metrics"
	"example.com/go-async-jobs/internal/operations"
	"example.com/go-async-jobs/internal/operations/idempotency"
	"example.com/go-async-jobs/internal/registry"
	"example.com/go-async-jobs/internal/spec"

	"github.com/hibiken/asynq"
)

var queues = []string{"critical", "default", "low"}

// Server exposes job submission, state inspection and operational controls.
type Server struct {
	Client     *asynq.Client
	Inspector  *asynq.Inspector
	Logger     *slog.Logger
	Gate       operations.Gate
	AdminToken string
	Metrics    *metrics.Collector
}

func (s *Server) logger() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.Default()
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "up"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := s.Gate.Redis.Ping(ctx).Err(); err != nil {
			s.logger().WarnContext(r.Context(), "api_readiness_failed", "error", err)
			http.Error(w, "Redis unavailable", http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("POST /jobs", s.submit)
	mux.HandleFunc("GET /jobs/{id}", s.status)
	mux.HandleFunc("GET /queue/depth", s.depth)
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		if !s.adminAuthorized(w, r) {
			return
		}
		if s.Metrics == nil {
			http.Error(w, "metrics unavailable", http.StatusServiceUnavailable)
			return
		}
		s.Metrics.ServeHTTP(w, r)
	})
	mux.HandleFunc("POST /admin/drain", s.adminDrain)
	mux.HandleFunc("POST /admin/resume", s.adminResume)
	mux.HandleFunc("GET /admin/drain", s.adminDrainStatus)
	return requestLogger(mux, s.logger())
}

func requestLogger(next http.Handler, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		recorder := &responseRecorder{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(recorder, r)
		logger.InfoContext(r.Context(), "http_request_finished",
			"method", r.Method,
			"path", r.URL.Path,
			"status", recorder.code,
			"elapsed_ms", time.Since(started).Milliseconds(),
		)
	})
}

type responseRecorder struct {
	http.ResponseWriter
	code int
}

func (r *responseRecorder) WriteHeader(status int) {
	r.code = status
	r.ResponseWriter.WriteHeader(status)
}

func (s *Server) submit(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 64*1024)

	draining, err := s.Gate.IsDraining(r.Context())
	if err != nil {
		s.logger().ErrorContext(r.Context(), "admission_check_failed", "error", err)
		http.Error(w, "queue temporarily unavailable", http.StatusServiceUnavailable)
		return
	}
	if draining {
		w.Header().Set("Retry-After", "5")
		http.Error(w, "submissions disabled while draining", http.StatusServiceUnavailable)
		return
	}

	req, err := spec.Decode(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	job, err := spec.Validate(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// The API needs only the validators. Definitions(nil) creates handler closures
	// but does not execute them; Redis is injected into handlers by the worker.
	if err := registry.Validate(registry.Definitions(nil), req.Type, req.Payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	task := asynq.NewTask(job.Type, job.Body)
	opts := []asynq.Option{
		asynq.Queue(job.Queue),
		asynq.MaxRetry(job.Retries),
		asynq.Timeout(job.Timeout),
		asynq.Retention(24 * time.Hour),
	}
	if job.RunAt != nil {
		opts = append(opts, asynq.ProcessAt(*job.RunAt))
	}

	// Client-supplied idempotency protects HTTP re-submissions, not worker retries.
	// The short Redis lease is recoverable after an API crash before enqueue.
	key := r.Header.Get("Idempotency-Key")
	var claim idempotency.Claim
	var deterministicID string
	if key != "" {
		if len(key) > 128 {
			http.Error(w, "idempotency key too long", http.StatusBadRequest)
			return
		}
		for _, ch := range key {
			if ch < 33 || ch > 126 {
				http.Error(w, "idempotency key must contain printable non-whitespace ASCII", http.StatusBadRequest)
				return
			}
		}
		deterministicID = idempotency.TaskID(key)
		opts = append(opts, asynq.TaskID(deterministicID))

		claim, err = idempotency.Reserve(r.Context(), s.Gate.Redis, key, job.Body)
		if err != nil {
			s.logger().ErrorContext(r.Context(), "idempotency_reservation_failed", "error", err)
			http.Error(w, "idempotency registry unavailable", http.StatusServiceUnavailable)
			return
		}
		switch claim.Decision {
		case idempotency.Conflict:
			http.Error(w, "idempotency key reused for different job configuration", http.StatusConflict)
			return
		case idempotency.Existing, idempotency.InProgress:
			existing, lookupErr := s.lookupExistingTask(deterministicID, job)
			if lookupErr != nil {
				s.logger().ErrorContext(r.Context(), "idempotency_lookup_failed", "error", lookupErr)
				http.Error(w, "queue temporarily unavailable", http.StatusServiceUnavailable)
				return
			}
			if existing != nil {
				s.confirmObserved(r, key, job.Body)
				s.writeDuplicate(w, r, existing, job)
				return
			}
			if claim.Decision == idempotency.Existing {
				// The task was accepted but is no longer retained in Asynq.
				// Never silently re-enqueue a possibly completed business operation.
				http.Error(w, "original job has expired from the result store", http.StatusGone)
				return
			}
			w.Header().Set("Retry-After", "1")
			http.Error(w, "submission in progress; retry with the same idempotency key", http.StatusServiceUnavailable)
			return
		}
	}

	info, err := s.Client.EnqueueContext(r.Context(), task, opts...)
	if key != "" && errors.Is(err, asynq.ErrTaskIDConflict) {
		// The previous API process may have enqueued successfully, then died
		// before writing the Redis confirmation record.
		existing, lookupErr := s.lookupExistingTask(deterministicID, job)
		if lookupErr != nil {
			s.logger().ErrorContext(r.Context(), "idempotency_lookup_failed", "error", lookupErr)
			http.Error(w, "queue temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		if existing == nil {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "task id is reserved; retry shortly", http.StatusServiceUnavailable)
			return
		}
		s.confirmObserved(r, key, job.Body)
		s.writeDuplicate(w, r, existing, job)
		return
	}
	if err != nil {
		s.logger().ErrorContext(r.Context(), "job_enqueue_failed",
			"queue", job.Queue, "task_type", job.Type, "error", err)
		http.Error(w, "queue temporarily unavailable", http.StatusServiceUnavailable)
		return
	}
	if key != "" {
		confirmed, confirmErr := idempotency.Confirm(r.Context(), s.Gate.Redis, key, job.Body, claim)
		if confirmErr != nil || !confirmed {
			// The job was accepted by Asynq. Retry inspection repairs the
			// reservation, so confirmation failure must not cause re-enqueue.
			s.logger().WarnContext(r.Context(), "idempotency_confirmation_deferred",
				"job_id", info.ID, "error", confirmErr, "lease_owner_confirmed", confirmed)
		}
	}

	s.logger().InfoContext(r.Context(), "job_enqueued",
		"job_id", info.ID, "queue", info.Queue, "task_type", job.Type,
		"priority", job.Priority, "max_retries", job.Retries,
		"timeout", job.Timeout.String())
	w.Header().Set("Location", "/jobs/"+info.ID)
	status := "queued"
	response := map[string]any{"job_id": info.ID, "status": status, "queue": info.Queue,
		"priority": job.Priority, "status_url": "/jobs/" + info.ID}
	if job.RunAt != nil {
		response["status"] = "scheduled"
		response["run_at"] = job.RunAt
	}
	writeJSON(w, http.StatusAccepted, response)
}

// lookupExistingTask inspects Asynq using the stable task ID. A matching
// task proves that enqueue happened, even when the API crashed before Confirm.
func (s *Server) lookupExistingTask(id string, job spec.Validated) (*asynq.TaskInfo, error) {
	for _, queue := range queues {
		info, err := s.Inspector.GetTaskInfo(queue, id)
		if errors.Is(err, asynq.ErrQueueNotFound) || errors.Is(err, asynq.ErrTaskNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(info.Payload, job.Body) || info.Type != job.Type {
			return nil, errors.New("idempotency key collided with an existing different task")
		}
		return info, nil
	}
	return nil, nil
}

func (s *Server) confirmObserved(r *http.Request, key string, body []byte) {
	ok, err := idempotency.ConfirmObserved(r.Context(), s.Gate.Redis, key, body)
	if err != nil || !ok {
		s.logger().WarnContext(r.Context(), "idempotency_repair_deferred",
			"error", err, "confirmed", ok)
	} else {
		s.logger().InfoContext(r.Context(), "idempotency_reservation_repaired")
	}
}

func (s *Server) writeDuplicate(w http.ResponseWriter, r *http.Request, existing *asynq.TaskInfo, job spec.Validated) {
	s.logger().InfoContext(r.Context(), "submission_deduplicated",
		"job_id", existing.ID, "queue", existing.Queue)
	w.Header().Set("Location", "/jobs/"+existing.ID)
	writeJSON(w, http.StatusOK, map[string]any{
		"job_id": existing.ID, "status": stateName(existing),
		"queue": existing.Queue, "priority": job.Priority, "deduplicated": true,
	})
}

func stateName(info *asynq.TaskInfo) string {
	switch info.State {
	case asynq.TaskStateActive:
		return "running"
	case asynq.TaskStateCompleted:
		return "succeeded"
	case asynq.TaskStateRetry:
		return "failed"
	case asynq.TaskStateArchived:
		return "dead_lettered"
	case asynq.TaskStateScheduled:
		return "scheduled"
	default:
		return "queued"
	}
}

func attempts(info *asynq.TaskInfo) int {
	switch info.State {
	case asynq.TaskStatePending, asynq.TaskStateScheduled, asynq.TaskStateRetry:
		return info.Retried
	default:
		return info.Retried + 1
	}
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if len(id) < 1 || len(id) > 128 {
		http.Error(w, "invalid job id", http.StatusBadRequest)
		return
	}
	for _, queue := range queues {
		info, err := s.Inspector.GetTaskInfo(queue, id)
		if errors.Is(err, asynq.ErrQueueNotFound) || errors.Is(err, asynq.ErrTaskNotFound) {
			continue
		}
		if err != nil {
			s.logger().ErrorContext(r.Context(), "job_inspection_failed",
				"job_id", id, "queue", queue, "error", err)
			http.Error(w, "queue temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		var job spec.Job
		_ = json.Unmarshal(info.Payload, &job)
		result := map[string]any{
			"job_id": info.ID, "type": info.Type, "queue": info.Queue,
			"priority": job.Priority, "status": stateName(info),
			"attempts": attempts(info), "max_retries": info.MaxRetry,
			"last_error": info.LastErr, "result": json.RawMessage("null"),
		}
		if len(info.Result) > 0 && json.Valid(info.Result) {
			result["result"] = json.RawMessage(info.Result)
		}
		if !info.NextProcessAt.IsZero() {
			result["next_run_at"] = info.NextProcessAt
		}
		if !info.CompletedAt.IsZero() {
			result["completed_at"] = info.CompletedAt
		}
		writeJSON(w, http.StatusOK, result)
		return
	}
	http.Error(w, "job not found (or retention expired)", http.StatusNotFound)
}

func (s *Server) depth(w http.ResponseWriter, r *http.Request) {
	stats, err := s.queueStats()
	if err != nil {
		s.logger().ErrorContext(r.Context(), "queue_depth_failed", "error", err)
		http.Error(w, "queue temporarily unavailable", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func (s *Server) queueStats() (map[string]any, error) {
	total, active, archived, completed := 0, 0, 0, 0
	byQueue := make(map[string]any, len(queues))
	existing, err := s.Inspector.Queues()
	if err != nil {
		return nil, err
	}
	present := make(map[string]bool, len(existing))
	for _, name := range existing {
		present[name] = true
	}
	for _, queue := range queues {
		if !present[queue] {
			byQueue[queue] = map[string]int{"waiting": 0, "active": 0, "archived": 0, "completed": 0}
			continue
		}
		info, err := s.Inspector.GetQueueInfo(queue)
		if errors.Is(err, asynq.ErrQueueNotFound) {
			byQueue[queue] = map[string]int{"waiting": 0, "active": 0, "archived": 0, "completed": 0}
			continue
		}
		if err != nil {
			return nil, err
		}
		waiting := info.Pending + info.Scheduled + info.Retry
		byQueue[queue] = map[string]int{
			"waiting": waiting, "active": info.Active,
			"archived": info.Archived, "completed": info.Completed,
		}
		total += waiting
		active += info.Active
		archived += info.Archived
		completed += info.Completed
	}
	return map[string]any{
		"waiting": total, "active": active, "archived": archived,
		"completed_retained": completed, "by_queue": byQueue,
		"note": "waiting=pending+scheduled+retry; does not count active",
	}, nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func (s *Server) adminAuthorized(w http.ResponseWriter, r *http.Request) bool {
	if s.AdminToken == "" {
		http.Error(w, "admin API disabled (set ADMIN_TOKEN)", http.StatusServiceUnavailable)
		return false
	}
	given := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if len(given) != len(s.AdminToken) ||
		subtle.ConstantTimeCompare([]byte(given), []byte(s.AdminToken)) != 1 {
		w.Header().Set("WWW-Authenticate", "Bearer")
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}

func (s *Server) adminDrain(w http.ResponseWriter, r *http.Request) {
	if !s.adminAuthorized(w, r) {
		return
	}
	if err := s.Gate.SetDraining(r.Context(), true); err != nil {
		s.logger().ErrorContext(r.Context(), "drain_failed", "error", err)
		http.Error(w, "could not enable drain", http.StatusServiceUnavailable)
		return
	}
	s.logger().WarnContext(r.Context(), "admission_draining")
	writeJSON(w, http.StatusAccepted, map[string]any{
		"draining": true, "message": "new submissions rejected; queued and active work continues",
	})
}

func (s *Server) adminResume(w http.ResponseWriter, r *http.Request) {
	if !s.adminAuthorized(w, r) {
		return
	}
	if err := s.Gate.SetDraining(r.Context(), false); err != nil {
		s.logger().ErrorContext(r.Context(), "resume_failed", "error", err)
		http.Error(w, "could not resume", http.StatusServiceUnavailable)
		return
	}
	s.logger().InfoContext(r.Context(), "admission_resumed")
	writeJSON(w, http.StatusOK, map[string]any{"draining": false})
}

func (s *Server) adminDrainStatus(w http.ResponseWriter, r *http.Request) {
	if !s.adminAuthorized(w, r) {
		return
	}
	draining, err := s.Gate.IsDraining(r.Context())
	if err != nil {
		http.Error(w, "redis unavailable", http.StatusServiceUnavailable)
		return
	}
	stats, err := s.queueStats()
	if err != nil {
		http.Error(w, "queue unavailable", http.StatusServiceUnavailable)
		return
	}
	waiting := stats["waiting"].(int)
	active := stats["active"].(int)
	writeJSON(w, http.StatusOK, map[string]any{
		"draining": draining, "drained": draining && waiting == 0 && active == 0,
		"waiting": waiting, "active": active,
	})
}

//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"example.com/go-async-jobs/internal/httpapi"
	"example.com/go-async-jobs/internal/metrics"
	"example.com/go-async-jobs/internal/operations"
	"example.com/go-async-jobs/internal/registry"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

func TestV2SchedulingAndMetrics(t *testing.T) {
	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("REDIS_TEST_ADDR is required")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: 11})
	defer rdb.Close()
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	defer rdb.FlushDB(ctx)
	opt := asynq.RedisClientOpt{Addr: addr, DB: 11}
	client := asynq.NewClient(opt)
	defer client.Close()
	inspector := asynq.NewInspector(opt)
	defer inspector.Close()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	collector := &metrics.Collector{Redis: rdb, Inspector: inspector, Logger: log}
	api := httptest.NewServer((&httpapi.Server{Client: client, Inspector: inspector, Logger: log, Gate: operations.Gate{Redis: rdb}, AdminToken: "metrics-secret", Metrics: collector}).Routes())
	defer api.Close()
	future := time.Now().Add(2 * time.Second).UTC().Format(time.RFC3339Nano)
	body := fmt.Sprintf("{\"type\":\"demo.sleep\",\"priority\":9,\"max_retries\":0,\"payload\":{\"duration_ms\":10},\"run_at\":%q}", future)
	res, err := http.Post(api.URL+"/jobs", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 202 {
		t.Fatalf("schedule POST returned %d", res.StatusCode)
	}
	// Extract task ID without introducing a dependency on HTTP JSON body shapes.
	var decoded struct {
		ID     string `json:"job_id"`
		Status string `json:"status"`
	}
	if err := json.NewDecoder(res.Body).Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ID == "" || decoded.Status != "scheduled" {
		t.Fatalf("unexpected response %+v", decoded)
	}
	taskInfo, err := inspector.GetTaskInfo("critical", decoded.ID)
	if err != nil || taskInfo.State != asynq.TaskStateScheduled {
		t.Fatalf("not scheduled: %+v %v", taskInfo, err)
	}
	srv := asynq.NewServer(opt, asynq.Config{Concurrency: 2, Queues: map[string]int{"critical": 1}, DelayedTaskCheckInterval: 100 * time.Millisecond, TaskCheckInterval: 100 * time.Millisecond})
	mux := asynq.NewServeMux()
	registry.Register(mux, registry.Definitions(rdb))
	mux.Use(collector.Middleware())
	if err := srv.Start(mux); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown()
	passed := false
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		info, err := inspector.GetTaskInfo("critical", decoded.ID)
		if err == nil && info.State == asynq.TaskStateCompleted {
			passed = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !passed {
		t.Fatal("scheduled job did not complete")
	}
	// Authenticated metrics must show the histogram and worker capacity.
	workerCtx, stopWorker := context.WithCancel(ctx)
	collector.Heartbeat(workerCtx, 2)
	defer stopWorker()
	collector.RecordAttempt("critical", 80*time.Millisecond, true)
	request, _ := http.NewRequest("GET", api.URL+"/metrics", nil)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 401 {
		t.Fatalf("metrics without auth: %d", response.StatusCode)
	}
	request, _ = http.NewRequest("GET", api.URL+"/metrics", nil)
	request.Header.Set("Authorization", "Bearer metrics-secret")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatalf("metrics with auth: %d", response.StatusCode)
	}
	contents, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"go_jobs_queue_waiting{queue=\"critical\"}", "go_jobs_attempt_duration_seconds_bucket{queue=\"critical\",le=\"0.1\"}", "go_jobs_worker_capacity 2", "go_jobs_worker_utilization", "go_jobs_dead_lettered_total"} {
		if !strings.Contains(string(contents), want) {
			t.Errorf("metrics missing %s", want)
		}
	}
}

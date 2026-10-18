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
	"example.com/go-async-jobs/internal/operations"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

func TestDelayedIdempotentReplayAfterRunAt(t *testing.T) {
	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("set REDIS_TEST_ADDR")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: 7})
	defer rdb.Close()
	ctx := context.Background()
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	defer rdb.FlushDB(ctx)
	opts := asynq.RedisClientOpt{Addr: addr, DB: 7}
	client := asynq.NewClient(opts)
	defer client.Close()
	inspector := asynq.NewInspector(opts)
	defer inspector.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := httptest.NewServer((&httpapi.Server{Client: client, Inspector: inspector, Logger: logger, Gate: operations.Gate{Redis: rdb}}).Routes())
	defer server.Close()
	runTime := time.Now().UTC().Add(2 * time.Second)
	when := runTime.Format(time.RFC3339Nano)
	requestBody := func(at, payload string) string {
		return fmt.Sprintf("{\"type\":\"demo.sleep\",\"priority\":9,\"max_retries\":0,\"payload\":%s,\"run_at\":%q}", payload, at)
	}
	body := requestBody(when, "{}")
	send := func(key, text string) (int, string, bool) {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, server.URL+"/jobs", strings.NewReader(text))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		if key != "" {
			req.Header.Set("Idempotency-Key", key)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		raw, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		var obj struct {
			ID           string `json:"job_id"`
			Deduplicated bool   `json:"deduplicated"`
		}
		_ = json.Unmarshal(raw, &obj)
		return res.StatusCode, obj.ID, obj.Deduplicated
	}
	key := "delayed-replay-accepted"
	code, id, dupe := send(key, body)
	if code != 202 || id == "" || dupe {
		t.Fatalf("initial submit: code=%d id=%q dupe=%t", code, id, dupe)
	}
	code, replayedID, dupe := send(key, body)
	if code != 200 || replayedID != id || !dupe {
		t.Fatalf("pre-deadline retry code=%d id=%q dupe=%t", code, replayedID, dupe)
	}
	if delay := time.Until(runTime.Add(150 * time.Millisecond)); delay > 0 {
		time.Sleep(delay)
	}
	code, replayedID, dupe = send(key, body)
	if code != 200 || replayedID != id || !dupe {
		t.Fatalf("post-deadline retry code=%d id=%q dupe=%t", code, replayedID, dupe)
	}

	// Equivalent instant with a different RFC3339 timezone must fingerprint
	// exactly like the original accepted request.
	offsetTime := runTime.In(time.FixedZone("IST", 19800)).Format(time.RFC3339Nano)
	code, replayedID, dupe = send(key, requestBody(offsetTime, "{}"))
	if code != 200 || replayedID != id || !dupe {
		t.Fatalf("timezone replay code=%d id=%q dupe=%t", code, replayedID, dupe)
	}

	// Conflicts are still 409, even when both run_at timestamps have passed.
	code, _, _ = send(key, requestBody(when, `{"duration_ms":20}`))
	if code != 409 {
		t.Fatalf("changed payload returned %d, expected 409", code)
	}
	old := runTime.Add(-time.Minute).Format(time.RFC3339Nano)
	code, _, _ = send(key, requestBody(old, "{}"))
	if code != 409 {
		t.Fatalf("changed past run_at returned %d, expected 409", code)
	}

	code, _, _ = send("", body)
	if code != 400 {
		t.Fatalf("new past-dated request without key: %d", code)
	}
	code, _, _ = send("unused-past-key", body)
	if code != 400 {
		t.Fatalf("new past-dated request with key: %d", code)
	}

	if err := inspector.DeleteTask("critical", id); err != nil {
		t.Fatalf("delete task for expiration scenario: %v", err)
	}
	code, _, _ = send(key, body)
	if code != 410 {
		t.Fatalf("committed task absent returned %d, expected 410", code)
	}
}

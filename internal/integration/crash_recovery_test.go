//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

const crashQueue = "crash-recovery"
const crashJobType = "test.worker-crash"

// This helper is executed as an OS subprocess, not a goroutine in the test process.
func TestWorkerCrashHelper(t *testing.T) {
	if os.Getenv("ASYNQ_CRASH_HELPER") != "yes" {
		return
	}
	addr := os.Getenv("REDIS_TEST_ADDR")
	opt := asynq.RedisClientOpt{Addr: addr, DB: 13}
	client := redis.NewClient(&redis.Options{Addr: addr, DB: 13})
	defer client.Close()
	mux := asynq.NewServeMux()
	mux.HandleFunc(crashJobType, func(ctx context.Context, task *asynq.Task) error {
		id, ok := asynq.GetTaskID(ctx)
		if !ok {
			return fmt.Errorf("missing job ID")
		}
		if _, err := client.SetNX(ctx, "crash:effect:"+id, "committed", time.Hour).Result(); err != nil {
			return err
		}
		if err := client.Incr(ctx, "crash:attempts:"+id).Err(); err != nil {
			return err
		}
		if err := client.Set(ctx, "crash:entered:"+id, "1", time.Hour).Err(); err != nil {
			return err
		}
		// Parent sends SIGKILL while blocked here, after the effect but before ACK.
		<-ctx.Done()
		return ctx.Err()
	})
	srv := asynq.NewServer(opt, asynq.Config{
		Concurrency: 1, Queues: map[string]int{crashQueue: 1},
		TaskCheckInterval: 100 * time.Millisecond, DelayedTaskCheckInterval: 100 * time.Millisecond,
	})
	if err := srv.Run(mux); err != nil {
		t.Fatal(err)
	}
}

func TestRealWorkerProcessCrashRecovery(t *testing.T) {
	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("set REDIS_TEST_ADDR")
	}
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: 13})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	// DB13 is intentionally disposable and isolated from the other integration suite (DB14).
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	defer rdb.FlushDB(ctx)
	opt := asynq.RedisClientOpt{Addr: addr, DB: 13}
	sender := asynq.NewClient(opt)
	defer sender.Close()
	inspector := asynq.NewInspector(opt)
	defer inspector.Close()
	info, err := sender.Enqueue(asynq.NewTask(crashJobType, []byte(`{"crash_test":true}`)),
		asynq.Queue(crashQueue), asynq.MaxRetry(5), asynq.Timeout(20*time.Second),
		asynq.Retention(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	id := info.ID

	child := exec.Command(os.Args[0], "-test.run=^TestWorkerCrashHelper$", "-test.v")
	child.Env = append(os.Environ(), "ASYNQ_CRASH_HELPER=yes")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if child.ProcessState == nil {
			_ = child.Process.Kill()
			_ = child.Wait()
		}
	}()

	entered := false
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); {
		n, e := rdb.Exists(ctx, "crash:entered:"+id).Result()
		if e != nil {
			t.Fatal(e)
		}
		if n == 1 {
			entered = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !entered {
		t.Fatal("worker never entered the handler before crash")
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = child.Wait()
	t.Logf("SIGKILL sent after Redis effect for task %s, before Asynq completion ACK", id)

	// A new real Asynq server recovers the abandoned active task.
	recovered := asynq.NewServer(opt, asynq.Config{
		Concurrency: 1, Queues: map[string]int{crashQueue: 1},
		TaskCheckInterval: 100 * time.Millisecond, DelayedTaskCheckInterval: 100 * time.Millisecond,
		RetryDelayFunc: func(int, error, *asynq.Task) time.Duration { return 100 * time.Millisecond },
	})
	mux := asynq.NewServeMux()
	mux.HandleFunc(crashJobType, func(ctx context.Context, task *asynq.Task) error {
		jobID, ok := asynq.GetTaskID(ctx)
		if !ok {
			return fmt.Errorf("missing recovered ID")
		}
		first, err := rdb.SetNX(ctx, "crash:effect:"+jobID, "committed", time.Hour).Result()
		if err != nil {
			return err
		}
		if first {
			return fmt.Errorf("business effect duplicated, dedupe record unexpectedly absent")
		}
		if err := rdb.Incr(ctx, "crash:attempts:"+jobID).Err(); err != nil {
			return err
		}
		result, _ := json.Marshal(map[string]any{"job_id": jobID, "recovered": true, "duplicate_effect": false})
		_, err = task.ResultWriter().Write(result)
		return err
	})
	if err := recovered.Start(mux); err != nil {
		t.Fatal(err)
	}
	defer recovered.Shutdown()

	for deadline := time.Now().Add(150 * time.Second); time.Now().Before(deadline); {
		state, err := inspector.GetTaskInfo(crashQueue, id)
		if err == nil && state.State == asynq.TaskStateCompleted {
			n, err := rdb.Get(ctx, "crash:attempts:"+id).Int()
			if err != nil {
				t.Fatal(err)
			}
			if n != 2 {
				t.Fatalf("expected 2 executions, got %d", n)
			}
			if len(state.Result) == 0 {
				t.Fatal("recovered job missing result")
			}
			t.Logf("Recovered and completed %s: attempts=%d, effect stored once", id, n)
			return
		}
		if err == nil && state.State == asynq.TaskStateArchived {
			t.Fatalf("crash job was archived rather than completed: %s", state.LastErr)
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("Asynq did not recover task %s after SIGKILL within 150 seconds", id)
}

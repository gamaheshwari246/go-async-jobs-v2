//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

// Two independent scheduler clients racing on the same logical occurrence
// must enqueue exactly one task. A restarted replica must see the same ID.
func TestConcurrentSchedulerTickDeduplication(t *testing.T) {
	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("set REDIS_TEST_ADDR")
	}
	opt := asynq.RedisClientOpt{Addr: addr, DB: 8}
	redisClient := redis.NewClient(&redis.Options{Addr: addr, DB: 8})
	defer redisClient.Close()
	ctx := context.Background()
	if err := redisClient.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	defer redisClient.FlushDB(ctx)
	config, err := loadSchedule(func(k string) string {
		if k == "CRON_SCHEDULE" {
			return "@every 3s"
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	tick := time.Now().UTC().Truncate(3 * time.Second).Add(3 * time.Second)
	const schedulers = 12
	var started sync.WaitGroup
	var wins atomic.Int32
	errs := make(chan error, schedulers)
	for n := 0; n < schedulers; n++ {
		started.Add(1)
		go func() {
			defer started.Done()
			client := asynq.NewClient(opt)
			defer client.Close()
			created, err := enqueueTick(ctx, client, config, tick)
			if err != nil {
				errs <- err
				return
			}
			if created {
				wins.Add(1)
			}
		}()
	}
	started.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("enqueuing tick: %v", err)
	}
	if wins.Load() != 1 {
		t.Fatalf("expected 1 winner, got %d", wins.Load())
	}
	inspector := asynq.NewInspector(opt)
	defer inspector.Close()
	info, err := inspector.GetTaskInfo(config.Job.Queue, taskID(config, tick))
	if err != nil || info.ID != taskID(config, tick) {
		t.Fatalf("tick not retained: %+v %v", info, err)
	}
	// Recreate the client to simulate a scheduler process restart.
	restarted := asynq.NewClient(opt)
	defer restarted.Close()
	created, err := enqueueTick(ctx, restarted, config, tick)
	if err != nil || created {
		t.Fatalf("restart repeated tick: created=%v err=%v", created, err)
	}
	next := tick.Add(3 * time.Second)
	created, err = enqueueTick(ctx, restarted, config, next)
	if err != nil || !created {
		t.Fatalf("next tick not accepted: created=%v err=%v", created, err)
	}
	if _, err := inspector.GetTaskInfo(config.Job.Queue, taskID(config, next)); err != nil {
		t.Fatal(fmt.Errorf("second tick: %w", err))
	}
}

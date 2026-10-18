package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"example.com/go-async-jobs/internal/registry"
	"example.com/go-async-jobs/internal/spec"
	"github.com/hibiken/asynq"
	"github.com/robfig/cron/v3"
)

type scheduleConfig struct {
	Cron string
	Job  spec.Validated
}

func loadSchedule(getenv func(string) string) (scheduleConfig, error) {
	rawCron := getenv("CRON_SCHEDULE")
	if rawCron == "" {
		rawCron = "*/1 * * * *"
	}
	taskType := getenv("CRON_TASK_TYPE")
	if taskType == "" {
		taskType = "demo.sleep"
	}
	rawPayload := getenv("CRON_PAYLOAD")
	if rawPayload == "" {
		rawPayload = "{}"
	}
	priority := 5
	if raw := getenv("CRON_PRIORITY"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			return scheduleConfig{}, fmt.Errorf("CRON_PRIORITY: %w", err)
		}
		priority = n
	}
	if !json.Valid([]byte(rawPayload)) {
		return scheduleConfig{}, fmt.Errorf("invalid CRON_PAYLOAD JSON")
	}
	req := spec.SubmitRequest{Type: taskType, Payload: json.RawMessage(rawPayload), Priority: &priority}
	job, err := spec.Validate(req)
	if err != nil {
		return scheduleConfig{}, err
	}
	if err := registry.Validate(registry.Definitions(nil), taskType, req.Payload); err != nil {
		return scheduleConfig{}, err
	}
	if _, err := parseSchedule(rawCron); err != nil {
		return scheduleConfig{}, fmt.Errorf("CRON_SCHEDULE: %w", err)
	}
	return scheduleConfig{Cron: rawCron, Job: job}, nil
}

// @every normally uses process-start relative ticks. Using an epoch anchor
// ensures independent replicas agree on the same logical occurrence.
type epochInterval struct{ period time.Duration }

func (s epochInterval) Next(after time.Time) time.Time {
	step := int64(s.period)
	next := (after.UTC().UnixNano()/step + 1) * step
	return time.Unix(0, next).UTC()
}
// All cron expressions are evaluated against UTC, even when tests or
// different scheduler replicas supply time.Time values in different zones.
type utcSchedule struct {
	cron.Schedule
}

func (s utcSchedule) Next(after time.Time) time.Time {
	return s.Schedule.Next(after.UTC()).UTC()
}

func parseSchedule(expr string) (cron.Schedule, error) {
	if strings.HasPrefix(expr, "@every ") {
		period, err := time.ParseDuration(strings.TrimSpace(strings.TrimPrefix(expr, "@every ")))
		if err != nil {
			return nil, err
		}
		if period < time.Second || period > 30*24*time.Hour {
			return nil, fmt.Errorf("@every must be between 1s and 30d")
		}
		return epochInterval{period: period}, nil
	}
	parsed, err := cron.ParseStandard(expr)
	if err != nil {
		return nil, err
	}
	return utcSchedule{parsed}, nil
}

func taskID(cfg scheduleConfig, tick time.Time) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%s\x00%d\x00%d\x00%d",
		cfg.Cron, cfg.Job.Type, cfg.Job.Body, cfg.Job.Priority,
		cfg.Job.Retries, cfg.Job.Timeout.Nanoseconds())))
	return fmt.Sprintf("cron-%s-%d", hex.EncodeToString(sum[:12]), tick.UTC().UnixNano())
}

// Asynq atomically rejects duplicate task IDs across scheduler processes.
func enqueueTick(ctx context.Context, client *asynq.Client, cfg scheduleConfig, tick time.Time) (bool, error) {
	_, err := client.EnqueueContext(ctx, asynq.NewTask(cfg.Job.Type, cfg.Job.Body),
		asynq.Queue(cfg.Job.Queue), asynq.TaskID(taskID(cfg, tick)),
		asynq.MaxRetry(cfg.Job.Retries), asynq.Timeout(cfg.Job.Timeout),
		asynq.Retention(48*time.Hour))
	if errors.Is(err, asynq.ErrTaskIDConflict) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func run(ctx context.Context, client *asynq.Client, cfg scheduleConfig, logger *slog.Logger) error {
	schedule, err := parseSchedule(cfg.Cron)
	if err != nil {
		return err
	}
	tick := schedule.Next(time.Now().UTC())
	logger.Info("cron_scheduler_started", "cron", cfg.Cron, "queue", cfg.Job.Queue, "timezone", "UTC", "first_tick", tick)
	for {
		wait := time.Until(tick)
		if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil
			case <-timer.C:
			}
		}
		// Retry the same occurrence after transient Redis errors. There is no
		// separate pre-enqueue claim that could become stranded on a crash.
		for {
			created, err := enqueueTick(ctx, client, cfg, tick)
			if err == nil {
				if created {
					logger.Info("cron_enqueued", "job_id", taskID(cfg, tick), "queue", cfg.Job.Queue, "tick", tick)
				}
				if !created {
					logger.Info("cron_tick_deduplicated", "job_id", taskID(cfg, tick), "tick", tick)
				}
				break
			}
			logger.Warn("cron_enqueue_failed", "tick", tick, "error", err)
			timer := time.NewTimer(time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil
			case <-timer.C:
			}
		}
		tick = schedule.Next(tick)
	}
}
func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	cfg, err := loadSchedule(os.Getenv)
	if err != nil {
		logger.Error("invalid_schedule_config", "error", err)
		os.Exit(2)
	}
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: addr})
	defer client.Close()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, client, cfg, logger); err != nil {
		logger.Error("cron_scheduler_stopped", "error", err)
		os.Exit(1)
	}
	logger.Info("cron_scheduler_stopped")
}

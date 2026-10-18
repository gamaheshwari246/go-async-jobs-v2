package main

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"example.com/go-async-jobs/internal/backoff"
	"example.com/go-async-jobs/internal/health"
	"example.com/go-async-jobs/internal/metrics"
	"example.com/go-async-jobs/internal/observability"
	"example.com/go-async-jobs/internal/registry"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}

	concurrency := 5
	if raw := os.Getenv("WORKER_CONCURRENCY"); raw != "" {
		number, err := strconv.Atoi(raw)
		if err != nil || number < 1 || number > 100 {
			logger.Error("invalid_worker_concurrency", "value", raw, "allowed", "1..100")
			os.Exit(2)
		}
		concurrency = number
	}

	readiness := &health.WorkerReadiness{}

	listen, err := net.Listen("tcp", workerHealthAddress())
	if err != nil {
		logger.Error("worker_health_bind_failed", "error", err)
		os.Exit(1)
	}
	healthServer := &http.Server{Handler: readiness.Routes(), ReadHeaderTimeout: 3 * time.Second}
	go func() {
		if err := healthServer.Serve(listen); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("worker_health_server_stopped", "error", err)
		}
	}()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = healthServer.Shutdown(ctx)
	}()

	redisClient := redis.NewClient(&redis.Options{Addr: addr})
	defer redisClient.Close()
	collector := &metrics.Collector{Redis: redisClient, Logger: logger}
	workerCtx, stopWorker := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stopWorker()
	collector.Heartbeat(workerCtx, concurrency)
	config := workerConfig(concurrency, logger, readiness.Observe)
	config.ErrorHandler = collector.ErrorHandler()
	server := asynq.NewServer(asynq.RedisClientOpt{Addr: addr}, config)

	mux := asynq.NewServeMux()
	registry.Register(mux, registry.Definitions(redisClient))
	mux.Use(observability.TaskMiddleware(logger))
	mux.Use(collector.Middleware())

	logger.Info("worker_starting", "redis_addr", addr, "concurrency", concurrency,
		"queue_weights", "critical:6 default:3 low:1", "health_addr", listen.Addr().String())
	if err := server.Run(mux); err != nil {
		logger.Error("worker_stopped", "error", err)
		os.Exit(1)
	}
	logger.Info("worker_shutdown")
}

func workerHealthAddress() string {
	if value := os.Getenv("WORKER_HEALTH_ADDR"); value != "" {
		return value
	}
	return ":8081"
}

func workerConfig(concurrency int, logger *slog.Logger, healthCheck func(error)) asynq.Config {
	return asynq.Config{
		Concurrency: concurrency,
		Queues: map[string]int{
			"critical": 6,
			"default":  3,
			"low":      1,
		},
		StrictPriority:           false,
		DelayedTaskCheckInterval: time.Second,
		HealthCheckInterval:      5 * time.Second,
		HealthCheckFunc:          healthCheck,
		ShutdownTimeout:          330 * time.Second,
		RetryDelayFunc: func(retry int, err error, task *asynq.Task) time.Duration {
			delay := backoff.Delay(retry, rand.Float64())
			logger.Warn("job_retry_scheduled",
				"task_type", task.Type(),
				"retry_number", retry,
				"delay", delay.String(),
				"error", err,
			)
			return delay
		},
	}
}

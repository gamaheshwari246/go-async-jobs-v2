package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"example.com/go-async-jobs/internal/httpapi"
	"example.com/go-async-jobs/internal/metrics"
	"example.com/go-async-jobs/internal/operations"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

func getenv(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	redisAddress := getenv("REDIS_ADDR", "127.0.0.1:6379")
	redisOptions := asynq.RedisClientOpt{Addr: redisAddress}
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddress})
	defer redisClient.Close()

	client := asynq.NewClient(redisOptions)
	defer client.Close()
	inspector := asynq.NewInspector(redisOptions)
	defer inspector.Close()

	httpServer := &http.Server{
		Addr: getenv("HTTP_ADDR", ":8080"),
		Handler: (&httpapi.Server{
			Client: client, Inspector: inspector, Logger: logger,
			Gate: operations.Gate{Redis: redisClient}, AdminToken: os.Getenv("ADMIN_TOKEN"),
			Metrics: &metrics.Collector{Redis: redisClient, Inspector: inspector, Logger: logger},
		}).Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		logger.Info("http_shutdown_requested")
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("http_shutdown_failed", "error", err)
		}
	}()

	logger.Info("http_server_starting", "addr", httpServer.Addr, "redis_addr", redisAddress)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("http_server_stopped", "error", err)
		os.Exit(1)
	}
	logger.Info("http_server_stopped")
}

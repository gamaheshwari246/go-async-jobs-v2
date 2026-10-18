package idempotency

import (
	"context"
	"github.com/redis/go-redis/v9"
	"testing"
)

// Database-backed concurrency behavior is exercised by integration tests.
func TestUnknownRedisReturnsError(t *testing.T) {
	c := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer c.Close()
	_, err := Reserve(context.Background(), c, "key", []byte("payload"))
	if err == nil {
		t.Fatal("expected Redis unavailable error")
	}
}

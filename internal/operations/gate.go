package operations

import (
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
)

const drainKey = "go-async-jobs:draining"

type Gate struct{ Redis redis.UniversalClient }

func (g Gate) IsDraining(ctx context.Context) (bool, error) {
	value, err := g.Redis.Get(ctx, drainKey).Result()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return value == "1", nil
}
func (g Gate) SetDraining(ctx context.Context, draining bool) error {
	if draining {
		return g.Redis.Set(ctx, drainKey, "1", 0).Err()
	}
	return g.Redis.Del(ctx, drainKey).Err()
}

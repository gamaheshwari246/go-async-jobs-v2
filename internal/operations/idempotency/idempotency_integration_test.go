//go:build integration

package idempotency

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// These tests use disposable Redis database 12. Do not use a production Redis!
func redisForTest(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("set REDIS_TEST_ADDR to run real-Redis reservation tests")
	}
	client := redis.NewClient(&redis.Options{Addr: addr, DB: 12})
	if err := client.Ping(context.Background()).Err(); err != nil {
		client.Close()
		t.Fatal(err)
	}
	if err := client.FlushDB(context.Background()).Err(); err != nil {
		client.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.FlushDB(context.Background()).Err()
		_ = client.Close()
	})
	return client
}

func TestReservationDecisionTable(t *testing.T) {
	client := redisForTest(t)
	ctx := context.Background()
	payload := []byte("validated-job-body")
	cases := []struct {
		name    string
		prepare func(string)
		want    Decision
	}{
		{name: "new", want: New},
		{name: "live claim", prepare: func(key string) {
			if _, err := Reserve(ctx, client, key, payload); err != nil {
				t.Fatal(err)
			}
		}, want: InProgress},
		{name: "conflicting fingerprint", prepare: func(key string) {
			if _, err := Reserve(ctx, client, key, []byte("different-job")); err != nil {
				t.Fatal(err)
			}
		}, want: Conflict},
		{name: "confirmed", prepare: func(key string) {
			claim, err := Reserve(ctx, client, key, payload)
			if err != nil {
				t.Fatal(err)
			}
			confirmed, err := Confirm(ctx, client, key, payload, claim)
			if err != nil || !confirmed {
				t.Fatalf("confirm: %v %v", confirmed, err)
			}
		}, want: Existing},
		{name: "expired lease reclaim", prepare: func(key string) {
			if _, err := reserveWithLease(ctx, client, key, payload, 60*time.Millisecond); err != nil {
				t.Fatal(err)
			}
			time.Sleep(120 * time.Millisecond)
		}, want: New},
		{name: "observed enqueue repaired", prepare: func(key string) {
			if _, err := Reserve(ctx, client, key, payload); err != nil {
				t.Fatal(err)
			}
			ok, err := ConfirmObserved(ctx, client, key, payload)
			if err != nil || !ok {
				t.Fatalf("observed confirmation: %v %v", ok, err)
			}
		}, want: Existing},
		{name: "legacy string upgraded", prepare: func(key string) {
			if err := client.Set(ctx, redisKey(key), fingerprint(payload), time.Hour).Err(); err != nil {
				t.Fatal(err)
			}
		}, want: New},
	}
	for n, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			key := fmt.Sprintf("reservation-table-%d", n)
			if test.prepare != nil {
				test.prepare(key)
			}
			claim, err := Reserve(ctx, client, key, payload)
			if err != nil {
				t.Fatal(err)
			}
			if claim.Decision != test.want {
				t.Fatalf("decision = %v, want %v", claim.Decision, test.want)
			}
		})
	}
}

func TestStaleLeaseOwnerCannotConfirm(t *testing.T) {
	client := redisForTest(t)
	ctx := context.Background()
	key, payload := "owner-takeover", []byte("payload")
	old, err := reserveWithLease(ctx, client, key, payload, 60*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(120 * time.Millisecond)
	current, err := Reserve(ctx, client, key, payload)
	if err != nil || current.Decision != New {
		t.Fatalf("lease not reclaimed: %+v %v", current, err)
	}
	ok, err := Confirm(ctx, client, key, payload, old)
	if err != nil || ok {
		t.Fatalf("stale owner confirmed: %v %v", ok, err)
	}
	ok, err = Confirm(ctx, client, key, payload, current)
	if err != nil || !ok {
		t.Fatalf("active owner did not confirm: %v %v", ok, err)
	}
	again, err := Reserve(ctx, client, key, payload)
	if err != nil || again.Decision != Existing {
		t.Fatalf("committed job not recognized: %+v %v", again, err)
	}
}

func TestConcurrentClaimOnlyOneOwner(t *testing.T) {
	client := redisForTest(t)
	ctx := context.Background()
	const callers = 24
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make(chan Decision, callers)
	errors := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			claim, err := Reserve(ctx, client, "concurrent-key", []byte("same"))
			if err != nil {
				errors <- err
				return
			}
			results <- claim.Decision
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errors)
	for err := range errors {
		t.Errorf("reservation failed: %v", err)
	}
	newCount, waitingCount := 0, 0
	for decision := range results {
		switch decision {
		case New:
			newCount++
		case InProgress:
			waitingCount++
		default:
			t.Errorf("unexpected decision %v", decision)
		}
	}
	if newCount != 1 || waitingCount != callers-1 {
		t.Fatalf("owners=%d, waiting=%d, want one owner", newCount, waitingCount)
	}
}

// Package idempotency coordinates optional HTTP submission retries across
// all Asynq priority queues using a recoverable Redis reservation.
package idempotency

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Decision is the result of atomically inspecting and claiming a submission key.
type Decision int

const (
	New        Decision = iota // Caller owns a short lease and may enqueue.
	Existing                   // The job was previously confirmed in Asynq.
	Conflict                   // Same key used for a different job configuration.
	InProgress                 // Another caller holds a live enqueue lease.
)

const (
	lifetime     = 48 * time.Hour
	defaultLease = 5 * time.Second
	keyPrefix    = "go-async-jobs:submission-idem:"
)

// Claim identifies the owner of an unfinished enqueue. Keep Token private to
// the submission code; a stale caller must not confirm another caller's claim.
type Claim struct {
	Decision Decision
	Token    string
}

// TaskID must be identical on every request retry, even across API processes.
func TaskID(key string) string {
	sum := sha256.Sum256([]byte(key))
	return "idem-" + hex.EncodeToString(sum[:])
}

func redisKey(key string) string {
	return keyPrefix + TaskID(key)[len("idem-"):]
}

func fingerprint(body []byte) string {
	hash := sha256.Sum256(body)
	return hex.EncodeToString(hash[:])
}

var reserveScript = redis.NewScript(`
local key = KEYS[1]
local fingerprint, token = ARGV[1], ARGV[2]
local lease_ms, ttl_ms = tonumber(ARGV[3]), tonumber(ARGV[4])
local clock = redis.call('TIME')
local now_ms = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)

-- Upgrade reservations produced by the original SETNX implementation.
-- A same-fingerprint legacy key can be inspected/repaired by the caller.
local kind = redis.call('TYPE', key).ok
if kind == 'string' then
  if redis.call('GET', key) ~= fingerprint then return 2 end
  local old_ttl = redis.call('PTTL', key)
  redis.call('DEL', key)
  redis.call('HSET', key, 'fingerprint', fingerprint,
    'state', 'pending', 'owner', token, 'lease_until', now_ms + lease_ms)
  redis.call('PEXPIRE', key, old_ttl > 0 and old_ttl or ttl_ms)
  return 0
end

if kind == 'none' then
  redis.call('HSET', key, 'fingerprint', fingerprint,
    'state', 'pending', 'owner', token, 'lease_until', now_ms + lease_ms)
  redis.call('PEXPIRE', key, ttl_ms)
  return 0
end

if redis.call('HGET', key, 'fingerprint') ~= fingerprint then return 2 end
if redis.call('HGET', key, 'state') == 'committed' then return 1 end

local until_ms = tonumber(redis.call('HGET', key, 'lease_until')) or 0
if now_ms < until_ms then return 3 end

-- Crash recovery: a later identical retry may take over an expired claim.
-- It uses the SAME queue and deterministic Asynq ID, preventing duplication.
redis.call('HSET', key, 'owner', token, 'lease_until', now_ms + lease_ms)
return 0
`)

var confirmScript = redis.NewScript(`
if redis.call('TYPE', KEYS[1]).ok ~= 'hash' then return 0 end
if redis.call('HGET', KEYS[1], 'fingerprint') ~= ARGV[1] then return 0 end
if redis.call('HGET', KEYS[1], 'state') == 'committed' then return 1 end
if ARGV[3] ~= 'observed' and redis.call('HGET', KEYS[1], 'owner') ~= ARGV[2] then
 return 0
end
redis.call('HSET', KEYS[1], 'state', 'committed')
return 1
`)

// Reserve claims the submission for a short lease. An interrupted caller does
// not block the same logical request for the entire 48-hour retention period.
func Reserve(ctx context.Context, client redis.UniversalClient, key string, body []byte) (Claim, error) {
	return reserveWithLease(ctx, client, key, body, defaultLease)
}

func reserveWithLease(ctx context.Context, client redis.UniversalClient, key string, body []byte, lease time.Duration) (Claim, error) {
	var claim Claim
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return claim, fmt.Errorf("generate idempotency lease token: %w", err)
	}
	claim.Token = hex.EncodeToString(nonce[:])
	result, err := reserveScript.Run(ctx, client, []string{redisKey(key)}, fingerprint(body),
		claim.Token, lease.Milliseconds(), lifetime.Milliseconds()).Int()
	if err != nil {
		return Claim{}, fmt.Errorf("reserve idempotency key: %w", err)
	}
	claim.Decision = Decision(result)
	return claim, nil
}

// Confirm marks a successfully enqueued task as present in Asynq. A stale lease
// holder cannot overwrite a newer claim.
func Confirm(ctx context.Context, client redis.UniversalClient, key string, body []byte, claim Claim) (bool, error) {
	return confirm(ctx, client, key, body, claim.Token, "owner")
}

// ConfirmObserved repairs the crash window where Asynq accepted the task but
// the API died before Confirm. Call ONLY after inspecting the matching task.
func ConfirmObserved(ctx context.Context, client redis.UniversalClient, key string, body []byte) (bool, error) {
	return confirm(ctx, client, key, body, "", "observed")
}

func confirm(ctx context.Context, client redis.UniversalClient, key string, body []byte, token, mode string) (bool, error) {
	n, err := confirmScript.Run(ctx, client, []string{redisKey(key)}, fingerprint(body), token, mode).Int()
	return n == 1, err
}

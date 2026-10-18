// Package backoff defines bounded exponential retry delays with jitter.
package backoff

import "time"

const (
	BaseDelay = time.Second
	MaxDelay  = time.Minute
)

// Delay returns 1s, 2s, 4s, ... 60s with up to 20% positive jitter.
// The result is capped at 60s, including jitter.
func Delay(retryNumber int, jitter float64) time.Duration {
	if retryNumber < 1 {
		retryNumber = 1
	}
	if jitter < 0 {
		jitter = 0
	}
	if jitter > 1 {
		jitter = 1
	}
	exponent := retryNumber - 1
	if exponent > 6 {
		exponent = 6
	}
	delay := BaseDelay * time.Duration(1<<exponent)
	if delay > MaxDelay {
		delay = MaxDelay
	}
	delay += time.Duration(float64(delay) * 0.2 * jitter)
	if delay > MaxDelay {
		return MaxDelay
	}
	return delay
}

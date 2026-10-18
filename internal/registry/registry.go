// Package registry declares installed job types, payload validators and handlers
// in one place. Both the API and worker load this same catalog.
package registry

import (
	"bytes"
	"encoding/json"
	"fmt"

	"example.com/go-async-jobs/internal/handlers"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

type Definition struct {
	Validate func(json.RawMessage) error
	Handle   asynq.HandlerFunc
}

func Definitions(redisClient redis.UniversalClient) map[string]Definition {
	return map[string]Definition{
		"demo.sleep":  {Validate: validateSleep, Handle: handlers.DemoSleep},
		"demo.effect": {Validate: validateEffect, Handle: handlers.EffectHandler(redisClient)},
	}
}

func Validate(defs map[string]Definition, jobType string, payload json.RawMessage) error {
	definition, exists := defs[jobType]
	if !exists {
		return fmt.Errorf("unknown job type %q", jobType)
	}
	if definition.Validate == nil {
		return nil
	}
	return definition.Validate(payload)
}

func Register(mux *asynq.ServeMux, defs map[string]Definition) {
	for jobType, definition := range defs {
		mux.HandleFunc(jobType, definition.Handle)
	}
}

func validateSleep(raw json.RawMessage) error {
	var payload struct {
		DurationMS int `json:"duration_ms"`
		FailFirst  int `json:"fail_first"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&payload); err != nil {
		return fmt.Errorf("invalid sleep payload: %w", err)
	}
	if payload.DurationMS < 0 || payload.DurationMS > 300000 {
		return fmt.Errorf("duration_ms must be 0..300000")
	}
	if payload.FailFirst < 0 || payload.FailFirst > 10 {
		return fmt.Errorf("fail_first must be 0..10")
	}
	return nil
}
func validateEffect(raw json.RawMessage) error {
	// Effects have a distinct schema: any JSON object up to the global size limit.
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(raw, &payload); err != nil {
		return err
	}
	if payload == nil {
		return fmt.Errorf("effect payload must be an object")
	}
	return nil
}

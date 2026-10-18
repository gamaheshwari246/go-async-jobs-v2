package spec

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

const (
	MinPriority           = 0
	MaxPriority           = 9
	DefaultPriority       = 5
	DefaultRetries        = 3
	DefaultTimeoutSeconds = 30
	MaxPayloadBytes       = 16 * 1024
)

type SubmitRequest struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       *int            `json:"priority,omitempty"`
	MaxRetries     *int            `json:"max_retries,omitempty"`
	TimeoutSeconds *int            `json:"timeout_seconds,omitempty"`
	RunAt          *string         `json:"run_at,omitempty"`
}
type Job struct {
	Priority    int             `json:"priority"`
	Fingerprint string          `json:"fingerprint,omitempty"`
	Payload     json.RawMessage `json:"payload"`
}
type Validated struct {
	Type     string
	Queue    string
	Priority int
	Retries  int
	Timeout  time.Duration
	RunAt    *time.Time
	Body     []byte
}

func Decode(r io.Reader) (SubmitRequest, error) {
	var req SubmitRequest
	dec := json.NewDecoder(io.LimitReader(r, 64*1024))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return req, fmt.Errorf("invalid JSON request: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return req, fmt.Errorf("request must contain exactly one JSON object")
	}
	return req, nil
}
func QueueForPriority(p int) (string, error) {
	if p < MinPriority || p > MaxPriority {
		return "", fmt.Errorf("priority must be between %d and %d", MinPriority, MaxPriority)
	}
	if p >= 7 {
		return "critical", nil
	}
	if p >= 4 {
		return "default", nil
	}
	return "low", nil
}
func Validate(req SubmitRequest) (Validated, error) {
	var out Validated
	p := DefaultPriority
	if req.Priority != nil {
		p = *req.Priority
	}
	queue, err := QueueForPriority(p)
	if err != nil {
		return out, err
	}
	retries := DefaultRetries
	if req.MaxRetries != nil {
		retries = *req.MaxRetries
	}
	if retries < 0 || retries > 10 {
		return out, fmt.Errorf("max_retries must be between 0 and 10")
	}
	secs := DefaultTimeoutSeconds
	if req.TimeoutSeconds != nil {
		secs = *req.TimeoutSeconds
	}
	if secs < 1 || secs > 300 {
		return out, fmt.Errorf("timeout_seconds must be between 1 and 300")
	}
	var runAt *time.Time
	if req.RunAt != nil {
		parsed, err := time.Parse(time.RFC3339Nano, *req.RunAt)
		if err != nil {
			return out, fmt.Errorf("run_at must be RFC3339 with a timezone: %w", err)
		}
		now := time.Now()
		if !parsed.After(now) {
			return out, fmt.Errorf("run_at must be in the future")
		}
		if parsed.After(now.Add(30 * 24 * time.Hour)) {
			return out, fmt.Errorf("run_at cannot exceed 30 days")
		}
		runAt = &parsed
	}
	if req.Type == "" {
		return out, fmt.Errorf("type is required")
	}
	trimmed := bytes.TrimSpace(req.Payload)
	if len(trimmed) < 2 || trimmed[0] != '{' || !json.Valid(trimmed) {
		return out, fmt.Errorf("payload must be a valid JSON object")
	}
	if len(trimmed) > MaxPayloadBytes {
		return out, fmt.Errorf("payload exceeds %d bytes", MaxPayloadBytes)
	}
	fingerprintData, _ := json.Marshal(struct {
		Type           string
		Priority       int
		Retries        int
		TimeoutSeconds int
		RunAt          string
		Payload        json.RawMessage
	}{req.Type, p, retries, secs, func() string {
		if runAt != nil {
			return runAt.UTC().Format(time.RFC3339Nano)
		}
		return ""
	}(), trimmed})
	sum := sha256.Sum256(fingerprintData)
	body, err := json.Marshal(Job{Priority: p, Fingerprint: hex.EncodeToString(sum[:]), Payload: trimmed})
	if err != nil {
		return out, fmt.Errorf("encode job: %w", err)
	}
	return Validated{Type: req.Type, Queue: queue, Priority: p, Retries: retries, Timeout: time.Duration(secs) * time.Second, RunAt: runAt, Body: body}, nil
}

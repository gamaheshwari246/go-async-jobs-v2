package spec

import (
	"strings"
	"testing"
	"time"
)

func ptr(n int) *int { return &n }
func TestPriorityMapping(t *testing.T) {
	for _, tt := range []struct {
		p    int
		want string
	}{{0, "low"}, {3, "low"}, {4, "default"}, {6, "default"}, {7, "critical"}, {9, "critical"}} {
		got, err := QueueForPriority(tt.p)
		if err != nil || got != tt.want {
			t.Fatalf("priority %d: got %q err %v", tt.p, got, err)
		}
	}
	for _, p := range []int{-1, 10, 100} {
		if _, err := QueueForPriority(p); err == nil {
			t.Fatalf("expected error %d", p)
		}
	}
}
func TestValidateDefaultsAndZero(t *testing.T) {
	req := SubmitRequest{Type: "demo.sleep", Payload: []byte(`{"duration_ms":10}`)}
	valid, err := Validate(req)
	if err != nil {
		t.Fatal(err)
	}
	if valid.Priority != 5 || valid.Retries != 3 || valid.Queue != "default" {
		t.Fatalf("unexpected defaults: %+v", valid)
	}
	req.Priority = ptr(0)
	req.MaxRetries = ptr(0)
	valid, err = Validate(req)
	if err != nil || valid.Priority != 0 || valid.Retries != 0 || valid.Queue != "low" {
		t.Fatalf("zero failed %+v %v", valid, err)
	}
}
func TestRejectBadInput(t *testing.T) {
	cases := []SubmitRequest{
		{Type: "demo.sleep", Payload: []byte(`{}`), Priority: ptr(100)},
		{Type: "demo.sleep", Payload: []byte(`{}`), MaxRetries: ptr(-1)},
		{Type: "demo.sleep", Payload: []byte(`{}`), TimeoutSeconds: ptr(0)},
		{Type: "demo.sleep", Payload: []byte(`[]`)},
	}
	for i, c := range cases {
		if _, err := Validate(c); err == nil {
			t.Fatalf("case %d accepted", i)
		}
	}
}
func TestDecodeStrictJSON(t *testing.T) {
	if _, err := Decode(strings.NewReader(`{"type":"demo.sleep","payload":{},"priority":1.4}`)); err == nil {
		t.Fatal("accepted fractional priority")
	}
	if _, err := Decode(strings.NewReader(`{"type":"demo.sleep","payload":{},"unknown":1}`)); err == nil {
		t.Fatal("accepted unknown field")
	}
}

func TestDelayedRunAt(t *testing.T) {
	future := time.Now().Add(3 * time.Minute).UTC().Format(time.RFC3339Nano)
	valid, err := Validate(SubmitRequest{Type: "demo.sleep", Payload: []byte("{}"), RunAt: &future})
	if err != nil || valid.RunAt == nil || !valid.RunAt.Equal(timeNowParse(t, future)) {
		t.Fatalf("valid run_at: %+v, %v", valid, err)
	}
	bad := []string{"invalid", "2020-01-01T00:00:00Z", time.Now().Add(31 * 24 * time.Hour).UTC().Format(time.RFC3339Nano)}
	for _, value := range bad {
		if _, err := Validate(SubmitRequest{Type: "demo.sleep", Payload: []byte("{}"), RunAt: &value}); err == nil {
			t.Fatalf("accepted invalid run_at %s", value)
		}
	}
}
func timeNowParse(t *testing.T, s string) time.Time {
	t.Helper()
	tm, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t.Fatal(err)
	}
	return tm
}

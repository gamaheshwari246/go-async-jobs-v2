package backoff

import (
	"testing"
	"time"
)

func TestDelayExponentialAndCap(t *testing.T) {
	for _, tt := range []struct {
		n    int
		want time.Duration
	}{{1, time.Second}, {2, 2 * time.Second}, {3, 4 * time.Second}, {6, 32 * time.Second}, {7, time.Minute}, {50, time.Minute}} {
		if got := Delay(tt.n, 0); got != tt.want {
			t.Errorf("Delay(%d,0)=%s;want %s", tt.n, got, tt.want)
		}
	}
}
func TestDelayJitterBounds(t *testing.T) {
	if got := Delay(2, 1); got != 2400*time.Millisecond {
		t.Fatalf("max jitter %v", got)
	}
	if got := Delay(2, -1); got != 2*time.Second {
		t.Fatalf("negative jitter %v", got)
	}
	if got := Delay(9, 1); got != time.Minute {
		t.Fatalf("cap %v", got)
	}
}

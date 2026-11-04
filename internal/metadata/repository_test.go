package metadata

import (
	"testing"
	"time"
)

func TestBackoffBounded(t *testing.T) {
	if Backoff(1) != 2*time.Second || Backoff(2) != 4*time.Second || Backoff(100) != 256*time.Second {
		t.Fatal("incorrect retry delay")
	}
}

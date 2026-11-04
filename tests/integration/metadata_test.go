//go:build integration

package integration

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"os"
	"strings"
	"testing"
	"time"
	"vaultstore/internal/jobs"
	"vaultstore/internal/metadata"
)

func TestPostgresTransactions(t *testing.T) {
	url := os.Getenv("DATABASE_URL")
	if !strings.Contains(url, "vaultstore_test") {
		t.Fatal("isolated vaultstore_test DATABASE_URL required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r, err := metadata.Open(ctx, url, "test-transactions")
	must(t, err)
	defer r.Pool.Close()
	must(t, r.Migrate(ctx))
	source := uuid.NewString()
	_, err = r.Pool.Exec(ctx, "INSERT INTO sources(id,name,path) VALUES($1,'transaction-test','.')", source)
	must(t, err)
	defer func() {
		_, err := r.Pool.Exec(context.Background(), "DELETE FROM snapshots WHERE source_id=$1", source)
		if err != nil {
			t.Error(err)
		}
		_, err = r.Pool.Exec(context.Background(), "DELETE FROM sources WHERE id=$1", source)
		if err != nil {
			t.Error(err)
		}
	}()
	_, id, err := r.NewBackup(ctx, source, "lease test", "full")
	must(t, err)
	first, err := r.Claim(ctx, id, "worker1", time.Minute)
	must(t, err)
	if _, err = r.Claim(ctx, id, "worker2", time.Minute); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("duplicate claimed: %v", err)
	}
	must(t, r.Fail(ctx, first, errors.New("temporary outage"), 5, false))
	if _, err = r.Claim(ctx, id, "worker2", time.Minute); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal("backoff bypassed")
	}
	_, err = r.Pool.Exec(ctx, "UPDATE jobs SET next_attempt_at=now() WHERE id=$1", id)
	must(t, err)
	second, err := r.Claim(ctx, id, "worker2", time.Minute)
	must(t, err)
	if err = r.Complete(ctx, first, nil); !errors.Is(err, metadata.ErrLease) {
		t.Fatal("old fencing token accepted")
	}
	_, err = r.Pool.Exec(ctx, "UPDATE jobs SET lease_until=now()-interval '1 second' WHERE id=$1", id)
	must(t, err)
	must(t, jobs.Recover(ctx, r, 5))
	if err = r.Complete(ctx, second, nil); !errors.Is(err, metadata.ErrLease) {
		t.Fatal("abandoned worker accepted")
	}
	var n int
	must(t, r.Pool.QueryRow(ctx, "SELECT count(*) FROM outbox WHERE job_id=$1", id).Scan(&n))
	if n != 3 {
		t.Fatalf("expected original, retry and recovery events; got %d", n)
	}
	var before, after int
	must(t, r.Pool.QueryRow(ctx, "SELECT count(*) FROM jobs").Scan(&before))
	if _, _, err = r.NewBackup(ctx, uuid.NewString(), "invalid source", "full"); err == nil {
		t.Fatal("invalid source accepted")
	}
	must(t, r.Pool.QueryRow(ctx, "SELECT count(*) FROM jobs").Scan(&after))
	if before != after {
		t.Fatal("partial transaction leaked job")
	}
}

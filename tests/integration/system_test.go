//go:build integration

package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"vaultstore/internal/api"
	"vaultstore/internal/chunking"
	"vaultstore/internal/config"
	"vaultstore/internal/engine"
	"vaultstore/internal/jobs"
	"vaultstore/internal/metadata"
	"vaultstore/internal/model"
	"vaultstore/internal/storage"
)

type outagePublisher struct{}

type faultStorage struct {
	storage.ObjectStorage
	after bool
}

func (s faultStorage) Put(ctx context.Context, k string, r io.Reader, n int64) error {
	if s.after {
		if err := s.ObjectStorage.Put(ctx, k, r, n); err != nil {
			return err
		}
	}
	return errors.New("injected storage upload interruption")
}

func (outagePublisher) WriteMessages(context.Context, ...kafka.Message) error {
	return errors.New("injected Kafka outage")
}
func TestSystem(t *testing.T) {
	if os.Getenv("VAULTSTORE_INTEGRATION") != "1" {
		t.Fatal("integration tests require VAULTSTORE_INTEGRATION=1 and isolated PostgreSQL, Kafka, MinIO; run scripts/integration.py")
	}
	c, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(c.DatabaseURL, "vaultstore_test") {
		t.Fatal("integration DATABASE_URL must use isolated vaultstore_test database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	r, err := metadata.Open(ctx, c.DatabaseURL, c.Topic)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Pool.Close()
	must(t, r.Migrate(ctx))
	c.BackupRoot = t.TempDir()
	c.RestoreRoot = t.TempDir()
	c.ChunkSize = 4096
	c.Bucket = "test-" + uuid.NewString()
	s, err := storage.New(c)
	must(t, err)
	must(t, s.Ensure(ctx))
	e := &engine.Engine{Repo: r, Store: s, Config: c}
	source := uuid.NewString()
	_, err = r.Pool.Exec(ctx, "INSERT INTO sources(id,name,path,retention) VALUES($1,'test','.',10)", source)
	must(t, err)
	original := bytes.Repeat([]byte("VaultStore\x00\xff"), 10000)
	must(t, os.WriteFile(filepath.Join(c.BackupRoot, "original.bin"), original, 0600))
	must(t, os.WriteFile(filepath.Join(c.BackupRoot, "duplicate.bin"), original, 0600))
	must(t, os.WriteFile(filepath.Join(c.BackupRoot, "empty.txt"), nil, 0600))
	sid, jid, err := r.NewBackup(ctx, source, "initial", "full")
	must(t, err)
	t.Run("transactional outbox survives Kafka outage", func(t *testing.T) {
		if err := jobs.Publish(ctx, r, outagePublisher{}); err == nil {
			t.Fatal("expected broker failure")
		}
		var pending int
		must(t, r.Pool.QueryRow(ctx, "SELECT count(*) FROM outbox WHERE job_id=$1 AND published_at IS NULL", jid).Scan(&pending))
		if pending != 1 {
			t.Fatal("event lost")
		}
		w := jobs.Writer(c.Brokers)
		defer w.Close()
		// Kafka topic auto-creation is asynchronous. The scheduler also retries
		// publication without acknowledging the outbox until metadata is ready.
		for attempt := 0; attempt < 20; attempt++ {
			err = jobs.Publish(ctx, r, w)
			if err == nil {
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
		must(t, err)
		reader := kafka.NewReader(kafka.ReaderConfig{Brokers: c.Brokers, Topic: c.Topic, Partition: 0, MinBytes: 1, MaxBytes: 1 << 20, MaxWait: time.Second})
		defer reader.Close()
		readCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		msg, err := reader.ReadMessage(readCtx)
		must(t, err)
		must(t, jobs.ValidateEvent(msg.Value))
	})
	t.Run("full backup and duplicate delivery", func(t *testing.T) {
		must(t, jobs.Execute(ctx, e, jid, "worker-one"))
		must(t, jobs.Execute(ctx, e, jid, "duplicate-worker"))
		var count int
		must(t, r.Pool.QueryRow(ctx, "SELECT count(*) FROM files WHERE snapshot_id=$1", sid).Scan(&count))
		if count != 3 {
			t.Fatalf("duplicate manifest: %d", count)
		}
		m, err := r.LoadManifest(ctx, sid)
		must(t, err)
		must(t, e.VerifyManifest(ctx, m))
		var physical int64
		must(t, r.Pool.QueryRow(ctx, "SELECT sum(size) FROM chunks").Scan(&physical))
		if physical >= int64(len(original)*2) {
			t.Fatal("deduplication did not save storage")
		}
	})
	var second string
	t.Run("incremental detects changes and deletion", func(t *testing.T) {
		must(t, os.Remove(filepath.Join(c.BackupRoot, "duplicate.bin")))
		must(t, os.WriteFile(filepath.Join(c.BackupRoot, "original.bin"), []byte("changed"), 0600))
		var job string
		second, job, err = r.NewBackup(ctx, source, "incremental", "incremental")
		must(t, err)
		must(t, jobs.Execute(ctx, e, job, "worker-two"))
		m, err := r.LoadManifest(ctx, second)
		must(t, err)
		if m.Parent != sid || len(m.Files) != 2 {
			t.Fatalf("bad incremental: %+v", m)
		}
		for _, f := range m.Files {
			if f.Path == "duplicate.bin" {
				t.Fatal("deleted file retained")
			}
		}
	})
	t.Run("earlier recovery point restores original bytes", func(t *testing.T) {
		id, err := r.NewJob(ctx, "restore", sid, model.RestoreRequest{Snapshot: sid, Destination: "recovered", Conflict: "skip"})
		must(t, err)
		must(t, jobs.Execute(ctx, e, id, "restore-worker"))
		got, err := os.ReadFile(filepath.Join(c.RestoreRoot, "recovered", "original.bin"))
		must(t, err)
		if !bytes.Equal(got, original) {
			t.Fatal("restored bytes differ")
		}
		must(t, jobs.Execute(ctx, e, id, "duplicate-restore"))
	})
	t.Run("expired lease is fenced and recovered", func(t *testing.T) {
		_, id, err := r.NewBackup(ctx, source, "crashed", "full")
		must(t, err)
		old, err := r.Claim(ctx, id, "crashed-worker", time.Minute)
		must(t, err)
		_, err = r.Pool.Exec(ctx, "UPDATE jobs SET lease_until=now()-interval '1 second' WHERE id=$1", id)
		must(t, err)
		must(t, jobs.Recover(ctx, r, c.MaxAttempts))
		_, err = r.Pool.Exec(ctx, "UPDATE jobs SET next_attempt_at=now() WHERE id=$1", id)
		must(t, err)
		if err = r.Complete(ctx, old, nil); !errors.Is(err, metadata.ErrLease) {
			t.Fatalf("stale worker committed: %v", err)
		}
		must(t, jobs.Execute(ctx, e, id, "replacement-worker"))
		assertStatus(t, ctx, r, id, "completed")
	})
	t.Run("concurrent backups and collection retain chunks", func(t *testing.T) {
		var ids []string
		for range 3 {
			_, id, err := r.NewBackup(ctx, source, "concurrent", "full")
			must(t, err)
			ids = append(ids, id)
		}
		var wg sync.WaitGroup
		for _, id := range ids {
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				if err := jobs.Execute(ctx, e, id, "parallel"); err != nil {
					t.Error(err)
				}
			}(id)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := e.GC(ctx); err != nil {
				t.Error(err)
			}
		}()
		wg.Wait()
		for _, id := range ids {
			assertStatus(t, ctx, r, id, "completed")
		}
		m, err := r.LoadManifest(ctx, sid)
		must(t, err)
		must(t, e.VerifyManifest(ctx, m))
	})
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprintf("upload_failure_after_object_commit_%v", after), func(t *testing.T) {
			content := []byte(uuid.NewString())
			name := filepath.Join(c.BackupRoot, "000-fault.bin")
			must(t, os.WriteFile(name, content, 0600))
			defer os.Remove(name)
			point, id, err := r.NewBackup(ctx, source, "upload fault", "full")
			must(t, err)
			fault := *e
			fault.Store = faultStorage{ObjectStorage: s, after: after}
			must(t, jobs.Execute(ctx, &fault, id, "fault-worker"))
			assertStatus(t, ctx, r, id, "retrying")
			var count int
			must(t, r.Pool.QueryRow(ctx, "SELECT count(*) FROM files WHERE snapshot_id=$1", point).Scan(&count))
			if count != 0 {
				t.Fatal("partial manifest published")
			}
			if after {
				_, err = s.Head(ctx, chunking.Key(chunking.Hash(content)))
				must(t, err)
				must(t, r.Pool.QueryRow(ctx, "SELECT count(*) FROM chunks WHERE hash=$1", chunking.Hash(content)).Scan(&count))
				if count != 0 {
					t.Fatal("metadata committed after interrupted upload")
				}
			}
			_, err = r.Pool.Exec(ctx, "UPDATE jobs SET next_attempt_at=now() WHERE id=$1", id)
			must(t, err)
			must(t, jobs.Execute(ctx, e, id, "recovered-worker"))
			assertStatus(t, ctx, r, id, "completed")
			m, err := r.LoadManifest(ctx, point)
			must(t, err)
			must(t, e.VerifyManifest(ctx, m))
		})
	}
	t.Run("manifest_transaction_failure_rolls_back_then_recovers", func(t *testing.T) {
		point, id, err := r.NewBackup(ctx, source, "transaction fault", "full")
		must(t, err)
		_, err = r.Pool.Exec(ctx, `CREATE FUNCTION inject_manifest_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected transaction failure'; END $$; CREATE TRIGGER inject_manifest_failure BEFORE INSERT ON files FOR EACH ROW EXECUTE FUNCTION inject_manifest_failure()`)
		must(t, err)
		must(t, jobs.Execute(ctx, e, id, "transaction-fault-worker"))
		assertStatus(t, ctx, r, id, "retrying")
		var count int
		must(t, r.Pool.QueryRow(ctx, "SELECT count(*) FROM files WHERE snapshot_id=$1", point).Scan(&count))
		if count != 0 {
			t.Fatal("failed transaction leaked file references")
		}
		_, err = r.Pool.Exec(ctx, "DROP TRIGGER inject_manifest_failure ON files; DROP FUNCTION inject_manifest_failure()")
		must(t, err)
		_, err = r.Pool.Exec(ctx, "UPDATE jobs SET next_attempt_at=now() WHERE id=$1", id)
		must(t, err)
		must(t, jobs.Execute(ctx, e, id, "transaction-recovery-worker"))
		assertStatus(t, ctx, r, id, "completed")
	})
	t.Run("restore_conflict_policies", func(t *testing.T) {
		target := filepath.Join(c.RestoreRoot, "recovered", "original.bin")
		must(t, os.WriteFile(target, []byte("existing"), 0600))
		for _, policy := range []string{"skip", "rename", "overwrite"} {
			id, err := r.NewJob(ctx, "restore", sid, model.RestoreRequest{Snapshot: sid, Destination: "recovered", Path: "original.bin", Conflict: policy})
			must(t, err)
			must(t, jobs.Execute(ctx, e, id, "restore-conflict-worker"))
			assertStatus(t, ctx, r, id, "completed")
			got, err := os.ReadFile(target)
			must(t, err)
			if policy == "overwrite" {
				if !bytes.Equal(got, original) {
					t.Fatal("explicit overwrite failed")
				}
			} else if string(got) != "existing" {
				t.Fatal("existing content overwritten")
			}
			if policy == "rename" {
				renamed, err := os.ReadFile(target + ".restored-" + id)
				must(t, err)
				if !bytes.Equal(renamed, original) {
					t.Fatal("renamed content incorrect")
				}
			}
		}
	})
	t.Run("corruption is reported then repaired", func(t *testing.T) {
		m, err := r.LoadManifest(ctx, sid)
		must(t, err)
		f := m.Files[0]
		if len(f.Chunks) == 0 {
			t.Fatal("fixture requires chunks")
		}
		ch := f.Chunks[0]
		key := "chunks/" + ch.Hash[:2] + "/" + ch.Hash[2:4] + "/" + ch.Hash
		b, err := storage.Read(ctx, s, key, ch.Size)
		must(t, err)
		bad := append([]byte{}, b...)
		bad[0] ^= 255
		must(t, s.Put(ctx, key, bytes.NewReader(bad), int64(len(bad))))
		id, err := r.NewJob(ctx, "verify", sid, map[string]string{})
		must(t, err)
		must(t, jobs.Execute(ctx, e, id, "verifier"))
		assertStatus(t, ctx, r, id, "failed")
		must(t, s.Put(ctx, key, bytes.NewReader(b), int64(len(b))))
		must(t, e.VerifyManifest(ctx, m))
	})
	t.Run("all_read_API_routes_execute_SQL", func(t *testing.T) {
		h := (&api.API{Engine: e}).Handler()
		for _, route := range []string{"sources", "sources/" + source, "backups", "backups/" + sid, "backups/" + sid + "/files", "backups/" + sid + "/status", "restores", "jobs", "jobs/" + jid, "recovery-points", "recovery-points/" + sid, "recovery-points/" + sid + "/files", "storage/overview", "storage/deduplication", "storage/chunks", "analytics/overview", "analytics/backups", "analytics/workers"} {
			req := httptest.NewRequest("GET", "/api/v1/"+route, nil)
			req.Header.Set("Authorization", "Bearer "+c.Token)
			response := httptest.NewRecorder()
			h.ServeHTTP(response, req)
			if response.Code != 200 {
				t.Errorf("%s: %d %s", route, response.Code, response.Body.String())
			}
		}
	})
	t.Run("deleting parent does not break incremental", func(t *testing.T) {
		must(t, e.DeleteSnapshot(ctx, sid))
		must(t, e.GC(ctx))
		m, err := r.LoadManifest(ctx, second)
		must(t, err)
		must(t, e.VerifyManifest(ctx, m))
	})
}
func must(t *testing.T, e error) {
	t.Helper()
	if e != nil {
		t.Fatal(e)
	}
}
func assertStatus(t *testing.T, ctx context.Context, r *metadata.Repository, id, want string) {
	t.Helper()
	var got string
	must(t, r.Pool.QueryRow(ctx, "SELECT status FROM jobs WHERE id=$1", id).Scan(&got))
	if got != want {
		t.Fatal(fmt.Sprintf("job %s: %s, want %s", id, got, want))
	}
}

package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/kafka-go"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"
	"vaultstore/internal/engine"
	"vaultstore/internal/metadata"
	"vaultstore/internal/model"
)

var totals = promauto.NewCounterVec(prometheus.CounterOpts{Name: "job_attempts_total", Help: "Executed job attempts"}, []string{"kind", "result"})
var duration = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "job_duration_seconds", Help: "Duration per job attempt"}, []string{"kind"})
var active = promauto.NewGauge(prometheus.GaugeOpts{Name: "worker_active_jobs", Help: "Currently executing jobs"})
var published=promauto.NewCounter(prometheus.CounterOpts{Name:"outbox_published_total",Help:"Outbox records acknowledged after Kafka publication"})
var publicationFailures=promauto.NewCounter(prometheus.CounterOpts{Name:"outbox_publish_failures_total",Help:"Failed Kafka outbox publication attempts"})

type Publisher interface {
	WriteMessages(context.Context, ...kafka.Message) error
}

func Writer(brokers []string) *kafka.Writer {
	return &kafka.Writer{Addr: kafka.TCP(brokers...), RequiredAcks: kafka.RequireAll, Async: false, WriteTimeout: 10 * time.Second, ReadTimeout: 10 * time.Second, AllowAutoTopicCreation: true}
}

// EnsureTopics completes topic creation before consumer-group assignment. Joining
// a group while its topic has no metadata can otherwise leave idle assignments.
func EnsureTopics(ctx context.Context, brokers []string, topic string) error {
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()
	if err = conn.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		return err
	}
	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	admin, err := kafka.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer admin.Close()
	if err = admin.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		return err
	}
	for _, name := range []string{topic, topic + ".dead"} {
		err = admin.CreateTopics(kafka.TopicConfig{Topic: name, NumPartitions: 8, ReplicationFactor: 1})
		if err != nil && !errors.Is(err, kafka.TopicAlreadyExists) {
			return err
		}
	}
	for attempt := 0; attempt < 30; attempt++ {
		parts, err := conn.ReadPartitions(topic)
		if err == nil && len(parts) > 0 {
			return nil
		}
		wait(ctx, 250*time.Millisecond)
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return fmt.Errorf("Kafka topic metadata not ready")
}
func Publish(ctx context.Context, r *metadata.Repository, w Publisher) error {
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var id, topic string
	var b []byte
	err = tx.QueryRow(ctx, "SELECT id::text,topic,payload FROM outbox WHERE published_at IS NULL AND available_at<=now() ORDER BY available_at LIMIT 1 FOR UPDATE SKIP LOCKED").Scan(&id, &topic, &b)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if err = w.WriteMessages(ctx, kafka.Message{Topic: topic, Key: []byte(id), Value: b}); err != nil {
		publicationFailures.Inc()
		return err
	}
	if _, err = tx.Exec(ctx, "UPDATE outbox SET published_at=now() WHERE id=$1", id); err != nil {
		return err
	}
	if err=tx.Commit(ctx);err!=nil{return err};published.Inc();return nil
}
func Recover(ctx context.Context, r *metadata.Repository, max int) error {
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, "SELECT id::text,kind,snapshot_id::text,attempt FROM jobs WHERE status='running' AND lease_until<now() FOR UPDATE SKIP LOCKED")
	if err != nil {
		return err
	}
	var abandoned []model.Job
	for rows.Next() {
		var j model.Job
		if err = rows.Scan(&j.ID, &j.Kind, &j.Snapshot, &j.Attempt); err != nil {
			rows.Close()
			return err
		}
		abandoned = append(abandoned, j)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, j := range abandoned {
		status, topic := "retrying", r.Topic
		if j.Attempt >= max {
			status = "failed"
			topic += ".dead"
		}
		if _, err = tx.Exec(ctx, "UPDATE jobs SET status=$2,token=NULL,lease_until=NULL,next_attempt_at=now()+$3*interval '1 second',error='worker lease expired',updated_at=now() WHERE id=$1", j.ID, status, metadata.Backoff(j.Attempt).Seconds()); err != nil {
			return err
		}
		if status == "failed" && j.Kind == "backup" {
			if _, err = tx.Exec(ctx, "UPDATE snapshots SET status='failed' WHERE id=$1 AND status='pending'", j.Snapshot); err != nil {
				return err
			}
		}
		if err = metadata.Enqueue(ctx, tx, topic, j.ID, metadata.Backoff(j.Attempt)); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
func Schedule(ctx context.Context, r *metadata.Repository) error {
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var id, name string
	var seconds int
	err = tx.QueryRow(ctx, "SELECT id::text,name,schedule_seconds FROM sources WHERE schedule_seconds>0 AND (next_run IS NULL OR next_run<=now()) ORDER BY next_run NULLS FIRST LIMIT 1 FOR UPDATE SKIP LOCKED").Scan(&id, &name, &seconds)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if _, _, err = r.BackupTx(ctx, tx, id, name+" scheduled", "incremental"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "UPDATE sources SET next_run=now()+$2*interval '1 second' WHERE id=$1", id, seconds); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func Execute(ctx context.Context, e *engine.Engine, id, worker string) error {
	j, err := e.Repo.Claim(ctx, id, worker, e.Config.Lease)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	jobCtx, cancel := context.WithTimeout(ctx, e.Config.JobTimeout)
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(e.Config.Lease / 3)
		defer t.Stop()
		for {
			select {
			case <-jobCtx.Done():
				return
			case <-t.C:
				if err := e.Repo.Heartbeat(jobCtx, j, e.Config.Lease); err != nil {
					slog.Error("lease renewal failed", "job_id", j.ID, "error", err)
					cancel()
					return
				}
			}
		}
	}()
	started := time.Now()
	active.Inc()
	err = e.Run(jobCtx, j)
	active.Dec()
	cancel()
	<-done
	duration.WithLabelValues(j.Kind).Observe(time.Since(started).Seconds())
	if err == nil {
		totals.WithLabelValues(j.Kind, "completed").Inc()
		return nil
	}
	totals.WithLabelValues(j.Kind, "failed").Inc()
	slog.Error("job attempt failed", "job_id", j.ID, "kind", j.Kind, "attempt", j.Attempt, "error", err)
	var permanent engine.Permanent
	cleanup, c := context.WithTimeout(context.Background(), 10*time.Second)
	defer c()
	failure := e.Repo.Fail(cleanup, j, err, e.Config.MaxAttempts, errors.As(err, &permanent))
	if errors.Is(failure, metadata.ErrLease) {
		return nil
	}
	return failure
}
func Workers(ctx context.Context, e *engine.Engine) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	fatal := make(chan error, e.Config.Concurrency)
	id := uuid.NewString()
	var wg sync.WaitGroup
	for n := 0; n < e.Config.Concurrency; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reader := kafka.NewReader(kafka.ReaderConfig{Brokers: e.Config.Brokers, GroupID: e.Config.Topic + ".workers-v1", Topic: e.Config.Topic, MinBytes: 1, MaxBytes: 1 << 20, MaxWait: time.Second, CommitInterval: 0, WatchPartitionChanges: true, PartitionWatchInterval: 5 * time.Second,
				ErrorLogger: kafka.LoggerFunc(func(format string, args ...interface{}) {
					slog.Error("Kafka consumer", "detail", fmt.Sprintf(format, args...))
				}),
			})
			defer reader.Close()
			for ctx.Err() == nil {
				msg, err := reader.FetchMessage(ctx)
				if err != nil {
					if ctx.Err() == nil {
						slog.Error("kafka fetch failed", "error", err)
						wait(ctx, time.Second)
					}
					continue
				}
				var ev model.Event
				if err = json.Unmarshal(msg.Value, &ev); err != nil || ev.Version != 1 || uuid.Validate(ev.JobID) != nil {
					slog.Error("invalid event retained; operator intervention required", "partition", msg.Partition, "offset", msg.Offset)
					fatal <- fmt.Errorf("invalid Kafka event at partition %d offset %d", msg.Partition, msg.Offset)
					return
				}
				for ctx.Err() == nil {
					if err = Execute(ctx, e, ev.JobID, id); err == nil {
						break
					}
					slog.Error("job dispatch failed", "job_id", ev.JobID, "error", err)
					wait(ctx, time.Second)
				}
				if ctx.Err() != nil {
					return
				}
				if err = reader.CommitMessages(ctx, msg); err != nil {
					slog.Error("offset commit failed", "error", err)
					fatal <- err
					return
				}
			}
		}()
	}
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for {
		_, err := e.Repo.Pool.Exec(ctx, "INSERT INTO workers(id,heartbeat) VALUES($1,now()) ON CONFLICT(id) DO UPDATE SET heartbeat=now(),active=(SELECT count(*) FROM jobs WHERE worker_id=$1 AND status='running')", id)
		if err != nil && ctx.Err() == nil {
			slog.Error("worker registration failed", "error", err)
		}
		select {
		case err := <-fatal:
			cancel()
			wg.Wait()
			return err
		case <-ctx.Done():
			wg.Wait()
			return nil
		case <-tick.C:
		}
	}
}
func wait(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
func Scheduler(ctx context.Context, e *engine.Engine) error {
	w := Writer(e.Config.Brokers)
	defer w.Close()
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	maintenance := time.NewTicker(time.Minute)
	defer maintenance.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			for _, fn := range []func(context.Context) error{func(c context.Context) error { return Publish(c, e.Repo, w) }, func(c context.Context) error { return Recover(c, e.Repo, e.Config.MaxAttempts) }, func(c context.Context) error { return Schedule(c, e.Repo) }} {
				call, cancel := context.WithTimeout(ctx, 15*time.Second)
				err := fn(call)
				cancel()
				if err != nil && ctx.Err() == nil {
					slog.Error("scheduler cycle failed", "error", err)
				}
			}
		case <-maintenance.C:
			if err := e.Retention(ctx); err != nil {
				slog.Error("retention failed", "error", err)
			}
		}
	}
}
func ValidateEvent(b []byte) error {
	var e model.Event
	if err := json.Unmarshal(b, &e); err != nil {
		return err
	}
	if e.Version != 1 || uuid.Validate(e.ID) != nil || uuid.Validate(e.JobID) != nil {
		return fmt.Errorf("invalid event")
	}
	return nil
}

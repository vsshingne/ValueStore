package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"io"
	"log/slog"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"
	"vaultstore/internal/engine"
	"vaultstore/internal/metadata"
	"vaultstore/internal/model"
	"vaultstore/internal/safepath"
)

type API struct{ Engine *engine.Engine }

func jsonResponse(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("response encoding failed", "error", err)
	}
}
func respond(w http.ResponseWriter, v any) { jsonResponse(w, 200, map[string]any{"data": v}) }
func fail(w http.ResponseWriter, status int, message string) {
	jsonResponse(w, status, map[string]any{"error": map[string]string{"message": message}})
}
func decode(w http.ResponseWriter, r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		return e
	}
	if e := d.Decode(&struct{}{}); e != io.EOF {
		return fmt.Errorf("request must contain one JSON object")
	}
	return nil
}
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/health", func(w http.ResponseWriter, r *http.Request) { respond(w, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /api/v1/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if err := a.Engine.Repo.Pool.Ping(ctx); err != nil {
			fail(w, 503, "database unavailable")
			return
		}
		respond(w, map[string]string{"status": "ready"})
	})
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("/api/v1/", a.route)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		id := uuid.NewString()
		started := time.Now()
		defer func() {
			slog.Info("http request", "request_id", id, "method", r.Method, "path", r.URL.Path, "duration_ms", time.Since(started).Milliseconds())
		}()
		w.Header().Set("X-Request-ID", id)
		if r.URL.Path != "/api/v1/health" && r.URL.Path != "/api/v1/ready" && r.URL.Path != "/metrics" {
			token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if subtle.ConstantTimeCompare([]byte(token), []byte(a.Engine.Config.Token)) != 1 {
				fail(w, 401, "authentication required")
				return
			}
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}
func (a *API) query(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
	rows, e := a.Engine.Repo.Pool.Query(ctx, sql, args...)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		values, e := rows.Values()
		if e != nil {
			return nil, e
		}
		v := map[string]any{}
		for i, f := range rows.FieldDescriptions() {
			v[f.Name] = values[i]
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func pagination(r *http.Request) (int, int, error) {
	limit, offset := 50, 0
	var e error
	if s := r.URL.Query().Get("limit"); s != "" {
		limit, e = strconv.Atoi(s)
		if e != nil {
			return 0, 0, e
		}
	}
	if s := r.URL.Query().Get("offset"); s != "" {
		offset, e = strconv.Atoi(s)
		if e != nil {
			return 0, 0, e
		}
	}
	if limit < 1 || limit > 500 || offset < 0 {
		return 0, 0, fmt.Errorf("invalid pagination")
	}
	return limit, offset, nil
}
func (a *API) route(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/"), "/"), "/")
	resource := parts[0]
	id, action := "", ""
	if len(parts) > 1 {
		id = parts[1]
	}
	if len(parts) > 2 {
		action = parts[2]
	}
	if len(parts) > 3 {
		fail(w, 404, "not found")
		return
	}
	if r.Method == http.MethodPost && action != "" && r.ContentLength > 0 {
		if err := decode(w, r, &struct{}{}); err != nil {
			fail(w, 400, "action body must be an empty object")
			return
		}
	}
	if id != "" && resource != "storage" && resource != "analytics" {
		if uuid.Validate(id) != nil {
			fail(w, 400, "invalid resource ID")
			return
		}
	}
	limit, offset, err := pagination(r)
	if err != nil {
		fail(w, 400, err.Error())
		return
	}
	var result any
	switch r.Method {
	case http.MethodGet:
		switch resource {
		case "sources":
			if id == "" {
				result, err = a.query(ctx, "SELECT id::text,name,path,exclusions,retention,schedule_seconds,next_run,created_at FROM sources ORDER BY created_at DESC LIMIT $1 OFFSET $2", limit, offset)
			} else {
				result, err = a.query(ctx, "SELECT id::text,name,path,exclusions,retention,schedule_seconds,next_run,created_at FROM sources WHERE id=$1", id)
			}
		case "backups", "recovery-points":
			if action == "files" {
				var m model.Manifest
				m, err = a.Engine.Repo.LoadManifest(ctx, id)
				if err == nil {
					files := []model.File{}
					prefix := r.URL.Query().Get("prefix")
					for _, f := range m.Files {
						if strings.HasPrefix(f.Path, prefix) {
							files = append(files, f)
						}
					}
					if offset > len(files) {
						offset = len(files)
					}
					end := offset + limit
					if end > len(files) {
						end = len(files)
					}
					result = files[offset:end]
				}
			} else if action == "" || action == "status" {
				filter := ""
				if resource == "recovery-points" {
					filter = " AND b.status='completed'"
				}
				if id != "" {
					result, err = a.query(ctx, "SELECT b.id::text,b.name,b.type,b.status,b.source_id::text,b.parent_id::text,b.logical_bytes,b.created_at,b.completed_at,b.checksum FROM snapshots b WHERE b.id=$1"+filter, id)
				} else {
					result, err = a.query(ctx, "SELECT b.id::text,b.name,b.type,b.status,b.source_id::text,b.parent_id::text,b.logical_bytes,b.created_at,b.completed_at FROM snapshots b WHERE ($3='' OR b.status=$3)"+filter+" ORDER BY b.created_at DESC LIMIT $1 OFFSET $2", limit, offset, r.URL.Query().Get("status"))
				}
			} else {
				err = pgx.ErrNoRows
			}
		case "jobs", "restores":
			filter := ""
			if resource == "restores" {
				filter = " AND kind='restore'"
			}
			if action != "" && action != "status" {
				err = pgx.ErrNoRows
				break
			}
			if id == "" {
				result, err = a.query(ctx, "SELECT id::text,kind,snapshot_id::text,status,attempt,progress,error,report,worker_id,created_at,updated_at FROM jobs WHERE ($3='' OR status=$3)"+filter+" ORDER BY created_at DESC LIMIT $1 OFFSET $2", limit, offset, r.URL.Query().Get("status"))
			} else {
				result, err = a.query(ctx, "SELECT id::text,kind,snapshot_id::text,status,attempt,progress,error,report,worker_id,created_at,updated_at FROM jobs WHERE id=$1"+filter, id)
			}
		case "storage":
			if id == "chunks" {
				result, err = a.query(ctx, "SELECT c.hash,c.size,c.storage_key,c.verified_at,(SELECT count(*) FROM file_chunks f WHERE f.chunk_hash=c.hash) AS \"references\" FROM chunks c ORDER BY c.created_at DESC LIMIT $1 OFFSET $2", limit, offset)
			} else if id == "overview" || id == "deduplication" {
				result, err = a.overview(ctx)
			} else {
				err = pgx.ErrNoRows
			}
		case "analytics":
			switch id {
			case "overview":
				result, err = a.overview(ctx)
			case "workers":
				result, err = a.query(ctx, "SELECT id,heartbeat,active,(heartbeat>now()-interval '30 seconds') healthy FROM workers ORDER BY heartbeat DESC LIMIT $1 OFFSET $2", limit, offset)
			case "backups":
				result, err = a.query(ctx, "SELECT date_trunc('day',created_at) AS day,count(*) AS backups,COALESCE(sum(logical_bytes),0)::bigint AS logical_bytes FROM snapshots GROUP BY 1 ORDER BY 1 DESC LIMIT $1 OFFSET $2", limit, offset)
			default:
				err = pgx.ErrNoRows
			}
		default:
			err = pgx.ErrNoRows
		}
	case http.MethodPost:
		switch resource {
		case "sources":
			var req struct {
				Name       string   `json:"name"`
				Path       string   `json:"path"`
				Exclusions []string `json:"exclusions"`
				Retention  int      `json:"retention"`
				Schedule   int      `json:"schedule_seconds"`
			}
			if err = decode(w, r, &req); err != nil {
				fail(w, 400, err.Error())
				return
			}
			if len(req.Name) < 1 || len(req.Name) > 200 || req.Retention < 1 || req.Retention > 10000 || req.Schedule < 0 || (req.Schedule > 0 && req.Schedule < 60) {
				fail(w, 400, "invalid source configuration")
				return
			}
			if err = safepath.Relative(req.Path); err != nil {
				fail(w, 400, err.Error())
				return
			}
			for _, p := range req.Exclusions {
				if _, err = path.Match(p, "test"); err != nil {
					fail(w, 400, "invalid exclusion pattern")
					return
				}
			}
			root, e := safepath.Root(a.Engine.Config.BackupRoot, req.Path)
			if e != nil {
				fail(w, 400, "source directory is unavailable under backup root")
				return
			}
			root.Close()
			sid := uuid.NewString()
			if req.Exclusions == nil {
				req.Exclusions = []string{}
			}
			ex, _ := json.Marshal(req.Exclusions)
			_, err = a.Engine.Repo.Pool.Exec(ctx, "INSERT INTO sources(id,name,path,exclusions,retention,schedule_seconds) VALUES($1,$2,$3,$4,$5,$6)", sid, req.Name, req.Path, ex, req.Retention, req.Schedule)
			result = map[string]string{"id": sid}
		case "backups":
			if action == "verify" {
				var jid string
				jid, err = a.Engine.Repo.NewJob(ctx, "verify", id, map[string]string{})
				result = map[string]string{"job_id": jid}
			} else if id == "" {
				var req struct {
					Source string `json:"source_id"`
					Name   string `json:"name"`
					Type   string `json:"type"`
				}
				if err = decode(w, r, &req); err != nil {
					fail(w, 400, err.Error())
					return
				}
				if uuid.Validate(req.Source) != nil || len(req.Name) < 1 || len(req.Name) > 200 || (req.Type != "full" && req.Type != "incremental") {
					fail(w, 400, "invalid backup request")
					return
				}
				var sid, jid string
				sid, jid, err = a.Engine.Repo.NewBackup(ctx, req.Source, req.Name, req.Type)
				result = map[string]string{"id": sid, "job_id": jid}
			} else {
				err = pgx.ErrNoRows
			}
		case "restores":
			var req model.RestoreRequest
			if err = decode(w, r, &req); err != nil {
				fail(w, 400, err.Error())
				return
			}
			if req.Conflict == "" {
				req.Conflict = "skip"
			}
			if uuid.Validate(req.Snapshot) != nil || safepath.Relative(req.Destination) != nil || (req.Path != "" && safepath.Relative(req.Path) != nil) || (req.Conflict != "skip" && req.Conflict != "rename" && req.Conflict != "overwrite") {
				fail(w, 400, "invalid restore request")
				return
			}
			var jid string
			jid, err = a.Engine.Repo.NewJob(ctx, "restore", req.Snapshot, req)
			result = map[string]string{"id": jid}
		case "jobs":
			if id == "" || (action != "cancel" && action != "retry") {
				err = pgx.ErrNoRows
			} else {
				err = a.jobAction(ctx, id, action)
				result = map[string]string{"id": id}
			}
		default:
			err = pgx.ErrNoRows
		}
	case http.MethodDelete:
		switch resource {
		case "backups":
			err = a.Engine.DeleteSnapshot(ctx, id)
		case "sources":
			_, err = a.Engine.Repo.Pool.Exec(ctx, "DELETE FROM sources WHERE id=$1", id)
		default:
			err = pgx.ErrNoRows
		}
		result = map[string]string{"id": id}
	default:
		fail(w, 405, "method not allowed")
		return
	}
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			fail(w, 404, "resource not found")
		} else {
			slog.Error("API operation failed", "resource", resource, "error", err)
			fail(w, 409, "operation could not complete; check resource state and server logs")
		}
		return
	}
	respond(w, result)
}
func (a *API) overview(ctx context.Context) (any, error) {
	rows, e := a.query(ctx, `SELECT (SELECT count(*) FROM snapshots) total_backups,(SELECT count(*) FROM snapshots WHERE status='completed') successful_backups,(SELECT count(*) FROM jobs WHERE status='failed') failed_jobs,(SELECT count(*) FROM jobs WHERE status='running') running_jobs,(SELECT COALESCE(sum(logical_bytes),0)::bigint FROM snapshots WHERE status='completed') logical_bytes,(SELECT COALESCE(sum(size),0)::bigint FROM chunks) physical_bytes,(SELECT count(*) FROM chunks) unique_chunks,(SELECT count(*) FROM file_chunks) chunk_references,(SELECT count(*) FROM workers WHERE heartbeat>now()-interval '30 seconds') active_workers,(SELECT count(*) FROM jobs WHERE kind='verify' AND status='failed') failed_verifications,(SELECT max(completed_at) FROM snapshots) latest_recovery_point`)
	if e != nil {
		return nil, e
	}
	return rows[0], nil
}
func (a *API) jobAction(ctx context.Context, id, action string) error {
	tx, e := a.Engine.Repo.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	var status, kind, sid string
	if e = tx.QueryRow(ctx, "SELECT status,kind,snapshot_id::text FROM jobs WHERE id=$1 FOR UPDATE", id).Scan(&status, &kind, &sid); e != nil {
		return e
	}
	if action == "cancel" {
		if status == "completed" || status == "failed" || status == "cancelled" {
			return fmt.Errorf("job is terminal")
		}
		if _, e = tx.Exec(ctx, "UPDATE jobs SET status='cancelled',token=NULL,lease_until=NULL,updated_at=now() WHERE id=$1", id); e != nil {
			return e
		}
		if kind == "backup" {
			if _, e = tx.Exec(ctx, "UPDATE snapshots SET status='cancelled' WHERE id=$1 AND status='pending'", sid); e != nil {
				return e
			}
		}
	} else {
		if status != "failed" && status != "cancelled" {
			return fmt.Errorf("job is not retryable")
		}
		if _, e = tx.Exec(ctx, "UPDATE jobs SET status='queued',attempt=0,error=NULL,token=NULL,next_attempt_at=now(),updated_at=now() WHERE id=$1", id); e != nil {
			return e
		}
		if kind == "backup" {
			if _, e = tx.Exec(ctx, "UPDATE snapshots SET status='pending' WHERE id=$1 AND status IN ('failed','cancelled')", sid); e != nil {
				return e
			}
		}
		if e = metadata.Enqueue(ctx, tx, a.Engine.Repo.Topic, id, 0); e != nil {
			return e
		}
	}
	if _, e = tx.Exec(ctx, "INSERT INTO audit_logs(action,resource_id) VALUES($1,$2)", "job."+action, id); e != nil {
		return e
	}
	return tx.Commit(ctx)
}

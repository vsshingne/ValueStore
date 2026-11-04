package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
	"time"
	"vaultstore/internal/chunking"
	"vaultstore/internal/config"
	"vaultstore/internal/metadata"
	"vaultstore/internal/model"
	"vaultstore/internal/safepath"
	"vaultstore/internal/storage"
)

var uploaded = promauto.NewCounter(prometheus.CounterOpts{Name: "chunks_uploaded_total", Help: "Verified chunk uploads"})
var reused = promauto.NewCounter(prometheus.CounterOpts{Name: "chunks_reused_total", Help: "Verified existing chunks reused"})
var processed = promauto.NewCounter(prometheus.CounterOpts{Name: "backup_bytes_processed_total", Help: "Bytes read by backup attempts"})
var uploadDuration = promauto.NewHistogram(prometheus.HistogramOpts{Name: "chunk_upload_duration_seconds", Help: "Upload and verification duration"})

type Permanent struct{ Err error }

func (p Permanent) Error() string { return p.Err.Error() }
func (p Permanent) Unwrap() error { return p.Err }
func invalid(s string) error      { return Permanent{errors.New(s)} }

type Engine struct {
	Repo   *metadata.Repository
	Store  storage.ObjectStorage
	Config config.Config
}
type ChunkRepository interface {
	Ensure(context.Context, chunking.Chunk, []byte) error
}
type FileScanner interface {
	Scan(context.Context, *os.Root, []string, func(string, fs.DirEntry) error) error
}
type Scanner struct{}

func (Scanner) Scan(ctx context.Context, root *os.Root, excludes []string, visit func(string, fs.DirEntry) error) error {
	return fs.WalkDir(root.FS(), ".", func(p string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if e = ctx.Err(); e != nil {
			return e
		}
		if p == "." {
			return nil
		}
		if e = safepath.Relative(p); e != nil {
			return Permanent{e}
		}
		if d.Type()&os.ModeSymlink != 0 {
			return invalid("symlinks are not supported: " + p)
		}
		for _, pattern := range excludes {
			match, e := path.Match(pattern, p)
			if e != nil {
				return Permanent{e}
			}
			if match {
				if d.IsDir() {
					return fs.SkipDir
				}
				return nil
			}
		}
		return visit(p, d)
	})
}
func (e *Engine) Ensure(ctx context.Context, c chunking.Chunk, b []byte) error {
	tx, err := e.Repo.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", c.Hash); err != nil {
		return err
	}
	key := chunking.Key(c.Hash)
	obj, err := e.Store.Head(ctx, key)
	exists := err == nil
	if err != nil && !storage.Missing(err) {
		return err
	}
	if exists {
		if obj.Size != c.Size {
			return invalid("stored chunk size mismatch: " + c.Hash)
		}
		data, err := storage.Read(ctx, e.Store, key, c.Size)
		if err != nil {
			return err
		}
		if chunking.Hash(data) != c.Hash {
			return invalid("stored chunk corruption: " + c.Hash)
		}
		reused.Inc()
	} else {
		started := time.Now()
		if err = e.Store.Put(ctx, key, bytes.NewReader(b), c.Size); err != nil {
			return err
		}
		data, err := storage.Read(ctx, e.Store, key, c.Size)
		if err != nil {
			return err
		}
		if chunking.Hash(data) != c.Hash {
			return fmt.Errorf("upload verification failed: %s", c.Hash)
		}
		uploaded.Inc()
		uploadDuration.Observe(time.Since(started).Seconds())
	}
	if _, err = tx.Exec(ctx, "INSERT INTO chunks(hash,size,storage_key) VALUES($1,$2,$3) ON CONFLICT(hash) DO UPDATE SET verified_at=now()", c.Hash, c.Size, key); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (e *Engine) Run(ctx context.Context, j model.Job) error {
	release, err := e.Repo.HoldGC(ctx, false)
	if err != nil {
		return err
	}
	defer release()
	switch j.Kind {
	case "backup":
		return e.backup(ctx, j)
	case "restore":
		return e.restore(ctx, j)
	case "verify":
		return e.verify(ctx, j)
	default:
		return invalid("unsupported job kind")
	}
}
func (e *Engine) backup(ctx context.Context, j model.Job) error {
	var source, rel string
	var parent *string
	var excludes []byte
	var created time.Time
	if err := e.Repo.Pool.QueryRow(ctx, "SELECT s.id::text,s.path,s.exclusions,b.parent_id::text,b.created_at FROM snapshots b JOIN sources s ON s.id=b.source_id WHERE b.id=$1", j.Snapshot).Scan(&source, &rel, &excludes, &parent, &created); err != nil {
		return err
	}
	var patterns []string
	if err := json.Unmarshal(excludes, &patterns); err != nil {
		return err
	}
	root, err := safepath.Root(e.Config.BackupRoot, rel)
	if err != nil {
		return err
	}
	defer root.Close()
	m := model.Manifest{Version: 1, ID: j.Snapshot, Source: source, Created: created, Files: []model.File{}}
	if parent != nil {
		m.Parent = *parent
	}
	var total int64
	entries := 0
	err = (Scanner{}).Scan(ctx, root, patterns, func(p string, d fs.DirEntry) error {
		info, err := d.Info()
		if err != nil {
			return err
		}
		f := model.File{Path: p, Mode: uint32(info.Mode().Perm()), Modified: info.ModTime(), Directory: d.IsDir(), Chunks: []chunking.Chunk{}}
		if !d.IsDir() {
			if !info.Mode().IsRegular() {
				return invalid("unsupported special file: " + p)
			}
			file, err := root.Open(p)
			if err != nil {
				return err
			}
			before, err := file.Stat()
			if err != nil {
				file.Close()
				return err
			}
			f.Hash, f.Size, err = (chunking.Fixed{Size: e.Config.ChunkSize}).Split(ctx, file, func(c chunking.Chunk, b []byte) error {
				entries++
				if entries > 100000 {
					return invalid("snapshot exceeds 100000 file/chunk entries; split the source")
				}
				if err := e.Ensure(ctx, c, b); err != nil {
					return err
				}
				f.Chunks = append(f.Chunks, c)
				return nil
			})
			after, statErr := file.Stat()
			closeErr := file.Close()
			if err != nil {
				return err
			}
			if statErr != nil {
				return statErr
			}
			if closeErr != nil {
				return closeErr
			}
			if before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) || f.Size != after.Size() {
				return fmt.Errorf("source changed while reading: %s", p)
			}
			processed.Add(float64(f.Size))
			total += f.Size
			if _, err = e.Repo.Pool.Exec(ctx, "UPDATE jobs SET progress=$3 WHERE id=$1 AND token=$2 AND status='running'", j.ID, j.Token, total); err != nil {
				return err
			}
		}
		entries++
		if entries > 100000 {
			return invalid("snapshot exceeds 100000 file/chunk entries; split the source")
		}
		m.Files = append(m.Files, f)
		return nil
	})
	if err != nil {
		return err
	}
	if err = e.VerifyManifest(ctx, m); err != nil {
		return err
	}
	return e.Repo.SaveManifest(ctx, j, m)
}
func (e *Engine) WriteFile(ctx context.Context, f model.File, w io.Writer) error {
	h := sha256.New()
	var size int64
	for _, c := range f.Chunks {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !chunking.ValidHash(c.Hash) {
			return invalid("invalid chunk hash")
		}
		b, err := storage.Read(ctx, e.Store, chunking.Key(c.Hash), c.Size)
		if err != nil {
			return fmt.Errorf("%s: %w", f.Path, err)
		}
		if chunking.Hash(b) != c.Hash {
			return invalid("chunk corruption in " + f.Path + ": " + c.Hash)
		}
		n, err := io.MultiWriter(w, h).Write(b)
		if err != nil {
			return err
		}
		size += int64(n)
	}
	if size != f.Size || hex.EncodeToString(h.Sum(nil)) != f.Hash {
		return invalid("file integrity mismatch: " + f.Path)
	}
	return nil
}
func (e *Engine) VerifyManifest(ctx context.Context, m model.Manifest) error {
	for _, f := range m.Files {
		if err := safepath.Relative(f.Path); err != nil {
			return Permanent{err}
		}
		if !f.Directory {
			if err := e.WriteFile(ctx, f, io.Discard); err != nil {
				return err
			}
		}
	}
	return nil
}
func (e *Engine) verify(ctx context.Context, j model.Job) error {
	m, err := e.Repo.LoadManifest(ctx, j.Snapshot)
	if err != nil {
		return err
	}
	if err = e.VerifyManifest(ctx, m); err != nil {
		return err
	}
	return e.Repo.Complete(ctx, j, map[string]any{"verified_files": len(m.Files), "checksum": "passed"})
}
func Selected(p, selection string) bool {
	return selection == "" || selection == "." || p == selection || strings.HasPrefix(p, selection+"/")
}
func (e *Engine) restore(ctx context.Context, j model.Job) error {
	var req model.RestoreRequest
	if err := json.Unmarshal(j.Payload, &req); err != nil {
		return Permanent{err}
	}
	m, err := e.Repo.LoadManifest(ctx, j.Snapshot)
	if err != nil {
		return err
	}
	if err = safepath.Relative(req.Destination); err != nil {
		return Permanent{err}
	}
	if req.Path != "" {
		if err = safepath.Relative(req.Path); err != nil {
			return Permanent{err}
		}
	}
	if req.Conflict != "skip" && req.Conflict != "rename" && req.Conflict != "overwrite" {
		return invalid("invalid conflict policy")
	}
	base, err := os.OpenRoot(e.Config.RestoreRoot)
	if err != nil {
		return err
	}
	defer base.Close()
	if err = base.MkdirAll(req.Destination, 0750); err != nil {
		return err
	}
	dest, err := base.OpenRoot(req.Destination)
	if err != nil {
		return err
	}
	defer dest.Close()
	done, skipped := 0, 0
	for _, f := range m.Files {
		if !Selected(f.Path, req.Path) {
			continue
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		if err = safepath.Relative(f.Path); err != nil {
			return Permanent{err}
		}
		if f.Directory {
			if err = dest.MkdirAll(f.Path, 0750); err != nil {
				return err
			}
			continue
		}
		if err = dest.MkdirAll(path.Dir(f.Path), 0750); err != nil {
			return err
		}
		target := f.Path
		if req.Conflict == "rename" {
			target = f.Path + ".restored-" + j.ID
		}
		info, statErr := dest.Lstat(target)
		if statErr == nil {
			if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
				return invalid("unsafe restore target: " + target)
			}
			if req.Conflict == "skip" {
				skipped++
				continue
			}
			if req.Conflict == "rename" {
				existing, er := dest.Open(target)
				if er != nil {
					return er
				}
				h := sha256.New()
				_, er = io.Copy(h, existing)
				existing.Close()
				if er != nil {
					return er
				}
				if hex.EncodeToString(h.Sum(nil)) == f.Hash {
					done++
					continue
				}
				return invalid("rename destination already exists")
			}
		} else if !errors.Is(statErr, fs.ErrNotExist) {
			return statErr
		}
		tmp := path.Join(path.Dir(target), ".vaultstore-"+uuid.NewString())
		file, er := dest.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if er != nil {
			return er
		}
		er = e.WriteFile(ctx, f, file)
		if er == nil {
			er = file.Sync()
		}
		if er == nil {
			er = file.Chmod(fs.FileMode(f.Mode) & 0777)
		}
		closeErr := file.Close()
		if er == nil {
			er = closeErr
		}
		if er == nil {
			er = e.Repo.Heartbeat(ctx, j, e.Config.Lease)
		}
		if er == nil {
			if req.Conflict == "overwrite" {
				er = dest.Rename(tmp, target)
			} else {
				er = dest.Link(tmp, target)
				if errors.Is(er, fs.ErrExist) && req.Conflict == "skip" {
					skipped++
					er = nil
				} else if er == nil {
					done++
				}
			}
		}
		cleanup := dest.Remove(tmp)
		if er != nil {
			return er
		}
		if cleanup != nil && !errors.Is(cleanup, fs.ErrNotExist) {
			return cleanup
		}
		if req.Conflict == "overwrite" {
			done++
		}
	}
	return e.Repo.Complete(ctx, j, map[string]int{"restored_files": done, "skipped_files": skipped})
}

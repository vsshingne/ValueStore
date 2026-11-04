package engine

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"vaultstore/internal/chunking"
	"vaultstore/internal/model"
	"vaultstore/internal/storage"
)

// Memory objects are exclusively a test double; production uses MinIO.
type testObjects struct {
	mu        sync.Mutex
	objects   map[string][]byte
	failAfter int
}

func (s *testObjects) Put(_ context.Context, k string, r io.Reader, n int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, e := io.ReadAll(r)
	if e != nil {
		return e
	}
	s.objects[k] = b
	return nil
}
func (s *testObjects) Get(_ context.Context, k string) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failAfter == 0 {
		return nil, errors.New("injected interrupted download")
	}
	if s.failAfter > 0 {
		s.failAfter--
	}
	return io.NopCloser(bytes.NewReader(append([]byte{}, s.objects[k]...))), nil
}
func (s *testObjects) Head(_ context.Context, k string) (storage.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.objects[k]
	if !ok {
		return storage.Object{}, fs.ErrNotExist
	}
	return storage.Object{Key: k, Size: int64(len(b))}, nil
}
func (s *testObjects) Delete(_ context.Context, k string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, k)
	return nil
}
func (s *testObjects) List(_ context.Context, f func(storage.Object) error) error { return nil }
func fixture(t *testing.T) (*Engine, model.File, []byte) {
	t.Helper()
	s := &testObjects{objects: map[string][]byte{}, failAfter: -1}
	data := bytes.Repeat([]byte{0, 255, 2, 8, 9}, 10000)
	f := model.File{Path: "binary.bin", Chunks: []chunking.Chunk{}}
	var err error
	f.Hash, f.Size, err = (chunking.Fixed{Size: 4096}).Split(context.Background(), bytes.NewReader(data), func(c chunking.Chunk, b []byte) error {
		f.Chunks = append(f.Chunks, c)
		s.objects[chunking.Key(c.Hash)] = append([]byte{}, b...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return &Engine{Store: s}, f, data
}
func TestReconstructionAndRecovery(t *testing.T) {
	e, f, want := fixture(t)
	s := e.Store.(*testObjects)
	s.failAfter = 2
	var partial bytes.Buffer
	if err := e.WriteFile(context.Background(), f, &partial); err == nil {
		t.Fatal("interruption not detected")
	}
	if partial.Len() == 0 || partial.Len() >= len(want) {
		t.Fatal("fault was not mid-stream")
	}
	s.failAfter = -1
	var recovered bytes.Buffer
	if err := e.WriteFile(context.Background(), f, &recovered); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(recovered.Bytes(), want) {
		t.Fatal("retry did not reconstruct original")
	}
}
func TestCorruptionFailsAndRepairRecovers(t *testing.T) {
	e, f, want := fixture(t)
	s := e.Store.(*testObjects)
	key := chunking.Key(f.Chunks[0].Hash)
	original := append([]byte{}, s.objects[key]...)
	s.objects[key][0] ^= 255
	var out bytes.Buffer
	if err := e.WriteFile(context.Background(), f, &out); err == nil {
		t.Fatal("corruption accepted")
	}
	if out.Len() != 0 {
		t.Fatal("corrupt chunk written")
	}
	s.objects[key] = original
	out.Reset()
	if err := e.WriteFile(context.Background(), f, &out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(want, out.Bytes()) {
		t.Fatal("repair failed")
	}
}
func TestEmptyAndFileHash(t *testing.T) {
	e := &Engine{Store: &testObjects{}}
	f := model.File{Hash: chunking.Hash(nil)}
	if err := e.WriteFile(context.Background(), f, io.Discard); err != nil {
		t.Fatal(err)
	}
	f.Hash = chunking.Hash([]byte("other"))
	if err := e.WriteFile(context.Background(), f, io.Discard); err == nil {
		t.Fatal("file hash ignored")
	}
}
func TestScannerExcludesAndSymlink(t *testing.T) {
	base := t.TempDir()
	if err := os.WriteFile(filepath.Join(base, "keep.txt"), []byte("ok"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(base, "cache"), 0700); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(base)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	var got []string
	err = (Scanner{}).Scan(context.Background(), root, []string{"cache"}, func(p string, d fs.DirEntry) error { got = append(got, p); return nil })
	if err != nil || len(got) != 1 || got[0] != "keep.txt" {
		t.Fatalf("%v %v", got, err)
	}
	if err = os.Symlink(t.TempDir(), filepath.Join(base, "escape")); err != nil {
		t.Logf("symlink creation unavailable: %v", err)
		return
	}
	if err = (Scanner{}).Scan(context.Background(), root, nil, func(string, fs.DirEntry) error { return nil }); err == nil {
		t.Fatal("symlink accepted")
	}
}
func TestSelection(t *testing.T) {
	if !Selected("a/b", "a") || Selected("ab/c", "a") || !Selected("x", "") {
		t.Fatal("incorrect directory selection")
	}
}
func TestConcurrentReconstruction(t *testing.T) {
	e, f, want := fixture(t)
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var b bytes.Buffer
			if err := e.WriteFile(context.Background(), f, &b); err != nil {
				t.Error(err)
			}
			if !bytes.Equal(b.Bytes(), want) {
				t.Error("concurrent restore changed content")
			}
		}()
	}
	wg.Wait()
}

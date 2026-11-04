package storage

import (
	"context"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"time"
	"vaultstore/internal/config"
)

type Object struct {
	Key      string
	Size     int64
	Modified time.Time
}
type ObjectStorage interface {
	Put(context.Context, string, io.Reader, int64) error
	Get(context.Context, string) (io.ReadCloser, error)
	Head(context.Context, string) (Object, error)
	Delete(context.Context, string) error
	List(context.Context, func(Object) error) error
}
type MinIO struct {
	client *minio.Client
	bucket string
}

func New(c config.Config) (*MinIO, error) {
	cl, e := minio.New(c.Endpoint, &minio.Options{Creds: credentials.NewStaticV4(c.AccessKey, c.SecretKey, ""), Secure: c.Secure})
	return &MinIO{client: cl, bucket: c.Bucket}, e
}
func (s *MinIO) Ensure(ctx context.Context) error {
	ok, e := s.client.BucketExists(ctx, s.bucket)
	if e != nil {
		return e
	}
	if !ok {
		e = s.client.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{})
		if e != nil {
			ok, check := s.client.BucketExists(ctx, s.bucket)
			if check != nil || !ok {
				return e
			}
		}
	}
	return nil
}
func (s *MinIO) Put(ctx context.Context, k string, r io.Reader, n int64) error {
	_, e := s.client.PutObject(ctx, s.bucket, k, r, n, minio.PutObjectOptions{ContentType: "application/octet-stream", DisableMultipart: true})
	return e
}
func (s *MinIO) Get(ctx context.Context, k string) (io.ReadCloser, error) {
	return s.client.GetObject(ctx, s.bucket, k, minio.GetObjectOptions{})
}
func (s *MinIO) Head(ctx context.Context, k string) (Object, error) {
	v, e := s.client.StatObject(ctx, s.bucket, k, minio.StatObjectOptions{})
	return Object{Key: k, Size: v.Size, Modified: v.LastModified}, e
}
func (s *MinIO) Delete(ctx context.Context, k string) error {
	return s.client.RemoveObject(ctx, s.bucket, k, minio.RemoveObjectOptions{})
}
func (s *MinIO) List(ctx context.Context, fn func(Object) error) error {
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	for v := range s.client.ListObjects(child, s.bucket, minio.ListObjectsOptions{Prefix: "chunks/", Recursive: true}) {
		if v.Err != nil {
			return v.Err
		}
		if e := fn(Object{v.Key, v.Size, v.LastModified}); e != nil {
			return e
		}
	}
	return ctx.Err()
}
func Missing(e error) bool {
	r := minio.ToErrorResponse(e)
	return r.Code == "NoSuchKey" || r.Code == "NoSuchObject" || r.Code == "NotFound"
}
func Read(ctx context.Context, s ObjectStorage, k string, n int64) ([]byte, error) {
	if n < 0 || n > 64<<20 {
		return nil, fmt.Errorf("invalid object size")
	}
	r, e := s.Get(ctx, k)
	if e != nil {
		return nil, e
	}
	defer r.Close()
	b, e := io.ReadAll(io.LimitReader(r, n+1))
	if e == nil && int64(len(b)) != n {
		e = fmt.Errorf("object size mismatch")
	}
	return b, e
}

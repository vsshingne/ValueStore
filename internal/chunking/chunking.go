package chunking

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
)

type Chunk struct {
	Hash string `json:"hash"`
	Size int64  `json:"size"`
}
type Chunker interface {
	Split(context.Context, io.Reader, func(Chunk, []byte) error) (string, int64, error)
}
type Hasher interface{ Sum([]byte) string }
type SHA256 struct{}

func (SHA256) Sum(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func Hash(b []byte) string         { return (SHA256{}).Sum(b) }
func Key(h string) string          { return "chunks/" + h[:2] + "/" + h[2:4] + "/" + h }
func ValidHash(h string) bool {
	b, e := hex.DecodeString(h)
	return e == nil && len(b) == 32 && hex.EncodeToString(b) == h
}

type Fixed struct{ Size int }

func (f Fixed) Split(ctx context.Context, r io.Reader, visit func(Chunk, []byte) error) (string, int64, error) {
	if f.Size < 1 || f.Size > 64<<20 {
		return "", 0, fmt.Errorf("invalid chunk size")
	}
	b := make([]byte, f.Size)
	whole := sha256.New()
	var size int64
	for {
		if e := ctx.Err(); e != nil {
			return "", size, e
		}
		n, e := io.ReadFull(r, b)
		if e != nil && e != io.EOF && e != io.ErrUnexpectedEOF {
			return "", size, e
		}
		if n > 0 {
			part := b[:n]
			whole.Write(part)
			size += int64(n)
			if err := visit(Chunk{Hash: Hash(part), Size: int64(n)}, part); err != nil {
				return "", size, err
			}
		}
		if e != nil {
			break
		}
	}
	return hex.EncodeToString(whole.Sum(nil)), size, nil
}

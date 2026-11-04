package model

import (
	"encoding/json"
	"fmt"
	"time"
	"vaultstore/internal/chunking"
)

type File struct {
	Path      string           `json:"path"`
	Size      int64            `json:"size"`
	Hash      string           `json:"hash"`
	Mode      uint32           `json:"mode"`
	Modified  time.Time        `json:"modified"`
	Directory bool             `json:"directory"`
	Chunks    []chunking.Chunk `json:"chunks"`
}
type Manifest struct {
	Version int       `json:"version"`
	ID      string    `json:"id"`
	Parent  string    `json:"parent,omitempty"`
	Source  string    `json:"source"`
	Created time.Time `json:"created"`
	Files   []File    `json:"files"`
}

func (m Manifest) Encode() ([]byte, string, error) {
	b, e := json.Marshal(m)
	return b, chunking.Hash(b), e
}
func Decode(b []byte, h string) (Manifest, error) {
	var m Manifest
	if chunking.Hash(b) != h {
		return m, fmt.Errorf("manifest checksum mismatch")
	}
	e := json.Unmarshal(b, &m)
	if e == nil && m.Version != 1 {
		e = fmt.Errorf("unsupported manifest version")
	}
	return m, e
}

type Job struct {
	ID       string          `json:"id"`
	Kind     string          `json:"kind"`
	Status   string          `json:"status"`
	Snapshot string          `json:"snapshot_id"`
	Source   string          `json:"source_id"`
	Payload  json.RawMessage `json:"payload"`
	Attempt  int             `json:"attempt"`
	Token    string          `json:"-"`
}
type Event struct {
	Version   int       `json:"schema_version"`
	ID        string    `json:"event_id"`
	JobID     string    `json:"job_id"`
	Requested time.Time `json:"requested_at"`
}
type RestoreRequest struct {
	Snapshot    string `json:"snapshot_id"`
	Destination string `json:"destination"`
	Path        string `json:"path"`
	Conflict    string `json:"conflict"`
}

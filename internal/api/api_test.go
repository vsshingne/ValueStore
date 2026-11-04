package api

import (
	"net/http/httptest"
	"strings"
	"testing"
	"vaultstore/internal/config"
	"vaultstore/internal/engine"
)

func TestAuthAndHealth(t *testing.T) {
	a := &API{Engine: &engine.Engine{Config: config.Config{Token: strings.Repeat("a", 32)}}}
	for _, v := range []struct {
		path string
		code int
	}{{"/api/v1/health", 200}, {"/api/v1/backups", 401}, {"/api/v1/sources", 401}} {
		w := httptest.NewRecorder()
		a.Handler().ServeHTTP(w, httptest.NewRequest("GET", v.path, nil))
		if w.Code != v.code {
			t.Fatalf("%s: %d", v.path, w.Code)
		}
	}
}
func TestStrictBody(t *testing.T) {
	for _, body := range []string{`{"unknown":1}`, `{} {}`, strings.Repeat("x", 1<<20+1)} {
		r := httptest.NewRequest("POST", "/", strings.NewReader(body))
		if decode(httptest.NewRecorder(), r, &struct{}{}) == nil {
			t.Fatal("invalid body accepted")
		}
	}
}

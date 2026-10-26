package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/DenisBorzenkov/ddia-2026/hw-1/internal/store"
)

func newServer() http.Handler {
	r := chi.NewRouter()
	r.Get("/health", Health)
	New(store.New()).Routes(r)
	return r
}

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	return rec
}

func TestItemsFlow(t *testing.T) {
	h := newServer()

	rec := do(t, h, http.MethodPost, "/items", `{"name":"a","value":"1"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rec.Code, rec.Body)
	}
	var it store.Item
	if err := json.Unmarshal(rec.Body.Bytes(), &it); err != nil || it.ID != 1 {
		t.Fatalf("create body: %s", rec.Body)
	}

	if rec := do(t, h, http.MethodGet, "/items/1", ""); rec.Code != http.StatusOK {
		t.Fatalf("get: %d", rec.Code)
	}

	rec = do(t, h, http.MethodPut, "/items/1", `{"name":"b"}`)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"name":"b"`) {
		t.Fatalf("update: %d %s", rec.Code, rec.Body)
	}
}

func TestErrors(t *testing.T) {
	h := newServer()
	cases := []struct {
		method, path, body string
		code               int
	}{
		{http.MethodGet, "/items/42", "", http.StatusNotFound},
		{http.MethodPut, "/items/42", `{}`, http.StatusNotFound},
		{http.MethodGet, "/items/abc", "", http.StatusBadRequest},
		{http.MethodPost, "/items", `{`, http.StatusBadRequest},
		{http.MethodPut, "/items/1", `{`, http.StatusBadRequest},
	}
	for _, c := range cases {
		if rec := do(t, h, c.method, c.path, c.body); rec.Code != c.code {
			t.Errorf("%s %s: got %d, want %d", c.method, c.path, rec.Code, c.code)
		}
	}
}

func TestHealth(t *testing.T) {
	if rec := do(t, newServer(), http.MethodGet, "/health", ""); rec.Code != http.StatusOK {
		t.Fatalf("health: %d", rec.Code)
	}
}

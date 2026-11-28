package httpapi

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"highload/lab1/internal/item"
)

func TestInternalErrorHidesStorageDetails(t *testing.T) {
	t.Parallel()

	secret := errors.New("secret-disk-failure")
	api, err := New(failingStore{err: secret}, NewHealth(), 1024, testMetrics(t))
	if err != nil {
		t.Fatalf("new api: %v", err)
	}
	api.health.SetReady(true)

	for _, method := range []string{http.MethodPost, http.MethodGet, http.MethodPut} {
		path := "/items"
		body := ""
		if method != http.MethodGet {
			body = `{"name":"a","value":"b"}`
		}
		if method != http.MethodPost {
			path = "/items/any"
		}
		rec := performRaw(api.Handler(), method, path, body)
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("%s status = %d, body %s", method, rec.Code, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), "secret-disk-failure") {
			t.Fatalf("%s leaked storage error: %s", method, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), `"error":"internal error"`) &&
			!strings.Contains(rec.Body.String(), `"error": "internal error"`) {
			t.Fatalf("%s body = %s", method, rec.Body.String())
		}
	}
}

func TestPanicResponseOmitsPanicValue(t *testing.T) {
	t.Parallel()

	handler := withRecovery(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("secret-panic-token")
	}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "secret-panic-token") || !strings.Contains(rec.Body.String(), "internal error") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestPanicAfterHeadersDoesNotReplaceStatus(t *testing.T) {
	t.Parallel()

	handler := withRecovery(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = ioWrite(w, "partial")
		panic("secret-panic-token")
	}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d", rec.Code)
	}
	if rec.Body.String() != "partial" || strings.Contains(rec.Body.String(), "secret-panic-token") {
		t.Fatalf("body = %q", rec.Body.String())
	}
}

func TestNewRejectsIncompleteDependencies(t *testing.T) {
	t.Parallel()

	if _, err := New(nil, NewHealth(), 1024, testMetrics(t)); err == nil {
		t.Fatal("nil store was accepted")
	}
	var empty *item.Store
	if _, err := New(empty, NewHealth(), 1024, testMetrics(t)); err == nil {
		t.Fatal("nil store pointer was accepted")
	}
	if _, err := New(item.NewStore(), nil, 1024, testMetrics(t)); err == nil {
		t.Fatal("nil health was accepted")
	}
	if _, err := New(item.NewStore(), NewHealth(), 0, testMetrics(t)); err == nil {
		t.Fatal("zero body limit was accepted")
	}
	if _, err := New(item.NewStore(), NewHealth(), 1024, nil); err == nil {
		t.Fatal("nil metrics was accepted")
	}
}

type failingStore struct {
	err error
}

func (f failingStore) Create(string, string) (item.Item, error) {
	return item.Item{}, f.err
}

func (f failingStore) Get(string) (item.Item, error) {
	return item.Item{}, f.err
}

func (f failingStore) Update(string, string, string) (item.Item, error) {
	return item.Item{}, f.err
}

func performRaw(handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func ioWrite(w http.ResponseWriter, body string) (int, error) {
	return w.Write([]byte(body))
}

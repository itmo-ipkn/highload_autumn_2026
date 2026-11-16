package dashboard

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// fakeGrafana запоминает вызовы и отвечает 404 на lookup, если пользователя нет.
func fakeGrafana(t *testing.T, exists bool) (*Grafana, *[]string) {
	t.Helper()
	var calls []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.Method+" "+r.URL.Path)
		if u, p, _ := r.BasicAuth(); u != "admin" || p != "secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch {
		case r.URL.Path == "/api/users/lookup" && !exists:
			w.WriteHeader(http.StatusNotFound)
		case r.URL.Path == "/api/org/users/7":
			var body map[string]string
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body["role"] != "Viewer" {
				t.Errorf("role: %q", body["role"])
			}
			_, _ = w.Write([]byte(`{}`))
		default:
			_, _ = w.Write([]byte(`{"id":7}`))
		}
	}))
	t.Cleanup(srv.Close)
	g := &Grafana{Client: Client{HTTP: srv.Client()}, URL: srv.URL, User: "admin", Password: "secret"}
	return g, &calls
}

func TestEnsureViewerCreates(t *testing.T) {
	g, calls := fakeGrafana(t, false)
	if err := g.EnsureViewer(context.Background(), "viewer", "pw"); err != nil {
		t.Fatal(err)
	}
	want := []string{"GET /api/users/lookup", "POST /api/admin/users", "PATCH /api/org/users/7"}
	assertCalls(t, *calls, want)
}

func TestEnsureViewerUpdatesPassword(t *testing.T) {
	g, calls := fakeGrafana(t, true)
	if err := g.EnsureViewer(context.Background(), "viewer", "pw"); err != nil {
		t.Fatal(err)
	}
	want := []string{"GET /api/users/lookup", "PUT /api/admin/users/7/password", "PATCH /api/org/users/7"}
	assertCalls(t, *calls, want)
}

func assertCalls(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("calls: %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("call %d: got %s, want %s", i, got[i], want[i])
		}
	}
}

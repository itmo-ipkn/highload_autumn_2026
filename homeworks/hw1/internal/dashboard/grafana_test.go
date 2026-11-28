package dashboard

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestApplyCreatesThenUpdatesNonEditableDashboard(t *testing.T) {
	t.Parallel()

	const password = `s3cret"value`
	grafana := newFakeGrafana(t, "admin", password)
	gone, err := Build([]Endpoint{{Method: "GET", Path: "/gone"}})
	if err != nil {
		t.Fatalf("build old dashboard: %v", err)
	}
	grafana.store(resource{
		APIVersion: apiVersion,
		Kind:       apiKind,
		Metadata:   metadata{Name: dashboardUID, ResourceVersion: "7"},
		Spec:       gone,
	})

	spec, err := Build([]Endpoint{
		{Method: "GET", Path: "/stay"},
		{Method: "PUT", Path: "/new"},
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	err = Apply(t.Context(), grafana.client(), Config{
		AppBaseURL: "http://app.example",
		GrafanaURL: grafana.url,
		Username:   "admin",
		Password:   password,
		Attempts:   2,
		Backoff:    -1,
	}, spec)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if grafana.posts != 0 || grafana.puts != 1 {
		t.Fatalf("posts=%d puts=%d", grafana.posts, grafana.puts)
	}
	if grafana.putVersion != "7" {
		t.Fatalf("put resourceVersion = %s", grafana.putVersion)
	}
	stored := grafana.current()
	if isEditable(stored.Spec) {
		t.Fatal("stored dashboard is editable")
	}
	joined := strings.Join(expressionsOf(stored.Spec), "\n")
	if strings.Contains(joined, `/gone`) || !strings.Contains(joined, `/stay`) || !strings.Contains(joined, `/new`) {
		t.Fatalf("stored queries = %s", joined)
	}
	if !strings.HasPrefix(grafana.authorization, "Basic ") {
		t.Fatal("authorization is not HTTP Basic")
	}
}

func TestApplyRetriesConflictAndTransientErrors(t *testing.T) {
	t.Parallel()

	const password = "s3cret-value"
	grafana := newFakeGrafana(t, "admin", password)
	grafana.failGets = 2
	grafana.failPosts = 1

	spec, err := Build([]Endpoint{{Method: "GET", Path: "/widgets/{id}"}})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	err = Apply(t.Context(), grafana.client(), Config{
		AppBaseURL: "http://app.example",
		GrafanaURL: grafana.url,
		Username:   "admin",
		Password:   password,
		Attempts:   3,
		Backoff:    -1,
	}, spec)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if grafana.posts != 1 || grafana.puts != 1 {
		t.Fatalf("posts=%d puts=%d gets=%d", grafana.posts, grafana.puts, grafana.gets)
	}
	if isEditable(grafana.current().Spec) {
		t.Fatal("stored dashboard is editable")
	}
}

func TestApplyDoesNotSucceedWhenSaveDisappears(t *testing.T) {
	t.Parallel()

	grafana := newFakeGrafana(t, "admin", "s3cret-value")
	grafana.dropAfterWrite = true
	spec, err := Build([]Endpoint{{Method: "GET", Path: "/widgets"}})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	err = Apply(t.Context(), grafana.client(), Config{
		AppBaseURL: "http://app.example",
		GrafanaURL: grafana.url,
		Username:   "admin",
		Password:   "s3cret-value",
		Attempts:   1,
		Backoff:    -1,
	}, spec)
	if err == nil {
		t.Fatal("expected an error")
	}
}

func TestGenerateRedactsPasswordAndSkipsBusinessRoutes(t *testing.T) {
	t.Parallel()

	const password = "s3cret-value"
	var calls []string
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.Method+" "+r.URL.RequestURI())
		if r.Header.Get("Authorization") != "" {
			t.Errorf("app request included authorization")
		}
		if r.URL.Path != "/handlers" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"handlers": []map[string]string{{"method": "GET", "path": "/widgets/{id}"}},
		})
	}))
	t.Cleanup(app.Close)

	grafana := newFakeGrafana(t, "admin", password)
	grafana.rejectAuth = true
	_, err := Generate(t.Context(), &http.Client{}, Config{
		AppBaseURL: app.URL,
		GrafanaURL: grafana.url,
		Username:   "admin",
		Password:   password,
		Attempts:   3,
		Backoff:    -1,
	})
	if err == nil {
		t.Fatal("expected an error")
	}
	if strings.Contains(err.Error(), password) {
		t.Fatalf("error contains the password: %s", err)
	}
	if grafana.requests != 1 {
		t.Fatalf("auth failure was retried %d times", grafana.gets)
	}
	if strings.Join(calls, ",") != "GET /handlers" {
		t.Fatalf("calls = %v", calls)
	}
}

func TestGenerateSavesCatalogRoute(t *testing.T) {
	t.Parallel()

	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/handlers" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"handlers": []map[string]string{
				{"method": "POST", "path": "/widgets"},
				{"method": "GET", "path": "/widgets/{id}"},
			},
		})
	}))
	t.Cleanup(app.Close)
	grafana := newFakeGrafana(t, "admin", "s3cret-value")

	result, err := Generate(t.Context(), &http.Client{}, Config{
		AppBaseURL: app.URL,
		GrafanaURL: grafana.url,
		Username:   "admin",
		Password:   "s3cret-value",
		Attempts:   2,
		Backoff:    -1,
	})
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if result.UID != dashboardUID || result.Panels != 6 {
		t.Fatalf("result = %+v", result)
	}
	stored := strings.Join(expressionsOf(grafana.current().Spec), "\n")
	if !strings.Contains(stored, `route="/widgets/{id}"`) || isEditable(grafana.current().Spec) {
		t.Fatalf("stored dashboard = %s editable=%v", stored, grafana.current().Spec.Editable)
	}
}

func TestGenerateRequiresClient(t *testing.T) {
	t.Parallel()

	_, err := Generate(t.Context(), nil, Config{
		AppBaseURL: "http://app.example",
		GrafanaURL: "http://grafana.example",
		Username:   "admin",
		Password:   "s3cret-value",
	})
	if err == nil {
		t.Fatal("nil client was accepted")
	}
}

func TestNewHTTPClientDoesNotFollowRedirects(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://example.invalid/next", http.StatusFound)
	}))
	t.Cleanup(server.Close)

	client := NewHTTPClient()
	resp, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if client.Timeout != requestTimeout {
		t.Fatalf("timeout = %s", client.Timeout)
	}
}

type fakeGrafana struct {
	t              *testing.T
	url            string
	user           string
	password       string
	mu             sync.Mutex
	stored         *resource
	gets           int
	requests       int
	posts          int
	puts           int
	failGets       int
	failPosts      int
	dropAfterWrite bool
	rejectAuth     bool
	putVersion     string
	authorization  string
	httpClient     *http.Client
}

func newFakeGrafana(t *testing.T, user, password string) *fakeGrafana {
	t.Helper()
	fake := &fakeGrafana{t: t, user: user, password: password}
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	fake.url = server.URL
	fake.httpClient = server.Client()
	return fake
}

func (f *fakeGrafana) client() *http.Client { return f.httpClient }

func (f *fakeGrafana) store(document resource) {
	f.mu.Lock()
	f.stored = &document
	f.mu.Unlock()
}

func (f *fakeGrafana) current() resource {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stored == nil {
		return resource{}
	}
	return *f.stored
}

func (f *fakeGrafana) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	user, password, _ := r.BasicAuth()
	f.authorization = r.Header.Get("Authorization")
	f.requests++
	if f.rejectAuth || user != f.user || password != f.password {
		http.Error(w, "denied "+f.password, http.StatusUnauthorized)
		return
	}
	switch {
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/dashboards/"+dashboardUID):
		f.gets++
		if f.failGets > 0 {
			f.failGets--
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		if f.stored == nil {
			http.NotFound(w, r)
			return
		}
		writeResource(f.t, w, *f.stored)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/dashboards"):
		f.posts++
		if f.failPosts > 0 {
			f.failPosts--
			existing, err := Build([]Endpoint{{Method: "GET", Path: "/race"}})
			if err != nil {
				f.t.Errorf("race dashboard: %v", err)
			}
			f.stored = &resource{
				APIVersion: apiVersion,
				Kind:       apiKind,
				Metadata:   metadata{Name: dashboardUID, ResourceVersion: "4"},
				Spec:       existing,
			}
			http.Error(w, "already exists", http.StatusConflict)
			return
		}
		document, err := readResource(r)
		if err != nil {
			f.t.Errorf("read create: %v", err)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		document.Metadata.Name = dashboardUID
		document.Metadata.ResourceVersion = "1"
		if f.dropAfterWrite {
			w.WriteHeader(http.StatusCreated)
			writeResource(f.t, w, document)
			f.stored = nil
			return
		}
		f.stored = &document
		w.WriteHeader(http.StatusCreated)
		writeResource(f.t, w, document)
	case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/dashboards/"+dashboardUID):
		f.puts++
		document, err := readResource(r)
		if err != nil {
			f.t.Errorf("read update: %v", err)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		f.putVersion = document.Metadata.ResourceVersion
		if f.stored == nil || document.Metadata.ResourceVersion != f.stored.Metadata.ResourceVersion {
			http.Error(w, "conflict", http.StatusConflict)
			return
		}
		document.Metadata.ResourceVersion = "2"
		f.stored = &document
		writeResource(f.t, w, document)
	default:
		http.NotFound(w, r)
	}
}

func readResource(r *http.Request) (resource, error) {
	defer r.Body.Close()
	var document resource
	err := json.NewDecoder(r.Body).Decode(&document)
	return document, err
}

func writeResource(t *testing.T, w http.ResponseWriter, document resource) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(document); err != nil {
		t.Errorf("write dashboard: %v", err)
	}
}

func TestConfigFromEnv(t *testing.T) {
	t.Parallel()

	cfg, err := ConfigFromEnv(func(key string) string {
		values := map[string]string{
			"APP_BASE_URL":           "http://app-1:8080",
			"GRAFANA_URL":            "http://grafana:3000",
			"GRAFANA_ADMIN_USER":     "admin",
			"GRAFANA_ADMIN_PASSWORD": "s3cret-value",
		}
		return values[key]
	})
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	if cfg.AppBaseURL != "http://app-1:8080" || cfg.Username != "admin" {
		t.Fatalf("config = %+v", cfg)
	}
}

func TestConfigRejectsSecretsInURL(t *testing.T) {
	t.Parallel()

	_, err := ConfigFromEnv(func(key string) string {
		values := map[string]string{
			"APP_BASE_URL":           "http://admin:s3cret-value@app-1:8080",
			"GRAFANA_URL":            "http://grafana:3000",
			"GRAFANA_ADMIN_USER":     "admin",
			"GRAFANA_ADMIN_PASSWORD": "s3cret-value",
		}
		return values[key]
	})
	if err == nil {
		t.Fatal("expected an error")
	}
	if strings.Contains(err.Error(), "s3cret-value") {
		t.Fatalf("error contains the password: %s", err)
	}
}

func TestConfigRequiresPassword(t *testing.T) {
	t.Parallel()

	_, err := ConfigFromEnv(func(key string) string {
		values := map[string]string{
			"APP_BASE_URL":       "http://app-1:8080",
			"GRAFANA_URL":        "http://grafana:3000",
			"GRAFANA_ADMIN_USER": "admin",
		}
		return values[key]
	})
	if err == nil || !strings.Contains(err.Error(), "GRAFANA_ADMIN_PASSWORD") {
		t.Fatalf("error = %v", err)
	}
}

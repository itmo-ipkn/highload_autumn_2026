package dashboard

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDiscoverMergesCatalogs(t *testing.T) {
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/handlers" || r.Header.Get("Authorization") != "" {
			t.Error("неверный запрос к приложению")
		}
		json.NewEncoder(w).Encode(Object{"handlers": []Endpoint{{Method: "GET", Path: "/a"}}})
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(Object{"handlers": []Endpoint{{Method: "GET", Path: "/a"}, {Method: "PUT", Path: "/b"}}})
	}))
	defer second.Close()
	routes, err := Discover(context.Background(), first.Client(), []string{first.URL, second.URL})
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 2 || routes[1].Path != "/b" {
		t.Fatalf("неверное объединение: %+v", routes)
	}
}

func TestDiscoverRejectsBadCatalog(t *testing.T) {
	for _, body := range []string{`{`, `{"handlers":[]}`, `{"handlers":[{"method":"GET","path":"bad"}]}`} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(body)) }))
			defer server.Close()
			if _, err := Discover(context.Background(), server.Client(), []string{server.URL}); err == nil {
				t.Fatal("некорректный каталог принят")
			}
		})
	}
}

func TestPublishUpdatesSameDashboard(t *testing.T) {
	writes := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != "test" || password != "example" {
			t.Error("нет авторизации Grafana")
		}
		if r.URL.Path == "/api/datasources/uid/"+DatasourceUID {
			w.Write([]byte(`{}`))
			return
		}
		if r.URL.Path != "/api/dashboards/db" || r.Method != "POST" {
			t.Error("неверный запрос публикации")
		}
		var payload struct {
			Dashboard Object `json:"dashboard"`
			Overwrite bool   `json:"overwrite"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		if !payload.Overwrite || payload.Dashboard["uid"] != UID {
			t.Error("повторный запуск создаст другой дашборд")
		}
		writes++
		json.NewEncoder(w).Encode(Object{"uid": UID, "status": "success"})
	}))
	defer server.Close()
	client := Client{HTTP: server.Client(), Username: "test", Password: "example"}
	for i := 0; i < 2; i++ {
		if err := client.Publish(context.Background(), server.URL, Build(nil)); err != nil {
			t.Fatal(err)
		}
	}
	if writes != 2 {
		t.Fatalf("получено %d записей", writes)
	}
}

func TestPublishFailsOnHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(401) }))
	defer server.Close()
	client := Client{HTTP: server.Client()}
	if err := client.Publish(context.Background(), server.URL, Build(nil)); err == nil {
		t.Fatal("ошибка Grafana проигнорирована")
	}
}

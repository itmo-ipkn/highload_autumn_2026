package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"highload-hw1/internal/api"
	"highload-hw1/internal/items"
)

func request(handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(method, path, strings.NewReader(body)))
	return response
}

func TestItemLifecycle(t *testing.T) {
	handler := api.New(items.NewStore(), "test-pod", "test-dc")
	created := request(handler, "POST", "/items", `{"name":"book","value":"first"}`)
	if created.Code != 201 || created.Header().Get("Location") != "/items/1" {
		t.Fatalf("create: %d %s", created.Code, created.Body)
	}
	for _, method := range []string{"GET", "PUT"} {
		response := request(handler, method, "/items/1", `{"name":"updated","value":"second"}`)
		if response.Code != 200 {
			t.Fatalf("%s: %d %s", method, response.Code, response.Body)
		}
	}
	response := request(handler, "GET", "/items/1", "")
	var item items.Item
	if err := json.Unmarshal(response.Body.Bytes(), &item); err != nil {
		t.Fatal(err)
	}
	if item.ID != 1 || item.Name != "updated" || item.Value != "second" {
		t.Fatalf("unexpected stored item: %+v", item)
	}
}

func TestInvalidUpdatePreservesItem(t *testing.T) {
	handler := api.New(items.NewStore(), "test-pod", "test-dc")
	request(handler, "POST", "/items", `{"name":"original","value":"unchanged"}`)
	bodies := []string{
		`{`, `null`, `[]`, `{"name":" "}`, `{"name":"x","extra":true}`,
		`{"name":"x"} {"name":"y"}`, `{"name":"x","value":42}`,
		`{"name":"` + strings.Repeat("x", 201) + `"}`,
		`{"name":"x","value":"` + strings.Repeat("x", 65536) + `"}`,
	}
	for _, body := range bodies {
		response := request(handler, "PUT", "/items/1", body)
		if response.Code != 400 {
			t.Errorf("invalid update returned %d", response.Code)
		}
	}
	response := request(handler, "GET", "/items/1", "")
	var item items.Item
	if err := json.Unmarshal(response.Body.Bytes(), &item); err != nil {
		t.Fatal(err)
	}
	if item.Name != "original" || item.Value != "unchanged" {
		t.Fatalf("invalid update changed item: %+v", item)
	}
}

func TestMissingItemsAndHealth(t *testing.T) {
	handler := api.New(items.NewStore(), "test-pod", "test-dc")
	cases := []struct {
		method, path, body string
		status             int
	}{
		{"GET", "/health", "", 200},
		{"GET", "/items/99", "", 404},
		{"PUT", "/items/99", `{"name":"missing"}`, 404},
		{"GET", "/items/99", "", 404}, // PUT не создаёт отсутствующую запись.
		{"GET", "/items/not-a-number", "", 400},
		{"GET", "/items/0", "", 400},
		{"DELETE", "/items/1", "", 405},
	}
	for _, tc := range cases {
		response := request(handler, tc.method, tc.path, tc.body)
		if response.Code != tc.status {
			t.Errorf("%s %s: got %d, want %d", tc.method, tc.path, response.Code, tc.status)
		}
	}
}

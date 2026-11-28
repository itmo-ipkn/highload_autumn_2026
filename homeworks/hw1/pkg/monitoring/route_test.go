package monitoring

import "testing"

func TestValidMethodAndRoutePath(t *testing.T) {
	t.Parallel()

	if !ValidMethod("GET") || !ValidMethod("POST") || ValidMethod("get") || ValidMethod("") {
		t.Fatal("method validation drifted")
	}
	valid := []string{"/items", "/items/{id}", "/v1/widgets/{id...}", "/a.b/~c"}
	for _, path := range valid {
		if !ValidRoutePath(path) {
			t.Fatalf("path %q was rejected", path)
		}
	}
	invalid := []string{"", "items", "/items/", "//items", "/a/$id", `/v1/"id"`, "/a b", "/items/{"}
	for _, path := range invalid {
		if ValidRoutePath(path) {
			t.Fatalf("path %q was accepted", path)
		}
	}
}

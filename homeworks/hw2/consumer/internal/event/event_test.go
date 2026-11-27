package event

import (
	"encoding/json"
	"testing"
)

func TestIsPoison(t *testing.T) {
	for _, tc := range []struct {
		payload string
		want    bool
	}{
		{` {"poison": true } `, true}, {`{}`, false}, {`{"poison":false}`, false},
		{`{"poison":"true"}`, false}, {`{"Poison":true}`, false}, {`{"nested":{"poison":true}}`, false},
		{`[ {"poison":true} ]`, false}, {`"text"`, false}, {`true`, false}, {`1e10000`, false}, {`null`, false},
	} {
		if got := IsPoison(json.RawMessage(tc.payload)); got != tc.want {
			t.Errorf("IsPoison(%s) = %v, want %v", tc.payload, got, tc.want)
		}
	}
}

package registry

import (
	"encoding/json"
	"testing"
)

func TestRegisteredPayloadSchemas(t *testing.T) {
	defs := Definitions(nil)
	cases := []struct {
		typ, raw string
		valid    bool
	}{
		{"demo.sleep", `{"duration_ms":5,"fail_first":1}`, true},
		{"demo.sleep", `{"duration_ms":-1}`, false},
		{"demo.sleep", `{"arbitrary":1}`, false},
		{"demo.effect", `{"order_id":"123","metadata":{"foo":"bar"}}`, true},
		{"other", `{}`, false},
	}
	for _, tc := range cases {
		err := Validate(defs, tc.typ, json.RawMessage(tc.raw))
		if (err == nil) != tc.valid {
			t.Errorf("%s %s: got %v", tc.typ, tc.raw, err)
		}
	}
}

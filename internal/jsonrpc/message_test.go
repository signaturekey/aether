package jsonrpc

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestEncodeOmitsJSONRPCAndTerminatesLine(t *testing.T) {
	request, err := EncodeRequest(7, "thread/start", map[string]string{"model": "test"})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(request, []byte("jsonrpc")) {
		t.Fatalf("request contains jsonrpc: %s", request)
	}
	if request[len(request)-1] != '\n' || bytes.Count(request, []byte{'\n'}) != 1 {
		t.Fatalf("request is not exactly one line: %q", request)
	}

	notification, err := EncodeNotification("initialized", nil)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(notification, []byte("jsonrpc")) {
		t.Fatalf("notification contains jsonrpc: %s", notification)
	}
}

func TestDecodeKindsAndUnknownFields(t *testing.T) {
	tests := []struct {
		name string
		wire string
		kind Kind
	}{
		{"response", `{"id":1,"result":{"ok":true},"future":"value"}`, Response},
		{"error response", `{"id":2,"error":{"code":42,"message":"no","data":{"why":1}},"future":true}`, Response},
		{"notification", `{"method":"future/event","params":{"x":1},"future":true}`, Notification},
		{"request", `{"id":"server-1","method":"approval","params":{},"future":true}`, Request},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg, err := Decode(json.NewDecoder(strings.NewReader(tt.wire)))
			if err != nil {
				t.Fatal(err)
			}
			if msg.Kind != tt.kind {
				t.Fatalf("kind = %v, want %v", msg.Kind, tt.kind)
			}
		})
	}
}

func TestDecodeLargeMessage(t *testing.T) {
	text := strings.Repeat("x", 256<<10)
	wire, err := json.Marshal(map[string]any{"method": "large", "params": map[string]string{"text": text}})
	if err != nil {
		t.Fatal(err)
	}
	msg, err := Decode(json.NewDecoder(bytes.NewReader(wire)))
	if err != nil {
		t.Fatal(err)
	}
	if msg.Kind != Notification || len(msg.Params) < len(text) {
		t.Fatalf("large notification was truncated: %d", len(msg.Params))
	}
}

func TestDecodeRejectsMalformedStructures(t *testing.T) {
	for _, wire := range []string{
		`{}`,
		`{"id":1}`,
		`{"id":1,"result":{},"error":{"code":1,"message":"both"}}`,
		`{"method":""}`,
		`{"id":null,"method":"x"}`,
		`{"jsonrpc":"2.0","method":"x"}`,
	} {
		if _, err := Decode(json.NewDecoder(strings.NewReader(wire))); err == nil {
			t.Fatalf("Decode(%s) unexpectedly succeeded", wire)
		}
	}
}

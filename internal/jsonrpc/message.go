package jsonrpc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

type Kind uint8

const (
	Invalid Kind = iota
	Response
	Notification
	Request
)

type RPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

type Message struct {
	Kind   Kind
	ID     json.RawMessage
	Method string
	Params json.RawMessage
	Result json.RawMessage
	Error  *RPCError
}

func Decode(dec *json.Decoder) (Message, error) {
	var fields map[string]json.RawMessage
	if err := dec.Decode(&fields); err != nil {
		return Message{}, err
	}
	if len(fields) == 0 {
		return Message{}, errors.New("empty app-server message")
	}

	if _, ok := fields["jsonrpc"]; ok {
		return Message{}, errors.New("unexpected jsonrpc field")
	}

	id, hasID := fields["id"]
	methodRaw, hasMethod := fields["method"]
	params := fields["params"]
	result, hasResult := fields["result"]
	errRaw, hasError := fields["error"]

	var method string
	if hasMethod {
		if err := json.Unmarshal(methodRaw, &method); err != nil || method == "" {
			return Message{}, errors.New("message method must be a non-empty string")
		}
	}

	switch {
	case hasID && !hasMethod:
		if bytes.Equal(bytes.TrimSpace(id), []byte("null")) || len(bytes.TrimSpace(id)) == 0 {
			return Message{}, errors.New("response id must not be null")
		}
		if hasResult == hasError {
			return Message{}, errors.New("response must contain exactly one of result or error")
		}
		msg := Message{Kind: Response, ID: clone(id), Result: clone(result)}
		if hasError {
			var wireErr RPCError
			if err := json.Unmarshal(errRaw, &wireErr); err != nil {
				return Message{}, fmt.Errorf("decode response error: %w", err)
			}
			msg.Error = &wireErr
		}
		return msg, nil
	case !hasID && hasMethod:
		return Message{Kind: Notification, Method: method, Params: clone(params)}, nil
	case hasID && hasMethod:
		if bytes.Equal(bytes.TrimSpace(id), []byte("null")) || len(bytes.TrimSpace(id)) == 0 {
			return Message{}, errors.New("request id must not be null")
		}
		return Message{Kind: Request, ID: clone(id), Method: method, Params: clone(params)}, nil
	default:
		return Message{}, errors.New("message has neither id nor method")
	}
}

func EncodeRequest(id uint64, method string, params any) ([]byte, error) {
	if method == "" {
		return nil, errors.New("method is empty")
	}
	return encode(struct {
		Method string `json:"method"`
		ID     uint64 `json:"id"`
		Params any    `json:"params"`
	}{method, id, normalizeParams(params)})
}

func EncodeNotification(method string, params any) ([]byte, error) {
	if method == "" {
		return nil, errors.New("method is empty")
	}
	return encode(struct {
		Method string `json:"method"`
		Params any    `json:"params"`
	}{method, normalizeParams(params)})
}

func EncodeResult(id json.RawMessage, result any) ([]byte, error) {
	if !json.Valid(id) || bytes.Equal(bytes.TrimSpace(id), []byte("null")) {
		return nil, errors.New("invalid response id")
	}
	return encode(struct {
		ID     json.RawMessage `json:"id"`
		Result any             `json:"result"`
	}{id, normalizeResult(result)})
}

func EncodeError(id json.RawMessage, rpcErr RPCError) ([]byte, error) {
	if !json.Valid(id) || bytes.Equal(bytes.TrimSpace(id), []byte("null")) {
		return nil, errors.New("invalid response id")
	}
	return encode(struct {
		ID    json.RawMessage `json:"id"`
		Error RPCError        `json:"error"`
	}{id, rpcErr})
}

func encode(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func normalizeParams(v any) any {
	if v == nil {
		return struct{}{}
	}
	return v
}

func normalizeResult(v any) any {
	if v == nil {
		return struct{}{}
	}
	return v
}

func clone(v []byte) []byte {
	return append([]byte(nil), v...)
}

func NewDecoder(r io.Reader) *json.Decoder {
	return json.NewDecoder(r)
}

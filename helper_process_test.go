package aether

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestHelperProcess(t *testing.T) {
	if os.Getenv("AETHER_HELPER_PROCESS") != "1" {
		return
	}
	runHelperServer(os.Getenv("AETHER_HELPER_SCENARIO"))
	os.Exit(0)
}

type helperServer struct {
	decoder *json.Decoder
	encoder *json.Encoder
	encMu   sync.Mutex

	scenario      string
	nextThread    int
	nextTurn      int
	turns         map[string]string
	serverReply   json.RawMessage
	serverError   *wireTestError
	awaitResponse json.RawMessage
}

type wireTestError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func runHelperServer(scenario string) {
	s := &helperServer{
		decoder:  json.NewDecoder(os.Stdin),
		encoder:  json.NewEncoder(os.Stdout),
		scenario: scenario,
		turns:    make(map[string]string),
	}
	for {
		var message map[string]json.RawMessage
		if err := s.decoder.Decode(&message); err != nil {
			return
		}
		methodRaw, hasMethod := message["method"]
		id, hasID := message["id"]
		if !hasMethod && hasID {
			s.captureServerResponse(message)
			continue
		}
		var method string
		_ = json.Unmarshal(methodRaw, &method)
		if !hasID {
			continue
		}
		s.handleRequest(id, method, message["params"])
	}
}

func (s *helperServer) handleRequest(id json.RawMessage, method string, params json.RawMessage) {
	switch method {
	case "initialize":
		switch s.scenario {
		case "handshake_error":
			s.sendError(id, 1001, "handshake rejected")
		case "handshake_timeout":
			return
		default:
			s.sendResult(id, map[string]any{"userAgent": "aether-test", "platformFamily": "unix", "platformOs": "test"})
		}
	case "echo", "mcpServerStatus/list":
		var value any
		_ = json.Unmarshal(params, &value)
		s.sendResult(id, value)
	case "echoDelayed":
		var value struct {
			Value   int `json:"value"`
			DelayMS int `json:"delayMs"`
		}
		_ = json.Unmarshal(params, &value)
		go func() {
			time.Sleep(time.Duration(value.DelayMS) * time.Millisecond)
			s.sendResult(id, map[string]int{"value": value.Value})
		}()
	case "fail":
		s.sendErrorWithData(id, 123, "expected failure", json.RawMessage(`{"kind":"test"}`))
	case "block":
		return
	case "closeStdout":
		s.sendResult(id, map[string]any{})
		_ = os.Stdout.Close()
		select {}
	case "die":
		_, _ = fmt.Fprint(os.Stderr, "helper process exploded")
		os.Exit(7)
	case "thread/start":
		s.nextThread++
		threadID := "thr_" + strconv.Itoa(s.nextThread)
		s.sendResult(id, map[string]any{"thread": map[string]any{"id": threadID, "ephemeral": false}})
	case "turn/start":
		s.startTurn(id, params)
	case "turn/interrupt":
		s.interruptTurn(id, params)
	case "triggerServerRequest":
		var request struct {
			Method string `json:"method"`
		}
		_ = json.Unmarshal(params, &request)
		s.sendResult(id, map[string]bool{"sent": true})
		s.send(map[string]any{"id": "server-1", "method": request.Method, "params": map[string]string{"value": "hello"}})
	case "triggerServerRequests":
		var request struct {
			Method string `json:"method"`
			Count  int    `json:"count"`
		}
		_ = json.Unmarshal(params, &request)
		for i := range request.Count {
			s.send(map[string]any{"id": "server-" + strconv.Itoa(i+1), "method": request.Method, "params": map[string]string{"value": "hello"}})
		}
		s.sendResult(id, map[string]bool{"sent": true})
	case "awaitServerResponse":
		if s.serverReply != nil || s.serverError != nil {
			s.sendCaptured(id)
		} else {
			s.awaitResponse = append(json.RawMessage(nil), id...)
		}
	default:
		s.sendResult(id, map[string]any{})
	}
}

func (s *helperServer) startTurn(id json.RawMessage, params json.RawMessage) {
	var request struct {
		ThreadID string `json:"threadId"`
		Input    []struct {
			Text string `json:"text"`
		} `json:"input"`
		OutputSchema json.RawMessage `json:"outputSchema"`
	}
	_ = json.Unmarshal(params, &request)
	s.nextTurn++
	turnID := "turn_" + strconv.Itoa(s.nextTurn)
	s.turns[request.ThreadID] = turnID
	if s.scenario == "late_item_before_start_response" && s.nextTurn == 2 {
		s.send(map[string]any{"method": "item/completed", "params": map[string]any{
			"threadId": request.ThreadID, "turnId": "turn_1",
			"item": map[string]any{"id": "late", "type": "agentMessage", "text": "late item"},
		}})
	}
	if s.scenario == "delayed_start" {
		time.Sleep(80 * time.Millisecond)
	}
	s.sendResult(id, map[string]any{"turn": map[string]any{"id": turnID, "status": "inProgress", "items": []any{}, "error": nil}})
	holds := s.scenario == "hold_turn" || s.scenario == "delayed_start" ||
		s.scenario == "interrupt_timeout" || s.scenario == "late_interrupt_completion" ||
		s.scenario == "interrupt_die" || s.scenario == "completion_before_interrupt_error"
	if holds && len(request.Input) != 0 && request.Input[0].Text == "hold" {
		return
	}
	if s.scenario == "failed_turn" || s.scenario == "failed_misalignment" {
		failure := map[string]any{"message": "model failed"}
		if s.scenario == "failed_misalignment" {
			failure["misalignment"] = map[string]any{"reason": "policy blocked", "continuation": "resume later"}
		}
		s.send(map[string]any{"method": "turn/completed", "params": map[string]any{
			"threadId": request.ThreadID,
			"turn":     map[string]any{"id": turnID, "status": "failed", "items": []any{}, "error": failure},
		}})
		return
	}
	s.send(map[string]any{"method": "future/notification", "params": map[string]any{"ignored": true}})
	s.send(map[string]any{"method": "item/completed", "params": map[string]any{
		"threadId": request.ThreadID, "turnId": turnID,
		"item": map[string]any{"id": "unknown", "type": "futureItem", "payload": map[string]bool{"preserved": true}},
	}})
	s.send(map[string]any{"method": "item/completed", "params": map[string]any{
		"threadId": request.ThreadID, "turnId": turnID,
		"item": map[string]any{"id": "commentary", "type": "agentMessage", "phase": "commentary", "text": "working"},
	}})
	text := "done for " + request.ThreadID
	if len(request.OutputSchema) != 0 {
		if s.scenario == "invalid_output" {
			text = "not json"
		} else {
			text = `{"answer":"ok"}`
		}
	}
	s.send(map[string]any{"method": "item/completed", "params": map[string]any{
		"threadId": request.ThreadID, "turnId": turnID,
		"item": map[string]any{"id": "final", "type": "agentMessage", "phase": "final_answer", "text": text},
	}})
	s.send(map[string]any{"method": "turn/completed", "params": map[string]any{
		"threadId": request.ThreadID,
		"turn":     map[string]any{"id": turnID, "status": "completed", "items": []any{}, "error": nil},
	}})
}

func (s *helperServer) interruptTurn(id json.RawMessage, params json.RawMessage) {
	var request struct {
		ThreadID string `json:"threadId"`
		TurnID   string `json:"turnId"`
	}
	_ = json.Unmarshal(params, &request)
	if s.scenario == "interrupt_die" {
		_, _ = fmt.Fprint(os.Stderr, "died during interrupt")
		os.Exit(9)
	}
	if s.scenario == "completion_before_interrupt_error" {
		s.send(map[string]any{"method": "turn/completed", "params": map[string]any{
			"threadId": request.ThreadID,
			"turn":     map[string]any{"id": request.TurnID, "status": "completed", "items": []any{}, "error": nil},
		}})
		s.sendError(id, -32000, "no active turn to interrupt")
		return
	}
	s.sendResult(id, map[string]any{})
	if s.scenario == "interrupt_timeout" {
		return
	}
	if s.scenario == "late_interrupt_completion" {
		go func() {
			time.Sleep(80 * time.Millisecond)
			s.send(map[string]any{"method": "turn/completed", "params": map[string]any{
				"threadId": request.ThreadID,
				"turn":     map[string]any{"id": request.TurnID, "status": "interrupted", "items": []any{}, "error": nil},
			}})
		}()
		return
	}
	s.send(map[string]any{"method": "turn/completed", "params": map[string]any{
		"threadId": request.ThreadID,
		"turn":     map[string]any{"id": request.TurnID, "status": "interrupted", "items": []any{}, "error": nil},
	}})
}

func (s *helperServer) captureServerResponse(message map[string]json.RawMessage) {
	s.serverReply = append(json.RawMessage(nil), message["result"]...)
	if raw := message["error"]; len(raw) != 0 {
		var wireErr wireTestError
		_ = json.Unmarshal(raw, &wireErr)
		s.serverError = &wireErr
	}
	if s.awaitResponse != nil {
		id := s.awaitResponse
		s.awaitResponse = nil
		s.sendCaptured(id)
	}
}

func (s *helperServer) sendCaptured(id json.RawMessage) {
	if s.serverError != nil {
		s.sendResult(id, map[string]any{"error": s.serverError})
		return
	}
	var result any
	_ = json.Unmarshal(s.serverReply, &result)
	s.sendResult(id, map[string]any{"result": result})
}

func (s *helperServer) sendResult(id json.RawMessage, result any) {
	s.send(map[string]any{"id": id, "result": result})
}

func (s *helperServer) sendError(id json.RawMessage, code int, message string) {
	s.send(map[string]any{"id": id, "error": map[string]any{"code": code, "message": message}})
}

func (s *helperServer) sendErrorWithData(id json.RawMessage, code int, message string, data json.RawMessage) {
	s.send(map[string]any{"id": id, "error": map[string]any{"code": code, "message": message, "data": data}})
}

func (s *helperServer) send(message any) {
	s.encMu.Lock()
	defer s.encMu.Unlock()
	if err := s.encoder.Encode(message); err != nil {
		os.Exit(2)
	}
}

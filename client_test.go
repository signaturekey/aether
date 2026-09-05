package aether

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

type rejectingWriter struct{}

func (rejectingWriter) Write([]byte) (int, error) { return 0, errors.New("reject stderr") }

func helperOptions(scenario string) Options {
	return Options{
		Command: []string{os.Args[0], "-test.run=TestHelperProcess", "--"},
		Env: map[string]string{
			"AETHER_HELPER_PROCESS":  "1",
			"AETHER_HELPER_SCENARIO": scenario,
		},
		HandshakeTimeout: 500 * time.Millisecond,
		InterruptTimeout: 500 * time.Millisecond,
		CloseTimeout:     time.Second,
	}
}

func startHelper(t *testing.T, scenario string, mutate func(*Options)) *Client {
	t.Helper()
	opts := helperOptions(scenario)
	if mutate != nil {
		mutate(&opts)
	}
	client, err := Start(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return client
}

func TestStartMissingExecutable(t *testing.T) {
	_, err := Start(context.Background(), Options{Command: []string{"definitely-not-a-real-aether-test-command"}})
	var startupErr *StartupError
	if !errors.As(err, &startupErr) {
		t.Fatalf("error = %v, want StartupError", err)
	}
}

func TestHandshakeFailureAndTimeout(t *testing.T) {
	t.Run("rpc error", func(t *testing.T) {
		_, err := Start(context.Background(), helperOptions("handshake_error"))
		var rpcErr *RPCError
		if !errors.As(err, &rpcErr) || rpcErr.Code != 1001 {
			t.Fatalf("error = %v, want RPCError 1001", err)
		}
	})
	t.Run("timeout", func(t *testing.T) {
		opts := helperOptions("handshake_timeout")
		opts.HandshakeTimeout = 30 * time.Millisecond
		_, err := Start(context.Background(), opts)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %v, want deadline exceeded", err)
		}
	})
}

func TestRawCallAndRPCError(t *testing.T) {
	client := startHelper(t, "", nil)
	var result map[string]any
	if err := client.Call(context.Background(), "echo", map[string]any{"answer": 42.0}, &result); err != nil {
		t.Fatal(err)
	}
	if result["answer"] != 42.0 {
		t.Fatalf("result = %#v", result)
	}

	err := client.Call(context.Background(), "fail", nil, nil)
	var rpcErr *RPCError
	if !errors.As(err, &rpcErr) || rpcErr.Code != 123 || string(rpcErr.Data) != `{"kind":"test"}` {
		t.Fatalf("error = %#v", err)
	}
}

func TestConcurrentCallsOutOfOrder(t *testing.T) {
	client := startHelper(t, "", nil)
	const calls = 40
	var wg sync.WaitGroup
	errs := make(chan error, calls)
	for i := 0; i < calls; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var result struct {
				Value int `json:"value"`
			}
			err := client.Call(context.Background(), "echoDelayed", map[string]int{
				"value": i, "delayMs": (calls - i) % 11,
			}, &result)
			if err == nil && result.Value != i {
				err = errors.New("response correlated to wrong request")
			}
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestCancelledCallIgnoresLateResponse(t *testing.T) {
	client := startHelper(t, "", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := client.Call(ctx, "echoDelayed", map[string]int{"value": 1, "delayMs": 50}, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v", err)
	}
	time.Sleep(60 * time.Millisecond)
	var result map[string]string
	if err := client.Call(context.Background(), "echo", map[string]string{"ok": "yes"}, &result); err != nil {
		t.Fatal(err)
	}
}

func TestProcessExitFailsPendingCalls(t *testing.T) {
	client := startHelper(t, "", nil)
	errs := make(chan error, 3)
	for range 2 {
		go func() { errs <- client.Call(context.Background(), "block", nil, nil) }()
	}
	time.Sleep(20 * time.Millisecond)
	go func() { errs <- client.Call(context.Background(), "die", nil, nil) }()
	for range 3 {
		err := <-errs
		if !errors.Is(err, ErrProcessExited) {
			t.Fatalf("error = %v, want process exit", err)
		}
		var processErr *ProcessError
		if !errors.As(err, &processErr) || processErr.ExitCode != 7 || !strings.Contains(processErr.Stderr, "exploded") {
			t.Fatalf("error = %#v", err)
		}
	}
}

func TestStdoutEOFFailsPendingCallsAndStopsProcess(t *testing.T) {
	client := startHelper(t, "", nil)
	done := make(chan error, 1)
	go func() {
		done <- client.Call(context.Background(), "block", nil, nil)
	}()
	time.Sleep(20 * time.Millisecond)
	_ = client.Call(context.Background(), "closeStdout", nil, nil)
	select {
	case err := <-done:
		if !errors.Is(err, ErrProcessExited) {
			t.Fatalf("error = %v, want process exit", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending call did not unblock after stdout EOF")
	}
}

func TestFailingStderrSinkDoesNotStopDrain(t *testing.T) {
	client := startHelper(t, "", func(opts *Options) {
		opts.Stderr = rejectingWriter{}
	})
	err := client.Call(context.Background(), "die", nil, nil)
	var processErr *ProcessError
	if !errors.As(err, &processErr) || !strings.Contains(processErr.Stderr, "exploded") {
		t.Fatalf("error = %#v", err)
	}
}

func TestCloseIsIdempotentAndCallsAfterCloseFail(t *testing.T) {
	client := startHelper(t, "", nil)
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.Call(context.Background(), "echo", nil, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("error = %v, want ErrClosed", err)
	}
}

func TestConcurrentCloseIsIdempotent(t *testing.T) {
	client := startHelper(t, "", nil)
	const closers = 32
	errs := make(chan error, closers)
	var wg sync.WaitGroup
	for range closers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- client.Close()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := client.Call(context.Background(), "echo", nil, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("error = %v, want ErrClosed", err)
	}
}

func TestRawMCPCallUsesConsumerOwnedType(t *testing.T) {
	client := startHelper(t, "", nil)
	type status struct {
		Name string `json:"name"`
	}
	var response status
	if err := client.Call(context.Background(), "mcpServerStatus/list", status{Name: "demo"}, &response); err != nil {
		t.Fatal(err)
	}
	if response.Name != "demo" {
		t.Fatalf("response = %#v", response)
	}
}

func TestServerRequestHandlers(t *testing.T) {
	tests := []struct {
		name    string
		method  string
		handler RequestHandler
		code    int
		value   string
	}{
		{"success", "demo/success", func(_ context.Context, req ServerRequest) (any, error) {
			var params map[string]string
			_ = json.Unmarshal(req.Params, &params)
			return map[string]string{"value": params["value"]}, nil
		}, 0, "hello"},
		{"typed error", "demo/error", func(context.Context, ServerRequest) (any, error) {
			return nil, &RPCError{Code: 4321, Message: "declined"}
		}, 4321, ""},
		{"panic", "demo/panic", func(context.Context, ServerRequest) (any, error) {
			panic("boom")
		}, -32603, ""},
		{"unserializable result", "demo/unserializable-result", func(context.Context, ServerRequest) (any, error) {
			return make(chan struct{}), nil
		}, -32603, ""},
		{"unserializable error data", "demo/unserializable-error", func(context.Context, ServerRequest) (any, error) {
			return nil, &RPCError{Code: 4321, Message: "declined", Data: json.RawMessage(`{`)}
		}, -32603, ""},
		{"unknown", "demo/unknown", nil, -32601, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := startHelper(t, "", func(opts *Options) {
				if tt.handler != nil {
					opts.Handlers = map[string]RequestHandler{tt.method: tt.handler}
				}
			})
			if err := client.Call(context.Background(), "triggerServerRequest", map[string]string{"method": tt.method}, nil); err != nil {
				t.Fatal(err)
			}
			var response struct {
				Result map[string]string `json:"result"`
				Error  *wireTestError    `json:"error"`
			}
			if err := client.Call(context.Background(), "awaitServerResponse", nil, &response); err != nil {
				t.Fatal(err)
			}
			if tt.code != 0 {
				if response.Error == nil || response.Error.Code != tt.code {
					t.Fatalf("response = %#v, want code %d", response, tt.code)
				}
			} else if response.Result["value"] != tt.value {
				t.Fatalf("response = %#v", response)
			}
		})
	}
}

func TestSlowHandlerDoesNotBlockDecoder(t *testing.T) {
	client := startHelper(t, "", func(opts *Options) {
		opts.Handlers = map[string]RequestHandler{
			"demo/slow": func(context.Context, ServerRequest) (any, error) {
				time.Sleep(100 * time.Millisecond)
				return map[string]bool{"ok": true}, nil
			},
		}
	})
	if err := client.Call(context.Background(), "triggerServerRequest", map[string]string{"method": "demo/slow"}, nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := client.Call(ctx, "echo", map[string]bool{"ok": true}, nil); err != nil {
		t.Fatalf("decoder blocked by handler: %v", err)
	}
}

func TestUnknownServerRequestDoesNotWaitForBusyHandlers(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	started := make(chan struct{}, maxServerRequestHandlers)
	client := startHelper(t, "", func(opts *Options) {
		opts.Handlers = map[string]RequestHandler{
			"demo/block": func(context.Context, ServerRequest) (any, error) {
				started <- struct{}{}
				<-release
				return nil, nil
			},
		}
	})
	if err := client.Call(context.Background(), "triggerServerRequests", map[string]any{
		"method": "demo/block", "count": maxServerRequestHandlers,
	}, nil); err != nil {
		t.Fatal(err)
	}
	for range maxServerRequestHandlers {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("server request handlers did not start")
		}
	}
	if err := client.Call(context.Background(), "triggerServerRequest", map[string]string{"method": "demo/unknown"}, nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var response struct {
		Error *wireTestError `json:"error"`
	}
	if err := client.Call(ctx, "awaitServerResponse", nil, &response); err != nil {
		t.Fatalf("unknown request waited for a handler: %v", err)
	}
	if response.Error == nil || response.Error.Code != -32601 {
		t.Fatalf("response = %#v", response)
	}
}

func TestServerRequestQueueIsBounded(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	client := startHelper(t, "", func(opts *Options) {
		opts.Handlers = map[string]RequestHandler{
			"demo/block": func(context.Context, ServerRequest) (any, error) {
				<-release
				return map[string]bool{"ok": true}, nil
			},
		}
	})
	if err := client.Call(context.Background(), "triggerServerRequests", map[string]any{
		"method": "demo/block", "count": maxServerRequestHandlers*2 + 1,
	}, nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var response struct {
		Error *wireTestError `json:"error"`
	}
	if err := client.Call(ctx, "awaitServerResponse", nil, &response); err != nil {
		t.Fatal(err)
	}
	if response.Error == nil || response.Error.Code != -32000 {
		t.Fatalf("response = %#v", response)
	}
}

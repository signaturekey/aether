package aether

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func textTurn(text string) TurnRequest {
	return TurnRequest{Input: []Input{{Type: "text", Text: text}}}
}

func TestPlainAndStructuredTurns(t *testing.T) {
	client := startHelper(t, "", nil)
	thread, err := client.StartThread(context.Background(), ThreadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := thread.Run(context.Background(), textTurn("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != TurnStatusCompleted || result.FinalText != "done for "+thread.ID() || len(result.Items) != 3 {
		t.Fatalf("result = %#v", result)
	}
	if !strings.Contains(string(result.Items[0]), "futureItem") {
		t.Fatalf("unknown item was not preserved: %s", result.Items[0])
	}

	structured := textTurn("json")
	structured.OutputSchema = json.RawMessage(`{"type":"object","properties":{"answer":{"type":"string"}}}`)
	result, err = thread.Run(context.Background(), structured)
	if err != nil {
		t.Fatal(err)
	}
	if string(result.JSON) != `{"answer":"ok"}` {
		t.Fatalf("JSON = %s", result.JSON)
	}
}

func TestInvalidStructuredOutput(t *testing.T) {
	client := startHelper(t, "invalid_output", nil)
	thread, err := client.StartThread(context.Background(), ThreadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	req := textTurn("json")
	req.OutputSchema = json.RawMessage(`{"type":"object"}`)
	result, err := thread.Run(context.Background(), req)
	if !errors.Is(err, ErrInvalidOutput) || result.FinalText != "not json" {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
}

func TestFailedTurnPreservesResult(t *testing.T) {
	client := startHelper(t, "failed_turn", nil)
	thread, err := client.StartThread(context.Background(), ThreadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := thread.Run(context.Background(), textTurn("fail"))
	var turnErr *TurnError
	if !errors.As(err, &turnErr) || result.Status != TurnStatusFailed || result.Failure == nil || result.Failure.Message != "model failed" {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
}

func TestParallelThreadsDoNotMixEvents(t *testing.T) {
	client := startHelper(t, "", nil)
	const count = 4
	threads := make([]*Thread, count)
	for i := range threads {
		var err error
		threads[i], err = client.StartThread(context.Background(), ThreadOptions{})
		if err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	errs := make(chan error, count)
	for _, thread := range threads {
		wg.Add(1)
		go func(thread *Thread) {
			defer wg.Done()
			result, err := thread.Run(context.Background(), textTurn("parallel"))
			if err == nil && result.FinalText != "done for "+thread.ID() {
				err = errors.New("turn received another thread's events")
			}
			errs <- err
		}(thread)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestSameThreadRejectsConcurrentRunAndCancellationInterrupts(t *testing.T) {
	client := startHelper(t, "hold_turn", nil)
	thread, err := client.StartThread(context.Background(), ThreadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := thread.Run(ctx, textTurn("hold"))
		done <- err
	}()
	time.Sleep(30 * time.Millisecond)
	if _, err := thread.Run(context.Background(), textTurn("second")); !errors.Is(err, ErrTurnActive) {
		t.Fatalf("error = %v, want ErrTurnActive", err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled turn did not finish")
	}

	other, err := client.StartThread(context.Background(), ThreadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := other.Run(context.Background(), textTurn("other")); err != nil || result.Status != TurnStatusCompleted {
		t.Fatalf("unrelated thread failed after cancellation: result=%#v error=%v", result, err)
	}
}

func TestCancellationBeforeRunWritesNothing(t *testing.T) {
	client := startHelper(t, "", nil)
	thread, err := client.StartThread(context.Background(), ThreadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := thread.Run(ctx, textTurn("never sent")); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled", err)
	}
	if result, err := thread.Run(context.Background(), textTurn("still usable")); err != nil || result.Status != TurnStatusCompleted {
		t.Fatalf("thread unusable after pre-write cancellation: result=%#v error=%v", result, err)
	}
}

func TestCancellationAfterWriteRecoversTurnIDAndInterrupts(t *testing.T) {
	client := startHelper(t, "delayed_start", nil)
	thread, err := client.StartThread(context.Background(), ThreadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct {
		result TurnResult
		err    error
	}, 1)
	go func() {
		result, err := thread.Run(ctx, textTurn("hold"))
		done <- struct {
			result TurnResult
			err    error
		}{result, err}
	}()
	time.Sleep(15 * time.Millisecond)
	cancel()
	select {
	case got := <-done:
		if !errors.Is(got.err, context.Canceled) || got.result.Status != TurnStatusInterrupted || got.result.TurnID == "" {
			t.Fatalf("result=%#v error=%v", got.result, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("turn/start recovery did not finish")
	}
}

func TestInterruptTimeoutIsBounded(t *testing.T) {
	client := startHelper(t, "interrupt_timeout", func(opts *Options) {
		opts.InterruptTimeout = 30 * time.Millisecond
	})
	thread, err := client.StartThread(context.Background(), ThreadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := thread.Run(ctx, textTurn("hold"))
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "timed out") {
			t.Fatalf("error = %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("interrupt timeout was not bounded")
	}
	if _, err := thread.Run(context.Background(), textTurn("second")); !errors.Is(err, ErrThreadStateUnknown) {
		t.Fatalf("error = %v, want ErrThreadStateUnknown", err)
	}
}

func TestLateStartAfterCancellationMakesThreadUnavailable(t *testing.T) {
	client := startHelper(t, "delayed_start", func(opts *Options) {
		opts.InterruptTimeout = 30 * time.Millisecond
	})
	thread, err := client.StartThread(context.Background(), ThreadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := thread.Run(ctx, textTurn("hold"))
		done <- err
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrThreadStateUnknown) {
			t.Fatalf("error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("turn/start cancellation recovery did not time out")
	}
	time.Sleep(100 * time.Millisecond)
	if _, err := thread.Run(context.Background(), textTurn("second")); !errors.Is(err, ErrThreadStateUnknown) {
		t.Fatalf("error = %v, want ErrThreadStateUnknown", err)
	}
}

func TestLateCompletionCannotFinishAnotherTurn(t *testing.T) {
	state := newTurnState("thr_1", false)
	if err := state.setTurnID("turn_new"); err != nil {
		t.Fatal(err)
	}
	state.complete("turn_old", TurnStatusCompleted, nil, nil)
	select {
	case completion := <-state.done:
		t.Fatalf("unexpected completion: %#v", completion)
	default:
	}
	state.complete("turn_new", TurnStatusCompleted, nil, nil)
	select {
	case completion := <-state.done:
		if completion.result.TurnID != "turn_new" || completion.result.Status != TurnStatusCompleted {
			t.Fatalf("completion = %#v", completion)
		}
	case <-time.After(time.Second):
		t.Fatal("expected completion")
	}
}

func TestLateInterruptedCompletionDoesNotReopenThread(t *testing.T) {
	client := startHelper(t, "late_interrupt_completion", func(opts *Options) {
		opts.InterruptTimeout = 30 * time.Millisecond
	})
	thread, err := client.StartThread(context.Background(), ThreadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := thread.Run(ctx, textTurn("hold"))
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrThreadStateUnknown) {
			t.Fatalf("error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("interrupt timeout was not bounded")
	}
	time.Sleep(100 * time.Millisecond)
	if _, err := thread.Run(context.Background(), textTurn("second")); !errors.Is(err, ErrThreadStateUnknown) {
		t.Fatalf("error = %v, want ErrThreadStateUnknown", err)
	}
}

func TestCompletionItemsAreAuthoritative(t *testing.T) {
	state := newTurnState("thr_1", false)
	if err := state.setTurnID("turn_1"); err != nil {
		t.Fatal(err)
	}
	state.addItem("turn_1", json.RawMessage(`{"id":"partial"}`))
	state.complete("turn_1", TurnStatusCompleted, []json.RawMessage{json.RawMessage(`{"id":"final"}`)}, nil)
	completion := <-state.done
	if len(completion.result.Items) != 1 || string(completion.result.Items[0]) != `{"id":"final"}` {
		t.Fatalf("items = %s", completion.result.Items)
	}
}

func TestClientCloseFailsActiveTurn(t *testing.T) {
	client := startHelper(t, "hold_turn", nil)
	thread, err := client.StartThread(context.Background(), ThreadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := thread.Run(context.Background(), textTurn("hold"))
		done <- err
	}()
	time.Sleep(30 * time.Millisecond)
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("error = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("active turn did not unblock on Close")
	}
}

func TestProcessDeathDuringInterruptIsPreserved(t *testing.T) {
	client := startHelper(t, "interrupt_die", nil)
	thread, err := client.StartThread(context.Background(), ThreadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := thread.Run(ctx, textTurn("hold"))
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrProcessExited) {
			t.Fatalf("error = %v, want cancellation joined with process exit", err)
		}
	case <-time.After(time.Second):
		t.Fatal("process death during interrupt did not unblock Run")
	}
}

func TestCancelOneOfFourThreads(t *testing.T) {
	client := startHelper(t, "hold_turn", nil)
	threads := make([]*Thread, 4)
	for i := range threads {
		var err error
		threads[i], err = client.StartThread(context.Background(), ThreadOptions{})
		if err != nil {
			t.Fatal(err)
		}
	}
	cancelCtx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, 4)
	go func() {
		_, err := threads[0].Run(cancelCtx, textTurn("hold"))
		errs <- err
	}()
	for i := 1; i < 4; i++ {
		go func(i int) {
			_, err := threads[i].Run(context.Background(), textTurn("complete"))
			errs <- err
		}(i)
	}
	time.Sleep(25 * time.Millisecond)
	cancel()
	cancelled := 0
	for range 4 {
		err := <-errs
		if errors.Is(err, context.Canceled) {
			cancelled++
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if cancelled != 1 {
		t.Fatalf("cancelled turns = %d, want 1", cancelled)
	}
}

func TestThreadCloseIsLocalAndIdempotent(t *testing.T) {
	client := startHelper(t, "", nil)
	thread, err := client.StartThread(context.Background(), ThreadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := thread.Close(); err != nil {
		t.Fatal(err)
	}
	if err := thread.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := thread.Run(context.Background(), textTurn("closed")); !errors.Is(err, ErrClosed) {
		t.Fatalf("error = %v, want ErrClosed", err)
	}
}

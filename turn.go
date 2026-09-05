package aether

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

type Input struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
	URL  string `json:"url,omitempty"`
	Path string `json:"path,omitempty"`
}

type TurnRequest struct {
	Input          []Input         `json:"input"`
	Model          string          `json:"model,omitempty"`
	Effort         string          `json:"effort,omitempty"`
	CWD            string          `json:"cwd,omitempty"`
	ApprovalPolicy string          `json:"approvalPolicy,omitempty"`
	SandboxPolicy  json.RawMessage `json:"sandboxPolicy,omitempty"`
	OutputSchema   json.RawMessage `json:"outputSchema,omitempty"`
}

type TurnStatus string

const (
	TurnStatusInProgress  TurnStatus = "inProgress"
	TurnStatusCompleted   TurnStatus = "completed"
	TurnStatusInterrupted TurnStatus = "interrupted"
	TurnStatusFailed      TurnStatus = "failed"
)

type TurnFailure struct {
	Message           string          `json:"message"`
	CodexErrorInfo    json.RawMessage `json:"codexErrorInfo,omitempty"`
	AdditionalDetails json.RawMessage `json:"additionalDetails,omitempty"`
	Raw               json.RawMessage `json:"-"`
}

func (f *TurnFailure) UnmarshalJSON(data []byte) error {
	type turnFailure TurnFailure
	var decoded turnFailure
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*f = TurnFailure(decoded)
	f.Raw = append(json.RawMessage(nil), data...)
	return nil
}

type TurnResult struct {
	ThreadID  string
	TurnID    string
	Status    TurnStatus
	FinalText string
	JSON      json.RawMessage
	Items     []json.RawMessage
	Failure   *TurnFailure
}

type turnState struct {
	mu sync.Mutex

	threadID   string
	turnID     string
	status     TurnStatus
	items      []json.RawMessage
	failure    *TurnFailure
	structured bool

	pendingEvents []pendingTurnEvent
	done          chan turnCompletion
	finished      bool
}

type pendingTurnEvent struct {
	turnID    string
	item      json.RawMessage
	status    TurnStatus
	items     []json.RawMessage
	failure   *TurnFailure
	completed bool
}

type turnCompletion struct {
	result TurnResult
	err    error
}

func newTurnState(threadID string, structured bool) *turnState {
	return &turnState{
		threadID: threadID, status: TurnStatusInProgress, structured: structured,
		done: make(chan turnCompletion, 1),
	}
}

func (t *Thread) Run(ctx context.Context, req TurnRequest) (TurnResult, error) {
	if ctx == nil {
		return TurnResult{}, errors.New("run context is nil")
	}
	if err := ctx.Err(); err != nil {
		return TurnResult{}, err
	}
	if len(req.Input) == 0 {
		return TurnResult{}, errors.New("turn input is empty")
	}
	if err := t.beginRun(); err != nil {
		return TurnResult{}, err
	}
	defer t.endRun()

	state := newTurnState(t.id, len(req.OutputSchema) != 0)
	if err := t.client.registerTurn(t.id, state); err != nil {
		return TurnResult{}, err
	}
	defer t.client.unregisterTurn(t.id, state)

	params := struct {
		ThreadID string `json:"threadId"`
		TurnRequest
	}{ThreadID: t.id, TurnRequest: req}
	var response struct {
		Turn struct {
			ID     string     `json:"id"`
			Status TurnStatus `json:"status"`
		} `json:"turn"`
	}

	startCtx, cancelStart := context.WithCancel(context.Background())
	defer cancelStart()
	startDone := make(chan error, 1)
	go func() {
		startDone <- t.client.Call(startCtx, "turn/start", params, &response)
	}()

	select {
	case err := <-startDone:
		if err != nil {
			return state.snapshot(), err
		}
	case <-ctx.Done():
		return t.recoverCancelledStart(ctx, cancelStart, startDone, &response, state)
	case <-t.client.terminal:
		return state.snapshot(), t.client.getTerminalError()
	}

	if response.Turn.ID == "" {
		return state.snapshot(), errors.New("turn/start response omitted turn id")
	}
	if err := state.setTurnID(response.Turn.ID); err != nil {
		return state.snapshot(), err
	}
	return t.waitForTurn(ctx, state)
}

func (t *Thread) recoverCancelledStart(
	callerCtx context.Context,
	cancelStart context.CancelFunc,
	startDone <-chan error,
	response *struct {
		Turn struct {
			ID     string     `json:"id"`
			Status TurnStatus `json:"status"`
		} `json:"turn"`
	},
	state *turnState,
) (TurnResult, error) {
	timer := time.NewTimer(t.client.opts.InterruptTimeout)
	defer timer.Stop()
	select {
	case err := <-startDone:
		if err != nil {
			return state.snapshot(), &TurnError{Result: state.snapshot(), Cause: errors.Join(callerCtx.Err(), err)}
		}
		if response.Turn.ID == "" {
			return state.snapshot(), &TurnError{Result: state.snapshot(), Cause: errors.Join(callerCtx.Err(), errors.New("turn/start response omitted turn id"))}
		}
		if err := state.setTurnID(response.Turn.ID); err != nil {
			return state.snapshot(), &TurnError{Result: state.snapshot(), Cause: errors.Join(callerCtx.Err(), err)}
		}
		return t.interruptAfterCancellation(callerCtx.Err(), state)
	case <-timer.C:
		t.markStateUnknown()
		cancelStart()
		return state.snapshot(), &TurnError{Result: state.snapshot(), Cause: errors.Join(callerCtx.Err(), ErrThreadStateUnknown, errors.New("timed out recovering turn/start after cancellation"))}
	case completion := <-state.done:
		return completion.result, &TurnError{Result: completion.result, Cause: callerCtx.Err()}
	}
}

func (t *Thread) waitForTurn(ctx context.Context, state *turnState) (TurnResult, error) {
	select {
	case completion := <-state.done:
		return finishTurn(completion, state.hasOutputSchema())
	case <-ctx.Done():
		return t.interruptAfterCancellation(ctx.Err(), state)
	case <-t.client.terminal:
		result := state.snapshot()
		return result, &TurnError{Result: result, Cause: t.client.getTerminalError()}
	}
}

func (t *Thread) interruptAfterCancellation(callerErr error, state *turnState) (TurnResult, error) {
	turnID := state.getTurnID()
	if turnID == "" {
		result := state.snapshot()
		return result, &TurnError{Result: result, Cause: callerErr}
	}
	interruptCtx, cancel := context.WithTimeout(context.Background(), t.client.opts.InterruptTimeout)
	defer cancel()
	interruptErr := t.Interrupt(interruptCtx, turnID)
	if interruptErr == nil {
		select {
		case completion := <-state.done:
			result := completion.result
			return result, &TurnError{Result: result, Cause: errors.Join(callerErr, completion.err)}
		case <-interruptCtx.Done():
			interruptErr = errors.New("timed out waiting for interrupted turn/completed")
		case <-t.client.terminal:
			interruptErr = t.client.getTerminalError()
		}
	}
	select {
	case completion := <-state.done:
		result := completion.result
		return result, &TurnError{Result: result, Cause: errors.Join(callerErr, completion.err)}
	default:
	}
	result := state.snapshot()
	t.markStateUnknown()
	return result, &TurnError{Result: result, Cause: errors.Join(callerErr, ErrThreadStateUnknown, interruptErr)}
}

func (t *Thread) Interrupt(ctx context.Context, turnID string) error {
	if t == nil || t.client == nil {
		return ErrClosed
	}
	if turnID == "" {
		return errors.New("turn id is empty")
	}
	return t.client.Call(ctx, "turn/interrupt", struct {
		ThreadID string `json:"threadId"`
		TurnID   string `json:"turnId"`
	}{t.id, turnID}, nil)
}

func (c *Client) registerTurn(threadID string, state *turnState) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing {
		return ErrClosed
	}
	if c.terminalErr != nil {
		return c.terminalErr
	}
	if _, exists := c.activeTurns[threadID]; exists {
		return ErrTurnActive
	}
	c.activeTurns[threadID] = state
	return nil
}

func (c *Client) unregisterTurn(threadID string, state *turnState) {
	c.mu.Lock()
	if c.activeTurns[threadID] == state {
		delete(c.activeTurns, threadID)
	}
	c.mu.Unlock()
}

func (c *Client) dispatchNotification(method string, params json.RawMessage) {
	switch method {
	case "item/completed":
		var event struct {
			ThreadID string          `json:"threadId"`
			TurnID   string          `json:"turnId"`
			Item     json.RawMessage `json:"item"`
		}
		if json.Unmarshal(params, &event) != nil || event.ThreadID == "" || len(event.Item) == 0 {
			return
		}
		if state := c.findTurn(event.ThreadID); state != nil {
			state.addItem(event.TurnID, event.Item)
		}
	case "turn/completed":
		var event struct {
			ThreadID string `json:"threadId"`
			Turn     struct {
				ID     string            `json:"id"`
				Status TurnStatus        `json:"status"`
				Items  []json.RawMessage `json:"items"`
				Error  *TurnFailure      `json:"error"`
			} `json:"turn"`
		}
		if json.Unmarshal(params, &event) != nil || event.ThreadID == "" || event.Turn.ID == "" {
			return
		}
		if state := c.findTurn(event.ThreadID); state != nil {
			state.complete(event.Turn.ID, event.Turn.Status, event.Turn.Items, event.Turn.Error)
		}
	default:

	}
}

func (c *Client) findTurn(threadID string) *turnState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.activeTurns[threadID]
}

func (s *turnState) setTurnID(turnID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.turnID != "" && s.turnID != turnID {
		return fmt.Errorf("turn id changed from %s to %s", s.turnID, turnID)
	}
	s.turnID = turnID
	for _, event := range s.pendingEvents {
		if event.turnID != turnID || s.finished {
			continue
		}
		if event.completed {
			s.completeLocked(event.status, event.items, event.failure)
			continue
		}
		s.addItemLocked(event.item)
	}
	s.pendingEvents = nil
	return nil
}

func (s *turnState) getTurnID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.turnID
}

func (s *turnState) addItem(turnID string, item json.RawMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finished {
		return
	}
	if s.turnID == "" {
		s.pendingEvents = append(s.pendingEvents, pendingTurnEvent{
			turnID: turnID,
			item:   append(json.RawMessage(nil), item...),
		})
		return
	}
	if s.turnID != turnID {
		return
	}
	s.addItemLocked(item)
}

func (s *turnState) addItemLocked(item json.RawMessage) {
	s.items = append(s.items, append(json.RawMessage(nil), item...))
}

func (s *turnState) complete(turnID string, status TurnStatus, items []json.RawMessage, failure *TurnFailure) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finished {
		return
	}
	if s.turnID == "" {
		s.pendingEvents = append(s.pendingEvents, pendingTurnEvent{
			turnID:    turnID,
			status:    status,
			items:     cloneRawMessages(items),
			failure:   cloneTurnFailure(failure),
			completed: true,
		})
		return
	}
	if s.turnID != turnID {
		return
	}
	s.completeLocked(status, items, failure)
}

func (s *turnState) completeLocked(status TurnStatus, items []json.RawMessage, failure *TurnFailure) {
	s.finished = true
	s.status = status
	if len(s.items) == 0 && len(items) != 0 {
		s.items = cloneRawMessages(items)
	}
	s.failure = cloneTurnFailure(failure)
	s.done <- turnCompletion{result: s.snapshotLocked()}
}

func cloneTurnFailure(failure *TurnFailure) *TurnFailure {
	if failure == nil {
		return nil
	}
	copyFailure := *failure
	copyFailure.CodexErrorInfo = append(json.RawMessage(nil), failure.CodexErrorInfo...)
	copyFailure.AdditionalDetails = append(json.RawMessage(nil), failure.AdditionalDetails...)
	copyFailure.Raw = append(json.RawMessage(nil), failure.Raw...)
	return &copyFailure
}

func (s *turnState) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finished {
		return
	}
	s.finished = true
	s.done <- turnCompletion{result: s.snapshotLocked(), err: err}
}

func (s *turnState) snapshot() TurnResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked()
}

func (s *turnState) snapshotLocked() TurnResult {
	items := cloneRawMessages(s.items)
	result := TurnResult{
		ThreadID: s.threadID,
		TurnID:   s.turnID,
		Status:   s.status,
		Items:    items,
		Failure:  s.failure,
	}
	result.FinalText = finalAgentText(items)
	return result
}

func cloneRawMessages(items []json.RawMessage) []json.RawMessage {
	copyItems := make([]json.RawMessage, len(items))
	for i, item := range items {
		copyItems[i] = append(json.RawMessage(nil), item...)
	}
	return copyItems
}

func (s *turnState) hasOutputSchema() bool { return s.structured }

func finishTurn(completion turnCompletion, structured bool) (TurnResult, error) {
	result := completion.result
	if completion.err != nil {
		return result, &TurnError{Result: result, Cause: completion.err}
	}
	switch result.Status {
	case TurnStatusCompleted:
		if structured {
			if !json.Valid([]byte(result.FinalText)) {
				return result, &TurnError{Result: result, Cause: ErrInvalidOutput}
			}
			result.JSON = append(json.RawMessage(nil), result.FinalText...)
		}
		return result, nil
	case TurnStatusInterrupted:
		return result, &TurnError{Result: result, Cause: ErrTurnInterrupted}
	case TurnStatusFailed:
		cause := errors.New("turn failed")
		if result.Failure != nil && result.Failure.Message != "" {
			cause = errors.New(result.Failure.Message)
		}
		return result, &TurnError{Result: result, Cause: cause}
	default:
		return result, &TurnError{Result: result, Cause: fmt.Errorf("unexpected final turn status %q", result.Status)}
	}
}

func finalAgentText(items []json.RawMessage) string {
	var fallback, final string
	for _, raw := range items {
		var item struct {
			Type  string `json:"type"`
			Text  string `json:"text"`
			Phase string `json:"phase"`
		}
		if json.Unmarshal(raw, &item) != nil || item.Type != "agentMessage" {
			continue
		}
		fallback = item.Text
		if item.Phase == "final_answer" {
			final = item.Text
		}
	}
	if final != "" {
		return final
	}
	return fallback
}

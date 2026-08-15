package aether

import (
	"context"
	"errors"
	"sync"
)

type ThreadOptions struct {
	Model          string         `json:"model,omitempty"`
	CWD            string         `json:"cwd,omitempty"`
	ApprovalPolicy string         `json:"approvalPolicy,omitempty"`
	Sandbox        string         `json:"sandbox,omitempty"`
	Ephemeral      bool           `json:"ephemeral,omitempty"`
	Config         map[string]any `json:"config,omitempty"`
}

type Thread struct {
	client *Client
	id     string

	mu     sync.Mutex
	active bool
	closed bool
}

func (c *Client) StartThread(ctx context.Context, opts ThreadOptions) (*Thread, error) {
	if c == nil {
		return nil, ErrClosed
	}
	var response struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
	}
	if err := c.Call(ctx, "thread/start", opts, &response); err != nil {
		return nil, err
	}
	if response.Thread.ID == "" {
		return nil, errors.New("thread/start response omitted thread id")
	}
	return &Thread{client: c, id: response.Thread.ID}, nil
}

func (t *Thread) ID() string {
	if t == nil {
		return ""
	}
	return t.id
}

func (t *Thread) Close() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	return nil
}

func (t *Thread) beginRun() error {
	if t == nil || t.client == nil {
		return ErrClosed
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return ErrClosed
	}
	if t.active {
		return ErrTurnActive
	}
	t.active = true
	return nil
}

func (t *Thread) endRun() {
	t.mu.Lock()
	t.active = false
	t.mu.Unlock()
}

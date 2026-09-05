package aether

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/signaturekey/aether/internal/jsonrpc"
)

const (
	stderrTailLimit          = 32 << 10
	maxServerRequestHandlers = 16
	maxQueuedWrites          = maxServerRequestHandlers * 4
	stdoutEOFWait            = 20 * time.Millisecond
)

type Client struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	stderr io.ReadCloser

	opts Options

	ctx    context.Context
	cancel context.CancelFunc

	writeCh        chan writeRequest
	serverRequests chan ServerRequest

	mu          sync.Mutex
	nextID      uint64
	pending     map[uint64]chan callResponse
	activeTurns map[string]*turnState
	closing     bool
	terminalErr error

	terminal     chan struct{}
	terminalOnce sync.Once
	processDone  chan struct{}
	stderrDone   chan struct{}
	closeDone    chan struct{}
	closeOnce    sync.Once
	closeErr     error

	stderrTail *tailBuffer
}

type writeRequest struct {
	data []byte
	done chan error
}

type callResponse struct {
	result []byte
	err    error
}

func Start(ctx context.Context, options Options) (*Client, error) {
	if ctx == nil {
		return nil, &StartupError{Cause: errors.New("nil context")}
	}
	opts, err := options.withDefaults()
	if err != nil {
		return nil, &StartupError{Cause: err}
	}
	executable, err := exec.LookPath(opts.Command[0])
	if err != nil {
		return nil, &StartupError{Command: opts.Command[0], Cause: err}
	}

	cmd := exec.Command(executable, opts.Command[1:]...)
	cmd.Dir = opts.Dir
	cmd.Env = overlayEnvironment(os.Environ(), opts.Env)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, &StartupError{Command: opts.Command[0], Cause: err}
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, &StartupError{Command: opts.Command[0], Cause: err}
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, &StartupError{Command: opts.Command[0], Cause: err}
	}
	if err := cmd.Start(); err != nil {
		return nil, &StartupError{Command: opts.Command[0], Cause: err}
	}

	lifecycle, cancel := context.WithCancel(context.Background())
	c := &Client{
		cmd:            cmd,
		stdin:          stdin,
		stdout:         stdout,
		stderr:         stderr,
		opts:           opts,
		ctx:            lifecycle,
		cancel:         cancel,
		writeCh:        make(chan writeRequest, maxQueuedWrites),
		serverRequests: make(chan ServerRequest, maxServerRequestHandlers),
		pending:        make(map[uint64]chan callResponse),
		activeTurns:    make(map[string]*turnState),
		terminal:       make(chan struct{}),
		processDone:    make(chan struct{}),
		stderrDone:     make(chan struct{}),
		closeDone:      make(chan struct{}),
		stderrTail:     newTailBuffer(stderrTailLimit),
	}

	go c.writeLoop()
	go c.decodeLoop()
	go c.stderrLoop()
	go c.waitLoop()
	for range maxServerRequestHandlers {
		go c.serverRequestLoop()
	}

	handshakeCtx, handshakeCancel := context.WithTimeout(ctx, opts.HandshakeTimeout)
	defer handshakeCancel()
	var initialized struct {
		UserAgent      string `json:"userAgent"`
		PlatformFamily string `json:"platformFamily"`
		PlatformOS     string `json:"platformOs"`
	}
	if err := c.Call(handshakeCtx, "initialize", struct {
		ClientInfo ClientInfo `json:"clientInfo"`
	}{opts.ClientInfo}, &initialized); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("initialize app-server: %w", err)
	}
	if err := c.Notify(handshakeCtx, "initialized", struct{}{}); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("acknowledge app-server initialization: %w", err)
	}
	return c, nil
}

func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		go c.closeProcess()
	})
	<-c.closeDone
	return c.closeErr
}

func (c *Client) closeProcess() {
	c.mu.Lock()
	c.closing = true
	pending := c.takePendingLocked()
	active := c.takeActiveLocked()
	c.mu.Unlock()
	failCalls(pending, ErrClosed)
	failTurns(active, ErrClosed)

	_ = c.stdin.Close()
	timer := time.NewTimer(c.opts.CloseTimeout)
	defer timer.Stop()
	select {
	case <-c.processDone:
	case <-timer.C:
		if c.cmd.Process != nil {
			if err := c.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				c.closeErr = fmt.Errorf("kill codex app-server: %w", err)
			}
		}
		select {
		case <-c.processDone:
		case <-time.After(c.opts.CloseTimeout):
			if c.closeErr == nil {
				c.closeErr = errors.New("timed out waiting for codex app-server to exit")
			}
		}
	}
	c.cancel()
	close(c.closeDone)
}

func (c *Client) writeLoop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case req := <-c.writeCh:
			_, err := c.stdin.Write(req.data)
			if req.done != nil {
				req.done <- err
			}
			if err != nil {
				c.terminate(fmt.Errorf("write app-server stdin: %w", err))
				c.killProcess()
				return
			}
		}
	}
}

func (c *Client) decodeLoop() {
	dec := jsonrpc.NewDecoder(c.stdout)
	for {
		msg, err := jsonrpc.Decode(dec)
		if err != nil {
			if errors.Is(err, io.EOF) {
				select {
				case <-c.processDone:
					return
				case <-time.After(stdoutEOFWait):
				}
				c.terminate(&ProcessError{
					ExitCode: -1,
					Cause:    errors.Join(ErrProcessExited, errors.New("app-server stdout closed")),
					Stderr:   c.stderrTail.String(),
				})
				c.killProcess()
				return
			}
			c.terminate(errors.Join(ErrUnsupportedMessage, fmt.Errorf("decode app-server stdout: %w", err)))
			c.killProcess()
			return
		}
		switch msg.Kind {
		case jsonrpc.Response:
			c.dispatchResponse(msg)
		case jsonrpc.Notification:
			c.dispatchNotification(msg.Method, msg.Params)
		case jsonrpc.Request:
			request := ServerRequest{ID: msg.ID, Method: msg.Method, Params: msg.Params}
			c.enqueueServerRequest(request)
		default:
			c.terminate(ErrUnsupportedMessage)
		}
	}
}

func (c *Client) enqueueServerRequest(request ServerRequest) {
	if c.opts.Handlers[request.Method] == nil {
		c.rejectServerRequest(request.ID, -32601, "method not found")
		return
	}
	select {
	case c.serverRequests <- request:
	default:
		c.rejectServerRequest(request.ID, -32000, "server request queue is full")
	}
}

func (c *Client) rejectServerRequest(id json.RawMessage, code int, message string) {
	wire, err := jsonrpc.EncodeError(id, jsonrpc.RPCError{Code: code, Message: message})
	if err != nil || !c.sendWireAsync(wire) {
		c.terminate(errors.New(message))
		c.killProcess()
	}
}

func (c *Client) serverRequestLoop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case request := <-c.serverRequests:
			c.handleServerRequest(request)
		}
	}
}

func (c *Client) stderrLoop() {
	defer close(c.stderrDone)
	_, _ = io.Copy(stderrCapture{tail: c.stderrTail, sink: c.opts.Stderr}, c.stderr)
}

func (c *Client) waitLoop() {
	err := c.cmd.Wait()
	<-c.stderrDone
	close(c.processDone)

	c.mu.Lock()
	closing := c.closing
	c.mu.Unlock()
	if closing {
		c.terminate(ErrClosed)
		return
	}
	exitCode := -1
	if c.cmd.ProcessState != nil {
		exitCode = c.cmd.ProcessState.ExitCode()
	}
	if err == nil {
		err = ErrProcessExited
	}
	c.terminate(&ProcessError{ExitCode: exitCode, Cause: err, Stderr: c.stderrTail.String()})
}

func (c *Client) terminate(err error) {
	if err == nil {
		err = ErrProcessExited
	}
	c.terminalOnce.Do(func() {
		c.mu.Lock()
		c.terminalErr = err
		pending := c.takePendingLocked()
		active := c.takeActiveLocked()
		c.mu.Unlock()
		c.cancel()
		close(c.terminal)
		failCalls(pending, err)
		failTurns(active, err)
	})
}

func (c *Client) killProcess() {
	if c.cmd.Process != nil {
		_ = c.cmd.Process.Kill()
	}
}

func (c *Client) takePendingLocked() []chan callResponse {
	result := make([]chan callResponse, 0, len(c.pending))
	for id, ch := range c.pending {
		delete(c.pending, id)
		result = append(result, ch)
	}
	return result
}

func (c *Client) takeActiveLocked() []*turnState {
	result := make([]*turnState, 0, len(c.activeTurns))
	for threadID, state := range c.activeTurns {
		delete(c.activeTurns, threadID)
		result = append(result, state)
	}
	return result
}

func failCalls(calls []chan callResponse, err error) {
	for _, ch := range calls {
		ch <- callResponse{err: err}
	}
}

func failTurns(turns []*turnState, err error) {
	for _, state := range turns {
		state.fail(err)
	}
}

type tailBuffer struct {
	mu    sync.Mutex
	limit int
	data  []byte
}

func newTailBuffer(limit int) *tailBuffer { return &tailBuffer{limit: limit} }

func (b *tailBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(p) >= b.limit {
		b.data = append(b.data[:0], p[len(p)-b.limit:]...)
		return len(p), nil
	}
	overflow := len(b.data) + len(p) - b.limit
	if overflow > 0 {
		copy(b.data, b.data[overflow:])
		b.data = b.data[:len(b.data)-overflow]
	}
	b.data = append(b.data, p...)
	return len(p), nil
}

func (b *tailBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(append([]byte(nil), b.data...))
}

type stderrCapture struct {
	tail *tailBuffer
	sink io.Writer
}

func (w stderrCapture) Write(p []byte) (n int, err error) {
	_, _ = w.tail.Write(p)
	if w.sink != nil {
		func() {
			defer func() { _ = recover() }()
			_, _ = w.sink.Write(p)
		}()
	}
	return len(p), nil
}

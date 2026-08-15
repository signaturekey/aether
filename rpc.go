package aether

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/signaturekey/aether/internal/jsonrpc"
)

type ServerRequest struct {
	ID     json.RawMessage
	Method string
	Params json.RawMessage
}

type RequestHandler func(context.Context, ServerRequest) (any, error)

func (c *Client) Call(ctx context.Context, method string, params any, result any) error {
	if c == nil {
		return ErrClosed
	}
	if ctx == nil {
		return errors.New("call context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	c.mu.Lock()
	if c.closing {
		c.mu.Unlock()
		return ErrClosed
	}
	if c.terminalErr != nil {
		err := c.terminalErr
		c.mu.Unlock()
		return err
	}
	c.nextID++
	id := c.nextID
	responseCh := make(chan callResponse, 1)
	c.pending[id] = responseCh
	c.mu.Unlock()

	wire, err := jsonrpc.EncodeRequest(id, method, params)
	if err != nil {
		c.removePending(id)
		return fmt.Errorf("encode %s request: %w", method, err)
	}
	if err := c.sendWire(ctx, wire); err != nil {
		c.removePending(id)
		return err
	}

	select {
	case response := <-responseCh:
		return decodeCallResponse(method, response, result)
	case <-ctx.Done():
		if c.removePending(id) {
			return ctx.Err()
		}
		response := <-responseCh
		return decodeCallResponse(method, response, result)
	case <-c.terminal:
		select {
		case response := <-responseCh:
			return decodeCallResponse(method, response, result)
		default:
			return c.getTerminalError()
		}
	}
}

func (c *Client) Notify(ctx context.Context, method string, params any) error {
	if c == nil {
		return ErrClosed
	}
	if ctx == nil {
		return errors.New("notify context is nil")
	}
	wire, err := jsonrpc.EncodeNotification(method, params)
	if err != nil {
		return fmt.Errorf("encode %s notification: %w", method, err)
	}
	return c.sendWire(ctx, wire)
}

func (c *Client) sendWire(ctx context.Context, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	closing := c.closing
	terminalErr := c.terminalErr
	c.mu.Unlock()
	if closing {
		return ErrClosed
	}
	if terminalErr != nil {
		return terminalErr
	}
	req := writeRequest{data: data, done: make(chan error, 1)}
	select {
	case c.writeCh <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-c.terminal:
		return c.getTerminalError()
	}
	select {
	case err := <-req.done:
		if err != nil {
			return fmt.Errorf("write app-server message: %w", err)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.terminal:
		return c.getTerminalError()
	}
}

func (c *Client) dispatchResponse(msg jsonrpc.Message) {
	var id uint64
	if err := json.Unmarshal(msg.ID, &id); err != nil {
		c.terminate(errors.Join(ErrUnsupportedMessage, fmt.Errorf("decode response id: %w", err)))
		return
	}
	c.mu.Lock()
	ch := c.pending[id]
	delete(c.pending, id)
	c.mu.Unlock()
	if ch == nil {
		return
	}
	response := callResponse{result: append([]byte(nil), msg.Result...)}
	if msg.Error != nil {
		response.err = &RPCError{Code: msg.Error.Code, Message: msg.Error.Message, Data: append([]byte(nil), msg.Error.Data...)}
	}
	ch <- response
}

func (c *Client) removePending(id uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.pending[id]; !ok {
		return false
	}
	delete(c.pending, id)
	return true
}

func decodeCallResponse(method string, response callResponse, result any) error {
	if response.err != nil {
		return response.err
	}
	if result == nil {
		return nil
	}
	if err := json.Unmarshal(response.result, result); err != nil {
		return fmt.Errorf("decode %s response: %w", method, err)
	}
	return nil
}

func (c *Client) getTerminalError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.terminalErr == nil {
		return ErrClosed
	}
	return c.terminalErr
}

func (c *Client) handleServerRequest(request ServerRequest) {
	handler := c.opts.Handlers[request.Method]
	if handler == nil {
		wire, err := jsonrpc.EncodeError(request.ID, jsonrpc.RPCError{Code: -32601, Message: "method not found"})
		if err == nil {
			_ = c.sendWire(c.ctx, wire)
		}
		return
	}

	result, handlerErr := callHandler(c.ctx, handler, request)
	var wire []byte
	var err error
	if handlerErr == nil {
		wire, err = jsonrpc.EncodeResult(request.ID, result)
	} else {
		wireErr := jsonrpc.RPCError{Code: -32603, Message: handlerErr.Error()}
		var rpcErr *RPCError
		if errors.As(handlerErr, &rpcErr) {
			wireErr.Code = rpcErr.Code
			wireErr.Message = rpcErr.Message
			wireErr.Data = rpcErr.Data
		}
		wire, err = jsonrpc.EncodeError(request.ID, wireErr)
	}
	if err == nil {
		_ = c.sendWire(c.ctx, wire)
	}
}

func callHandler(ctx context.Context, handler RequestHandler, request ServerRequest) (result any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("server request handler panicked: %v", recovered)
		}
	}()
	return handler(ctx, request)
}

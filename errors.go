package aether

import (
	"encoding/json"
	"errors"
	"fmt"
)

var (
	ErrClosed             = errors.New("aether client closed")
	ErrProcessExited      = errors.New("codex app-server exited")
	ErrTurnActive         = errors.New("thread already has an active turn")
	ErrThreadStateUnknown = errors.New("thread state is unknown")
	ErrTurnInterrupted    = errors.New("turn interrupted")
	ErrInvalidOutput      = errors.New("invalid structured output")
	ErrUnsupportedMessage = errors.New("unsupported app-server message")
)

type StartupError struct {
	Command string
	Cause   error
}

func (e *StartupError) Error() string {
	return fmt.Sprintf("start codex app-server %q: %v", e.Command, e.Cause)
}

func (e *StartupError) Unwrap() error { return e.Cause }

type RPCError struct {
	Code    int
	Message string
	Data    json.RawMessage
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("app-server RPC error %d: %s", e.Code, e.Message)
}

type ProcessError struct {
	ExitCode int
	Cause    error
	Stderr   string
}

func (e *ProcessError) Error() string {
	if e.Stderr == "" {
		return fmt.Sprintf("codex app-server exited with code %d: %v", e.ExitCode, e.Cause)
	}
	return fmt.Sprintf("codex app-server exited with code %d: %v (stderr: %s)", e.ExitCode, e.Cause, e.Stderr)
}

func (e *ProcessError) Unwrap() error { return e.Cause }

func (e *ProcessError) Is(target error) bool { return target == ErrProcessExited }

type TurnError struct {
	Result TurnResult
	Cause  error
}

func (e *TurnError) Error() string { return fmt.Sprintf("turn %s: %v", e.Result.TurnID, e.Cause) }
func (e *TurnError) Unwrap() error { return e.Cause }

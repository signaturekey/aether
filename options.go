package aether

import (
	"errors"
	"io"
	"sort"
	"strings"
	"time"
)

const (
	defaultHandshakeTimeout = 10 * time.Second
	defaultInterruptTimeout = 5 * time.Second
	defaultCloseTimeout     = 5 * time.Second
)

type ClientInfo struct {
	Name    string `json:"name"`
	Title   string `json:"title"`
	Version string `json:"version"`
}

type Options struct {
	Command []string
	Dir     string
	Env     map[string]string

	ClientInfo ClientInfo
	Stderr     io.Writer
	Handlers   map[string]RequestHandler

	HandshakeTimeout time.Duration
	InterruptTimeout time.Duration
	CloseTimeout     time.Duration
}

func (o Options) withDefaults() (Options, error) {
	if len(o.Command) == 0 {
		o.Command = []string{"codex", "app-server", "--listen", "stdio://"}
	}
	if strings.TrimSpace(o.Command[0]) == "" {
		return Options{}, errors.New("command executable is empty")
	}
	for method, handler := range o.Handlers {
		if method == "" || handler == nil {
			return Options{}, errors.New("handler methods and functions must be non-empty")
		}
	}
	if o.HandshakeTimeout < 0 || o.InterruptTimeout < 0 || o.CloseTimeout < 0 {
		return Options{}, errors.New("timeouts must not be negative")
	}
	if o.HandshakeTimeout == 0 {
		o.HandshakeTimeout = defaultHandshakeTimeout
	}
	if o.InterruptTimeout == 0 {
		o.InterruptTimeout = defaultInterruptTimeout
	}
	if o.CloseTimeout == 0 {
		o.CloseTimeout = defaultCloseTimeout
	}
	if o.ClientInfo.Name == "" {
		o.ClientInfo.Name = "aether"
	}
	if o.ClientInfo.Title == "" {
		o.ClientInfo.Title = "Aether"
	}
	if o.ClientInfo.Version == "" {
		o.ClientInfo.Version = "0.1.0"
	}
	o.Command = append([]string(nil), o.Command...)
	o.Env = cloneStringMap(o.Env)
	o.Handlers = cloneHandlers(o.Handlers)
	return o, nil
}

func overlayEnvironment(base []string, overrides map[string]string) []string {
	values := make(map[string]string, len(base)+len(overrides))
	for _, entry := range base {
		key, _, ok := strings.Cut(entry, "=")
		if ok {
			values[key] = entry
		}
	}
	for key, value := range overrides {
		values[key] = key + "=" + value
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	env := make([]string, 0, len(keys))
	for _, key := range keys {
		env = append(env, values[key])
	}
	return env
}

func cloneStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneHandlers(in map[string]RequestHandler) map[string]RequestHandler {
	out := make(map[string]RequestHandler, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

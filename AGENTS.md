# Aether Engineering and Product Specification

This file is the repository-level source of truth for coding agents and contributors.

## 1. Product boundary

`aether` is a small, consumer-neutral Go SDK for controlling a locally installed
`codex app-server` process over the stable stdio protocol.

The SDK owns:

- child-process startup and shutdown;
- App Server initialization;
- JSONL encoding and decoding;
- RPC request/response correlation;
- notification dispatch;
- server-initiated request handling;
- thread and turn lifecycle;
- structured output transport;
- cancellation and process-failure propagation.

The SDK does not own a consumer's business workflow. Do not add repository review,
Git, routing, role, report, issue-tracker, orchestration-policy, or other
application-specific concepts to public or internal packages.

## 2. Working agreement

Before implementing a change:

1. Check the current official App Server documentation:
   <https://learn.chatgpt.com/docs/app-server>.
2. Distinguish stable protocol behavior from experimental behavior.
3. Keep the change inside the current implementation phase.
4. Prefer the smallest public API that satisfies a demonstrated consumer need.
5. Record protocol decisions in tests and documentation, not only comments.
6. Report what was implemented, what was actually tested, and what remains unverified.

Do not commit, push, create branches, publish releases, or mutate external systems
unless the user explicitly requests that action.

## 3. Non-goals

Do not add:

- direct OpenAI HTTP API calls;
- API-key management;
- Codex installation or self-update behavior;
- credential discovery, copying, or migration;
- a private replacement for the user's Codex home;
- Git or source-repository logic;
- a CLI application;
- WebSocket or Unix-socket transports;
- a database or daemon;
- automatic retries;
- exhaustive generated App Server types;
- consumer-specific convenience APIs.

## 4. Protocol rules

The default command is:

```text
codex app-server --listen stdio://
```

The stdio protocol is newline-delimited JSON. It resembles JSON-RPC 2.0 but omits the
`jsonrpc` field on the wire.

The connection must send exactly one `initialize` request and then one `initialized`
notification before ordinary methods.

Classify inbound messages structurally:

- `id` without `method`: response;
- `method` without `id`: notification;
- `method` with `id`: server-initiated request.

Use `json.Decoder`; do not use `bufio.Scanner` for App Server stdout. Messages can be
larger than Scanner's default token limit.

Unknown fields must be tolerated. Unknown notification methods must not crash or close
the client. Unknown server-initiated requests must receive a prompt method-not-found
response instead of being ignored.

Stay on the stable API surface by default. Do not set `experimentalApi: true` without
an explicit product decision, documentation, and isolated tests.

## 5. Public API rules

- Prefer concrete exported types over package-owned interfaces.
- Accept `context.Context` as the first argument of blocking operations.
- Do not store a caller context inside a long-lived public object.
- Keep raw `Call` and `Notify` available as forward-compatible escape hatches.
- Type only the stable, commonly used thread and turn fields.
- Preserve evolving item and error payloads with `json.RawMessage`.
- Do not expose internal goroutines, channels, maps, or wire envelopes.
- Do not add an option until the implementation and tests define its semantics.
- Do not silently change defaults in a minor release.

Public documentation must state whether an operation is safe for concurrent use.

## 6. Concurrency invariants

One `Client` may be used concurrently by multiple goroutines.

The implementation has:

- one serialized outbound writer path;
- one stdout decoder;
- one stderr drainer;
- one process waiter;
- a pending-call registry;
- an active-turn registry.

Each request ID belongs to at most one pending call. Each pending call completes
exactly once. Child-process failure completes every pending call and active turn.

Different threads may run turns concurrently. The same thread may have at most one
active `Run` operation unless the stable App Server contract explicitly changes.

Never invoke user handlers synchronously from the stdout decoder goroutine. A slow or
panicking handler must not stop protocol decoding.

Do not hold a mutex while:

- writing to a user-provided writer;
- waiting on a channel;
- calling a user handler;
- waiting for the child process;
- performing a potentially blocking pipe write.

Run concurrency-sensitive tests with `go test -race` and repeated counts.

## 7. Context and cancellation

The context passed to `Start` controls startup and handshake. A successfully returned
client owns a private lifecycle context that ends on `Close` or process exit.

Cancelling `Call` removes the local waiter but does not imply the server did not execute
the request. Therefore the SDK must not retry automatically.

Cancelling an active turn must use `turn/interrupt` with an internal bounded context.
It must not kill the shared client or unrelated threads.

Cancellation paths must be bounded and leak-free. Test cancellation before write,
after write, after response, during event streaming, during interruption, and during
client shutdown.

## 8. Process and authentication safety

- Use `os/exec` with an argv slice; never invoke a shell.
- Resolve the default executable through `exec.LookPath`.
- Inherit the caller environment by default.
- Apply environment overrides explicitly and deterministically.
- Never inspect, log, copy, or rewrite credential contents.
- Never accept or persist an API key.
- Never replace common environment variables such as `HOME` internally.
- Start stdout and stderr draining before waiting for handshake results.
- Call `cmd.Wait` exactly once.
- `Close` must be idempotent and bounded.
- Kill only the child process owned by the current client.
- Do not include unbounded stderr or the full environment in returned errors.

The user is responsible for installing and authenticating Codex. Return actionable
startup or authentication errors without attempting repair.

## 9. Errors

Use typed errors and support `errors.Is` / `errors.As`.

Preserve:

- App Server RPC error code, message, and raw data;
- child exit code and cause;
- turn status and partial result;
- the original context error;
- bounded diagnostic stderr when safe.

Do not flatten structured failures into opaque formatted strings. Do not panic for
ordinary protocol, process, configuration, cancellation, or decoding failures.

## 10. MCP boundary

Codex/App Server owns configured MCP servers. This SDK transports App Server messages.
It does not own MCP server configuration or any particular external service.

The raw RPC API is sufficient for initial MCP access, including status, resource read,
and tool calls. Add typed MCP helpers only after repeated real consumer demand. Typed
helpers must remain generic and must not encode a specific server's tools or schemas.

Server-initiated MCP elicitation must be handled through the generic request-handler
mechanism or rejected promptly. It must never hang silently.

## 11. Compatibility

- The installed Codex CLI is an external runtime dependency.
- Record exact versions used by live integration tests.
- Do not claim compatibility that was not tested.
- Tolerate unknown fields and notifications from newer versions.
- Return explicit errors for incompatible required behavior.
- Do not reject a newer version solely because it is newer.
- Do not bundle the full generated App Server schema into production code.
- Generated schemas may be used in development to compare protocol versions.

Changes to public behavior require README and test updates. Breaking public API changes
before `v1` still require explicit release notes.

## 12. Dependencies and Go style

- The Go version in `go.mod` is authoritative.
- Prefer the standard library.
- Add a dependency only when it materially reduces correctness risk or maintenance.
- Avoid package-level mutable state.
- Keep package boundaries capability-oriented; do not create generic utility packages.
- Keep exported methods small and delegate protocol mechanics to unexported code.
- Wrap errors with operation context while preserving typed causes.
- Use `gofmt`.
- Follow ordinary Go naming; avoid Java-style builders and option hierarchies.
- Do not add interfaces solely for mocks.
- Do not add logging frameworks; use a narrow callback or `io.Writer` only when needed.

## 13. Testing

Most tests must be hermetic and must not require network access, authentication, or an
installed Codex CLI.

Use in-memory transports and a fake child App Server to cover:

- handshake success and failure;
- large JSON messages;
- out-of-order concurrent responses;
- notifications interleaved with responses;
- server-initiated requests;
- context cancellation and late responses;
- turn event correlation;
- process exit and stderr handling;
- close and cancellation races;
- goroutine cleanup.

Live tests must be opt-in, clearly named integration tests. They must use harmless
temporary directories and must not modify user projects or external systems.

Before reporting completion, run as applicable:

```text
gofmt -w <Go files changed by this task>
gofmt -l .
go test ./...
go test -race ./...
go vet ./...
```

For race-prone tests, also use repeated execution such as `-count=100`. Never describe
an unrun or skipped command as passing.

## 14. Documentation

Keep these documents aligned:

- `README.md`: installation, examples, compatibility, and user-facing behavior;
- Go doc comments: exact exported API semantics.

When official App Server documentation and local assumptions conflict, stop and resolve
the conflict explicitly. Update tests and design documentation together with a protocol
decision.

## 15. Change discipline

- Preserve unrelated user changes in a dirty worktree.
- Do not rewrite broad areas to satisfy a local change.
- Do not add speculative abstractions for deferred features.
- Prefer one complete vertical behavior with tests over many empty layers.
- Keep transport correctness separate from consumer policy.
- Surface blockers with concrete evidence instead of silently weakening an invariant.

Completion reports must state:

- files changed;
- behavior implemented;
- tests actually run and their result;
- live integration status and exact Codex version when tested;
- remaining limitations;
- whether commit, push, or release actions were intentionally not performed.

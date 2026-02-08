# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Proglog is a distributed commit log service exposed over gRPC with mTLS authentication and Casbin-based ACL
authorization. Written in Go (`github.com/Devin-Yeung/proglog`).

## Development Environment

The project uses [devenv](https://devenv.sh/) (Nix-based) to manage tooling. The devenv shell provides: `protoc`,
`protoc-gen-go`, `protoc-gen-go-grpc`, `gotestsum`, `cfssl`, and `just`. The `PROGLOG_CONFIG_DIR` environment variable
is set automatically by devenv and points to generated TLS certs and ACL config needed by tests.

**First-time setup** (generates TLS certs and copies ACL rules):

```
just setup
```

## Build & Test Commands

| Task                         | Command                                              |
|------------------------------|------------------------------------------------------|
| Run all tests                | `just test` (uses `gotestsum`) or `go test ./...`    |
| Run tests with race detector | `go test -race ./...`                                |
| Run a single test            | `go test -run TestName ./internal/log/...`           |
| Run a single subtest         | `go test -run TestLog/produce ./internal/server/...` |
| Compile protobuf             | `just compile`                                       |
| Generate TLS certs           | `just gencert`                                       |
| Copy ACL rules to config dir | `just gen-acl-rules`                                 |
| Run dev server               | `just dev-up`                                        |

## Architecture

### Commit Log (`internal/log/`)

A segmented, append-only log persisted to disk. See `internal/log/DESIGN.md` for the full on-disk layout.

- **Log** - top-level type managing a slice of segments; one active segment receives appends, rolls to a new segment
  when full. Uses `RWMutex` for concurrency. Has a broadcast channel (`notify`) that is closed-and-replaced on each
  `Append` to wake consumers via `WaitForAppend`.
- **Segment** - pairs a store + index for a contiguous offset range (`[baseOffset, nextOffset)`).
- **Store** - length-prefixed binary file (`<8-byte len><payload>`), buffered writes via `bufio.Writer`.
- **Index** - memory-mapped (mmap via `gommap`) fixed-width entries (4-byte relative offset + 8-byte store position = 12
  bytes). File is pre-allocated to `maxIndexBytes`, then truncated on close.
- **Config** - builder-pattern config (`NewConfig().WithSegmentMaxStoreBytes(...)`) with unexported `segment` struct
  fields.

### gRPC Server (`internal/server/`)

- **`grpc_server.go`** - `NewGRPCServer` wires up interceptors (zap logging, TLS auth) and registers the `Log` service.
- **`grpc_log_server.go`** - implements `Produce`, `Consume`, `ProduceStream`, `ConsumeStream`. `ConsumeStream` acts
  like `tail -f` using `WaitForAppend` to block when caught up.
- **`config.go`** - defines `CommitLog` and `Authorizer` interfaces that the server depends on (not the concrete
  implementations).
- **`middleware/auth.go`** - extracts the client's TLS CommonName from the peer certificate into context.
- **`middleware/zap.go`** - adapts `zap.Logger` to the `go-grpc-middleware` logging interceptor.

### Auth & TLS

- **`internal/auth/`** - Casbin enforcer wrapper. ACL model: `(subject, object, action)`. Policy file grants `root` user
  `produce`/`consume` on `*`.
- **`internal/config/`** - `files.go` resolves cert/key/ACL paths from `PROGLOG_CONFIG_DIR` env var (panics if unset).
  `tls.go` provides a `SetupTLSConfig` helper for both server (mTLS requiring client certs) and client roles.
- Two client identities for tests: `root` (superuser) and `nobody` (unprivileged).

### CLI (`cmd/server/`)

Uses `alecthomas/kong` for CLI arg parsing. Entry point: `cmd/server/main.go`.

### Protobuf (`api/v1/`)

- `log.proto` defines `Record`, the `Log` service (Produce, Consume, ProduceStream, ConsumeStream).
- `error.go` defines `ErrOffsetOutOfRange` implementing gRPC `GRPCStatus()` interface for rich error details.

## Testing Guidelines

- Use `require` for setup/teardown and critical operations that must succeed for the test to continue.
- Use `assert` inside loops and for multiple related field checks so all failures are reported in one run.
- See `guidelines/testing.md` for the full decision flow.

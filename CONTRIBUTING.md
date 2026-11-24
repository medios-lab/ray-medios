# Contributing to `ray-medios`

Thank you for your interest in contributing to `ray-medios`.

## Design Principle

[KubeRay](https://github.com/ray-project/kuberay) is the behavioral reference for `ray-medios`. Demand calculation, worker selection, graceful draining, and task/actor scheduling remain in Ray; `ray-medios` translates KubeRay's desired-state and pod lifecycle model onto Medios tasks.

If a proposed change intentionally departs from KubeRay's semantics (configuration meaning, scaling bounds, failure behavior, or networking), please open an issue to discuss the tradeoff before submitting a PR.

## Development Workflow

### Prerequisites

- Go `>= 1.25.0`

### Running Checks

```sh
go vet ./...
go test -race -v ./...
go build ./cmd/ray-medios
```

> Note: `internal/endpoint` contract tests bind loopback TLS listeners on `127.0.0.1` and require loopback networking in the test environment.

## Pull Requests

1. Fork the repository and create a topic branch from `main`.
2. Keep changes focused and include unit or contract tests covering the behavior change.
3. Update `docs/endpoint-contract.md`, `docs/task-mapping.md`, or `README.md` when contracts, flags, or supported behaviors change.
4. Ensure `go vet ./...` and `go test -race ./...` pass cleanly.

## License

By contributing to `ray-medios`, you agree that your contributions will be licensed under the [Apache License, Version 2.0](LICENSE).

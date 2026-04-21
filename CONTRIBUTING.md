# Contributing to zarrow

Thank you for contributing to `zarrow`.

## Development Setup

1. Install Zig `0.15.x` (minimum `0.15.1`).
2. Clone the repository.
3. Run:

```sh
zig build test
```

## Before Opening a PR

1. Keep changes focused and reviewable.
2. Add or update tests for behavior changes.
3. Run formatting and tests locally:

```sh
zig fmt src tools examples benchmarks build.zig
zig build test
```

4. If the change is performance-sensitive, also run:

```sh
zig build benchmark-smoke
```

## PR Guidelines

1. Explain what changed and why.
2. Mention compatibility impact (API/IPC/interop).
3. Include benchmark notes for perf changes.
4. Keep CI green.

## Coding Guidelines

1. Prefer clear, explicit code over clever shortcuts.
2. Preserve ownership/ref-count invariants.
3. Keep Arrow layout and IPC behavior spec-aligned.
4. Add tests for error paths, not only happy paths.

## API Stability

See [docs/api-stability.md](docs/api-stability.md).

## Release Process

See [docs/release-checklist.md](docs/release-checklist.md).

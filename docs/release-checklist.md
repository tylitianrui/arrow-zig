# Release Checklist

## Versioning (SemVer)

`zarrow` follows Semantic Versioning:

- `MAJOR`: breaking changes to stable public API/behavior.
- `MINOR`: backward-compatible features.
- `PATCH`: backward-compatible bug fixes and internal improvements.

## Pre-Release Checklist

1. Update version in `build.zig.zon` when needed.
2. Confirm changelog/release notes are prepared.
3. Ensure CI is green.
4. Run local validation:

```sh
zig build test
zig build examples
zig build fuzz-corpus
```

5. For perf-sensitive changes, run:

```sh
zig build benchmark-smoke
```

6. Verify interop checks are not regressed (PyArrow/arrow-rs/Arrow C++ lanes in CI).
7. Confirm docs for user-facing changes are updated.

## Release Notes Minimum

1. New features.
2. Bug fixes.
3. Breaking changes (if any) with migration guidance.
4. Compatibility notes (Zig version, IPC/interop behavior).

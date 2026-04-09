# Fuzz Corpus

This directory stores seed inputs used by CI and local fuzz replay.

## Layout

- `array-validate-layout/`: seeds for `zig build fuzz-array-layout`
- `ipc-reader/`: seeds for `zig build fuzz-ipc-reader`

## Why commit these files

- Reproducibility: any crash found in CI/nightly can be replayed locally.
- Regression protection: `zig build fuzz-corpus` runs in PR/Push CI.
- Fast signal: seed replay is deterministic and much faster than long random fuzzing.

## How to run

- Replay all seeds: `zig build fuzz-corpus`
- Single harness: `zig build fuzz-array-layout -- <seed.bin>`
- Single harness: `zig build fuzz-ipc-reader -- <seed.bin>`

## Seed conventions

Prefer small, focused files. Use descriptive names such as:

- `malformed_eos.bin`
- `negative_length_like.bin`
- `oob_offset_like.bin`

When a new crash is found:

1. Minimize the input.
2. Add it under the corresponding subdirectory.
3. Use a descriptive filename and include context in the PR description.

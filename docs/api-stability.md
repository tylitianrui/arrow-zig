# API Stability Policy

## Scope

This policy applies to public symbols exported from `src/root.zig` and documented behavior used by downstream libraries.

## Stability Levels

## Stable

- Explicitly exported and documented APIs intended for external use.
- Backward compatibility is preserved across patch/minor releases.
- Breaking changes require a major version bump.

## Experimental

- New or evolving APIs that may change quickly.
- May change in minor releases with clear release notes.
- Should be clearly marked in docs/comments as experimental.

## Internal

- Non-exported modules, helper types, and test-only utilities.
- No compatibility guarantees.

## Compatibility Rules

1. Stable API signatures should not be changed incompatibly without major bump.
2. Behavior changes to stable APIs must be documented.
3. Experimental APIs can evolve, but migration notes are still encouraged.
4. Any promotion from experimental to stable should be documented in release notes.

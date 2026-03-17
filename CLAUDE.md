# Redpanda

High-performance, Kafka-compatible streaming platform. Core in C++23 (Seastar, thread-per-core), CLI in Go (`rpk`), integration tests in Python.

## Stack
- Core: `src/v/` (C++23, Seastar, Bazel)
- CLI: `src/go/rpk/`
- Tests: `tests/rptest/` (Python), plus Bazel unit tests
- Protos: `proto/`
- Build: Bazel via Bazelisk — never use a plain `bazel` binary

## Commands
```bash
# Build
bazel build //...
bazel build //:rpk          # Go CLI only

# Test
bazel test //...

# Lint/format — MUST pass before any PR
bazel run //tools:clang_format
```

## Verification
Before marking any task done, run in order:
1. `bazel build //...` (or the specific target)
2. `bazel test <relevant targets>`
3. `bazel run //tools:clang_format` — must produce no diff

## Don't
- Don't open PRs against `main` — always target `dev`
- Don't manually install C++ deps — Bazel manages them via `MODULE.bazel`

## Commit messages
Format: `area[/detail]: short description` (≤72 chars, imperative mood, lowercase after colon, no period)

Check area style first: `git log --oneline --no-merges -- <path> | head -20`

Body: explain the "why" and non-obvious "what" in 1–2 lines. Don't reference GitHub issues or Jira tickets.


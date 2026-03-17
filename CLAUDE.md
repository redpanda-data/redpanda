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

## Architecture

Redpanda is sharded and thread-per-core. Each CPU core runs one Seastar reactor
shard. Cross-shard calls use `peering_sharded_service::invoke_on(shard, fn)` —
never shared memory.

Write path:
```
Kafka client → src/v/kafka/   (protocol parsing, request routing)
             → src/v/raft/    (consensus, replication to followers)
             → src/v/storage/ (log segment append, fsync)
```

Key subsystems:
```
src/v/raft/          HIGH RISK — consensus; bugs cause data loss or split-brain
src/v/storage/       HIGH RISK — log segments; bugs corrupt persistent data
src/v/cluster/       partition leadership, membership, metadata coordination
src/v/kafka/         Kafka protocol translation; no persistent state of its own
src/v/cloud_storage/ tiered storage (S3/GCS offload)
src/v/ssx/           Seastar extensions (futures, semaphores, background tasks)
src/v/container/     chunked_vector, chunked_hash_map
src/go/rpk/          CLI; no broker-side state
```

## Don't
- Don't open PRs against `main` — always target `dev`
- Don't manually install C++ deps — Bazel manages them via `MODULE.bazel`

## Commit messages
Format: `area[/detail]: short description` (≤72 chars, imperative mood, lowercase after colon, no period)

Check area style first: `git log --oneline --no-merges -- <path> | head -20`

Body: explain the "why" and non-obvious "what" in 1–2 lines. Don't reference GitHub issues or Jira tickets.


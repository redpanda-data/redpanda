# KIP-848 Implementation Plan

> Each phase defines a **goal** and a **done condition** — a testable, observable outcome.
> Ralph determines the implementation steps by exploring the codebase. Mark items `[x]`
> only when the done condition is fully met. Run `bazel build //src/v/kafka/...` to verify
> the build is green before marking any phase complete.

---

## Phase 1 — Protocol Scaffolding

**Goal:** Redpanda compiles with `ConsumerGroupHeartbeat` (API key 68) registered as a
supported API, returning `UNSUPPORTED_VERSION` to any client that calls it. No logic yet.

**Done condition:**
- `bazel build //src/v/kafka/...` passes with no errors or warnings
- `bazel run //tools:clang_format` shows no diffs
- Running `grep -r "consumer_group_heartbeat" src/v/kafka/` shows entries in:
  `schemata/generator.bzl`, `protocol/messages.h`, `server/handlers/handlers.h`,
  and the new handler `.h` and `.cc` files

**Files to create/modify** (explore the codebase to find exact patterns before editing):
- `src/v/kafka/protocol/schemata/consumer_group_heartbeat_request.json`
  — Use the upstream Kafka reference schema (fetch it as described in PROMPT.md)
  — Match the JSON format of `join_group_request.json`
- `src/v/kafka/protocol/schemata/consumer_group_heartbeat_response.json`
  — Same: use the upstream Kafka reference schema
- `src/v/kafka/protocol/schemata/generator.bzl` — add `"consumer_group_heartbeat"` to `MESSAGES`
- `src/v/kafka/protocol/messages.h` — add include + add `consumer_group_heartbeat_api` to its
  `request_types` (this file has its own `request_types` separate from `handlers.h`)
- `src/v/kafka/server/handlers/consumer_group_heartbeat.h` — type alias only
- `src/v/kafka/server/handlers/consumer_group_heartbeat.cc` — stub `handle()` implementation;
  read `find_coordinator.cc` for the exact template specialization pattern
- `src/v/kafka/server/handlers/handlers.h` — add include + register in `request_types`
- `src/v/kafka/server/BUILD` — add the new `.cc` to the server library's `srcs`

**Commit:** `kafka/protocol: add ConsumerGroupHeartbeat (api key 68) stub`

- [ ] Phase 1 complete

---

## CHECKPOINT — Halt for Human Review

**Ralph: stop here. Do not proceed to Phase 2 until this checkpoint is cleared.**

A human needs to verify:
1. Schema field names in `consumer_group_heartbeat_request.json` and `_response.json`
   match the upstream Kafka reference (compare against the files fetched from GitHub)
2. The new files follow Redpanda code style and project conventions
3. The build is genuinely green with no suppressed warnings

To clear this checkpoint, the human will replace this block with:
```
## CHECKPOINT — Cleared
```

- [ ] Checkpoint cleared by human reviewer

---

## Phase 2 — Feature Flag

**Goal:** The `ConsumerGroupHeartbeat` handler is gated behind a runtime feature flag.
When the flag is off (default), the API returns `UNSUPPORTED_VERSION`. When on, it returns
a stub response AND emits a log line that proves the new code path executed.

**Done condition:**
- `bazel build //src/v/kafka/...` green
- With flag **off**: handler returns `UNSUPPORTED_VERSION`
- With flag **on**: the server log contains `kip848:` when a heartbeat is processed
  (add a `vlog(klog.debug, "kip848: ...")` trace that fires only when the flag is enabled)

**Implementation guidance:**
- Grep `src/v/config/configuration.h` for existing `enable_` flags to find the exact
  pattern (property type, macro, default value, how to read it with
  `config::shard_local_cfg()`)
- The flag logic belongs in `consumer_group_heartbeat.cc`, not the `.h`

**Commit:** `kafka/server: gate KIP-848 heartbeat behind feature flag`

- [ ] Phase 2 complete

---

## Phase 3 — In-Memory State Machine

**Goal:** A `nextgen_coordinator` class (in namespace `kafka::nextgen`, under
`src/v/kafka/server/nextgen/`) handles heartbeats with real member lifecycle logic:
member registration, epoch tracking, and fencing. The coordinator is reachable from
the `consumer_group_heartbeat` handler. All logic is proven by passing unit tests.

**Done condition (tests must pass — this is the primary verifier):**

```
bazel test //src/v/kafka/server/nextgen/tests:coordinator_test
```

The test suite must cover:
1. **New member registration:** A heartbeat with `member_epoch = -1` (new member sentinel)
   returns a response with a freshly assigned `member_id` and `member_epoch = 0`.
   State transitions to `RECONCILING`.
2. **Epoch advancement:** The same member sending a subsequent heartbeat with `member_epoch = 0`
   returns `member_epoch = 1`. State advances toward `STABLE`.
3. **Epoch fencing:** A heartbeat with a stale `member_epoch` returns
   `error_code::fenced_member_epoch`.
4. **Unknown member:** A heartbeat with a `member_id` that doesn't exist in the group
   returns `error_code::unknown_member_id`.

**Implementation guidance:**
- Create all new files under `src/v/kafka/server/nextgen/` with their own `BUILD` file
- Explore how other sharded services are exposed via `request_context` — grep
  `src/v/kafka/server/request_context.h` and `server.h` to understand the pattern
- The coordinator should run as `ss::sharded<nextgen_coordinator>` on each shard
- Log every state transition: `vlog(klog.info, "kip848: group {} {} -> {}", ...)`
- Unit tests should use Seastar's test fixture (`seastar::testing::seastar_test`);
  look at existing btests in `src/v/kafka/server/tests/` for the exact BUILD target type
  and fixture pattern

**Commit:** `kafka/server/nextgen: add KIP-848 in-memory group coordinator with tests`

- [ ] Phase 3 complete

---

## Phase 4 — Observability (Low Priority)

**Goal:** Production-observable metrics for the new code path.

**Done condition:**
- `bazel build //src/v/kafka/...` green
- Prometheus metrics are registered and visible in the metrics endpoint:
  `kafka_nextgen_heartbeat_total` and `kafka_nextgen_group_state_transitions_total`

**Commit:** `kafka/server/nextgen: add Prometheus metrics for KIP-848`

- [ ] Phase 4 complete

---

## Pre-PR Checklist

Before opening a pull request, verify all of the following:

- [ ] `bazel build //src/v/kafka/...` green
- [ ] `bazel test //src/v/kafka/server/nextgen/tests/...` green
- [ ] `bazel run //tools:clang_format` shows no diffs
- [ ] `git diff dev -- 'src/v/kafka/server/group*'` shows no changes (isolation preserved)
- [ ] `git log --oneline dev..HEAD` shows one commit per phase with clean messages

---

## Blockers
_(Ralph: if stuck for more than 2 attempts, record the error and what was tried here, then halt)_

---

## Completed
- [x] Project enabled for Ralph
- [x] Architectural boundary defined: strict isolation, no changes to `group*` files
- [x] Ralph tool access configured: Glob, Grep, WebFetch, Bazel build/test
- [x] Implementation plan restructured around testable goals

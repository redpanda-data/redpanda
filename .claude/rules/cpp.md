---
paths:
  - "src/v/**/*.{h,cc}"
  - "src/transform-sdk/cpp/**/*.{h,cc}"
---

# C++ Conventions

## Seastar patterns
- Prefix Seastar types with `ss::` (e.g. `ss::future`, `ss::promise`, `ss::logger`)
- Assertions: `vassert(cond, msg, args...)` — always enabled, logs message. Use `dassert` for debug-only.
- Logging: `vlog(logger.level, fmt, args...)` e.g. `vlog(stlog.info, "msg {}", val)` where `stlog` is `ss::logger stlog("storage")`

## Coroutine / lambda capture safety
Lambda coroutines passed to `seastar::future::then()` risk use-after-free: the lambda object is
freed when the continuation returns, but the coroutine frame may still reference its captures.

Fix: use C++23 "deducing this" — `[captures...](this auto, args...)` — which moves captures into
the coroutine frame. Required for memory safety, not just style.

## Don't
- Don't use `std::vector` for large containers — use `chunked_vector`
- Don't use `std::unordered_map` for large containers — use `chunked_hash_map`
- Don't declare `operator<<(ostream&, T)` — use a `format_to` member (see `src/v/base/format_to.h`)
- Don't use `ss::parallel_for_each` on large ranges (partitions, topics, segments) — use `ss::max_concurrent_for_each`
- Don't call `.get_exception()` inside log/assert args — assign to a variable first

## Naming & style
- snake_case uniformly — identifiers, class names, namespaces, file names
- `/// \brief` Doxygen on public types
- Map strings to values with `string_switch` (`src/v/strings/string_switch.h`), not if-else chains

## Comments
Default to no comments. Add for: non-obvious algorithms, gotchas, wire protocol mappings,
ASCII state diagrams, and links to external specs. Don't restate what the code says.

## Benchmarks
```bash
bazel run --config=release //src/v/utils/tests:coro_rpbench -- --help
```
See also: `external/+non_module_dependencies+seastar/tests/perf/perf-tests.md`

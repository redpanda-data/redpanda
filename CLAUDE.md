# Copilot Coding Agent Onboarding Guide for `redpanda-data/redpanda`

## High-Level Overview

**What is Redpanda?**

Redpanda is a high-performance, Apache Kafka®-compatible streaming data platform. It is written primarily in C++ for the core, with Go for CLI tooling (`rpk`), and some Python for auxiliary scripts and tests. Redpanda is designed to be lightweight, fast, and simple to operate, omitting ZooKeeper and the JVM.

It uses extensively the thread-per-core model and asynchronous (coroutines, futures) programming model.

**Repository Characteristics:**
- **Large multi-language codebase:** C++ (core), Go (CLI/tools), Python (testing/scripts), Bash and Bazel for builds.
- **Build System:** Bazel (with Bazelisk) for core.
- **Target Platforms:** Linux (primary), some support for macOS and Windows.
- **Key Directories:**
  - `src/v/`: Core C++ source code
  - `src/go/`: Go CLI and tools
  - `bazel/`, `BUILD`, `MODULE.bazel`: Bazel scripts and definitions
  - `tools/`: Development and helper scripts
  - `tests/`: Test suites
  - `conf/`: Configuration files
  - `proto/`: Protobuf definitions for Redpanda services and APIs
  - `.github/`, `.buildkite/`: CI/workflow automation
- **Documentation:** [Docs site](https://redpanda.com/documentation) and `docs/`.

## Build, Test, and Validation Instructions

### Prerequisites (Always Perform)

1. **Install Bazelisk** (the required Bazel wrapper):
   ```bash
   wget -O ~/bin/bazel https://github.com/bazelbuild/bazelisk/releases/latest/download/bazelisk-linux-amd64
   chmod +x ~/bin/bazel
   export PATH="$HOME/bin:$PATH"
   ```
2. **Install system dependencies**:
   ```bash
   sudo ./bazel/install-deps.sh
   ```
   _This must always be run before any Bazel build, especially on a fresh system or after dependency updates._

### Core Build Steps

- **Build all (fastbuild):**
  ```bash
  bazel build //...
  ```
- **Test all:**
  ```bash
  bazel test //...
  ```
- **Lint (C++):**
  - Formatting and linting are enforced. Use:
    ```bash
    bazel run //tools:clang_format
    ```
  - Configs: `.clang-format`, `.clang-tidy`, etc.

- **Go CLI (`rpk`) Build:**
  ```bash
  bazel build //:rpk
  ```

See `.bazelrc` for more details on build settings and config modes

### Validation and CI

- **Pre-push/merge:** All changes are validated by CI (GitHub Actions, Buildkite) for build, test, and lint.
- **Formatting and lint checks are enforced; run locally before PRs.**
- **Target branch:** Always open PRs against `dev`.

## Project Layout & Architectural Notes

- **Main C++ source:** `src/v/`
- **Go CLI:** `src/go/rpk/`
- **Build configuration:** `.bazelrc`, `.bazelversion`, `BUILD`, `MODULE.bazel`, and `bazel/`
- **CI configuration:** `.github/workflows/`, `.buildkite/`
- **Testing:** `tests/`
- **Config:** `conf/`
- **Docker:** `tools/docker/`

### Lint/Formatting Configs:
- `.clang-format`, `.clang-tidy*`: C++ style
- `.style.yapf`, `.yapfignore`: Python formatting

### CI/CD Checks

- **Build, test, and lint are enforced by CI.** Use the same steps as above locally before PRs.

### File Index (Root Level)
- `.bazelignore`, `.bazelrc`, `.bazelversion`, `BUILD`, `MODULE.bazel`, `README.md`, `CONTRIBUTING.md`, `SECURITY.md`, `CODE_OF_CONDUCT.md`, `LICENSES/`, `bazel/`, `src/`, `tests/`, `conf/`, `tools/`, `.github/`, `.buildkite/`, etc.

---

## Protobuf-Specific Instructions

### Protobuf Coding Guidelines

Follow the guidelines provided in `proto/redpanda/README.md` for basic Protobuf standards.

### Protobuf Build & Environment
- **Primary Protobuf code lives in `proto/`.**
- **Formatting:** `.clang-format` in the root directory is enforced. Always run `clang-format` before committing changes or submitting a PR:
  ```bash
  bazel run //tools:clang_format
  ```

## C++-Specific Instructions

### C++ Build & Environment

- **Primary C++ code lives in `src/v/`.**
- **C++ build is managed by Bazel.** All dependencies and toolchains are configured via Bazel rules and the `MODULE.bazel` file. Do not manually install C++ dependencies unless explicitly instructed in documentation.
- **Compiler Standard:** C++23 is required. Some SDK components (e.g., `src/transform-sdk/cpp/`) use C++23 and specific flags like `-Wall`, `-fno-exceptions`, and for some targets, `-stdlib=libc++`.
- **Sanitizers:** Some components and test builds use sanitizers (address, leak, undefined) via `-fsanitize=address,leak,undefined` for both compile and link.
- **Suppression Files:** Leak, undefined, and other sanitizer suppressions can be found in the root as `lsan_suppressions.txt`, `ubsan_suppressions.txt`.
- **C++ Linting:** `.clang-format` and `.clang-tidy` in the root directory are enforced. Always run `clang-format` before committing changes or submitting a PR:
  ```bash
  bazel run //tools:clang_format
  ```
- **C++ Libraries:** Bazel dependencies are managed in `MODULE.bazel` (e.g., Boost, Abseil, fmt, protobuf, googletest, yaml-cpp, etc.).
- **Testing:** C++ unit tests are run via Bazel.

### Common C++ Pitfalls & Workarounds

- **Always use Bazelisk and Bazel for building the core.** Using a plain Bazel binary may result in missing dependencies or incompatible flags.
- **If you encounter build issues related to missing system libraries, rerun `sudo ./bazel/install-deps.sh`.**
- **Do not attempt to manually install or update C++ dependencies unless specifically instructed.**
- **Always run lint and formatter before pushing. CI will fail on formatting/lint discrepancies.**
- **If building in CI or a containerized environment, ensure the correct toolchain is available as specified in `tools/docker/README.md` or CI scripts.**
- **Check for additional build and compile flags in `BUILD`, `MODULE.bazel` and related files.**

### C++ coding guidelines

Check that these guidelines are followed for new code.

- Do not declare new `operator<<(ostream& os, type)` overloads, instead prefer to use a `format_to` member function inside `type` as described in
src/v/base/format_to.h.
- Prefer using latest C++ features (C++23).
- Use `ss` namespace as a prefix for Seastar types (e.g. `ss::future`, `ss::promise`).
- Use `vassert(cond, msg, msg_args...)` macro for assertions. It is
  similar to `assert(cond)` but it is always enabled and it prints the message
  to the log. Use `dassert` for assertions that are only enabled in debug mode.
- Use `vlog(method, fmt, args...)` for logging. `method` is the method reference
  for the logger to use. I.e. `vlog(stlog.info, "Hello world");`. Where `stlog`
  is defined as `ss::logger stlog("storage");`.
- Do not use `std::vector` for containers that may grow very large, instead use `chunked_vector`.
- Do not use `std::unordered_map` for containers that may grow very large, instead use `chunked_hash_map`.
- Instead of long if-else chains for mapping string to values, use
  `string_switch` mechanism defined in
  [string_switch.h](./src/v/strings/string_switch.h).
- Don't use `ss::parallel_for_each` with ranges that can grow large such as
  partitions, topics or segments. Instead prefer `ss::max_concurrent_for_each` to
  limit concurrency. Think about how much concurrency is needed to adequately hide
  latency. See our
  [docs](https://redpandadata.atlassian.net/wiki/x/AQBZTw#Managing-Concurrency-in-the-system)
  for more background.
- Do not call the `get_exception` method on a future within a logging or
  assertion statement (e.g. `vlog(..., fut.get_exception(), ...)`). Instead,
  assign the return value of `get_exception` in a variable and pass the variable.

#### Lambda coroutines, coroutine argument capture, and deducing this

When a lambda coroutine is passed to APIs like `seastar::future::then()`,
the lambda object is stored in managed memory that gets freed once the
continuation returns—but the coroutine may still be suspended and later
access its captures, causing use-after-free. This happens because the
coroutine frame holds a reference to the lambda's capture storage, which
becomes dangling. The C++23 "deducing this" syntax
`([captures...](this auto, args...))` solves this by moving the captures
directly into the coroutine frame rather than referencing them through the
lambda object, decoupling capture lifetime from the lambda's lifetime.
This is distinct from the recursive-lambda use case—here this auto is required
for memory safety in coroutines, not self-reference.

### C++ coding style

- Use snake_case for identifiers. Use CamelCase for concepts.
- Use Doxygen comments with 3-slashes (///) for public APIs

### Code comments

- **Default to no comments** - clear code and good names are better
- **Avoid comments that restate the code** - they become stale
- **Prefer alternatives:**
  - Better variable/function names
  - Log lines (serve as documentation and debugging)
- **Do add comments for:**
  - Doc comments (`/// \brief`) on public types explaining purpose/usage
  - Complex algorithms or non-obvious "gotchas"
  - Test comments explaining input format or test intent
  - Links to external resources (specs, docs, issues)
  - Mapping internal types/concepts to external formats (e.g., wire protocols, APIs)
  - ASCII diagrams for complex state machines or data flows
- **Avoid obvious branching comments** - `if (x)` rarely needs `// when x is true`

### Benchmarking

  - Run benchmarks like `bazel run --config=release //src/v/utils/tests:coro_rpbench`
  - Get --help like: `bazel run --config=release //src/v/utils/tests:coro_rpbench -- --help`
  - If running/writing benchmarks read this also: external/+non_module_dependencies+seastar/tests/perf/perf-tests.md

### More C++-Specific References

- [MODULE.bazel](https://github.com/redpanda-data/redpanda/blob/dev/MODULE.bazel)
- [BUILD](https://github.com/redpanda-data/redpanda/blob/dev/BUILD)

---

## Python specific instructions

### Instructions for python test code under tests/rptest

- Avoid catching bare `except:` as this can hide system exceptions, including exceptions
  raised by a signal when a test is being forcibly timed out. Instead use `except Exception:`.

### Instructions for type hints

- Use modern style with `|` instead of `Union` or `Optional`

For further details, consult:
- [README.md](https://github.com/redpanda-data/redpanda/blob/dev/README.md)
- [CONTRIBUTING.md](https://github.com/redpanda-data/redpanda/blob/dev/CONTRIBUTING.md)
- [Redpanda Documentation](https://redpanda.com/documentation)
- CI/CD configs in `.github/workflows/` and `.buildkite/`

---

## Commit message instructions

When writing or critiquing commit messages, follow these guidelines:

**Format:**
```
area[/detail]: short description

<optional body>
```

- Title: ≤72 chars
- Body: wrapped at 72 chars

**Check git history first** to match the style of the area you're changing:
```bash
git log --oneline --no-merges -- path/to/changed/files | head -20
```

**Goal:** Make the commit easy to review and provide context for future readers (via `git blame`/`git log`)

**Rules:**
- Title: imperative mood, lowercase after colon, no period
- Keep bodies concise (1-2 lines typical, trivial changes need only a title)
- Don't reference GitHub issues/PRs or Jira tickets
- Use the body to help reviewers and future readers understand:
  - The "why": motivation, design choices, preparation for future work
  - The "what": new abstractions introduced, non-obvious changes
  - Integration tests: briefly note what behaviors are covered
- Don't restate what's obvious from the diff, but duplicating a doc comment is fine if it helps reviewers understand the change faster.

_Results from code search may be incomplete. For more C++ details, see the [repository code search](https://github.com/redpanda-data/redpanda/search?q=c%2B%2B)._

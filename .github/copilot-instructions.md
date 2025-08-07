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

- **Build all (Release Mode):**
  ```bash
  bazel build --config=release //...
  ```
- **Test all:**
  ```bash
  bazel test --config=release //...
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
  _Requires Go toolchain._

- **Alternative (Docker Toolchain):**
  See `tools/docker/README.md`. Example:
  ```bash
  docker run --rm -ti -v $PWD:$PWD:Z -w $PWD vectorized/redpanda-toolchain ./build.sh
  ```

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

## C++-Specific Instructions

### C++ Build & Environment

- **Primary C++ code lives in `src/v/`.**
- **C++ build is managed by Bazel.** All dependencies and toolchains are configured via Bazel rules and the `MODULE.bazel` file. Do not manually install C++ dependencies unless explicitly instructed in documentation.
- **Compiler Standard:** C++23 is required. Some SDK components (e.g., `src/transform-sdk/cpp/`) use C++23 and specific flags like `-Wall`, `-fno-exceptions`, and for some targets, `-stdlib=libc++`.
- **Sanitizers:** Some components and test builds use sanitizers (address, leak, undefined) via `-fsanitize=address,leak,undefined` for both compile and link.
- **Suppression Files:** Leak, undefined, and other sanitizer suppressions can be found in the root as `lsan_suppressions.txt`, `ubsan_suppressions.txt`.
- **C++ Linting:** `.clang-format` and `.clang-tidy` in the root directory are enforced. Always run formatting tools before submitting a PR:
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

### C++ coding style

- Use snake_case for identifiers. Use CamelCase for concepts.
- Use Doxygen comments with 3-slashes (///) for public APIs

### More C++-Specific References

- [MODULE.bazel](https://github.com/redpanda-data/redpanda/blob/dev/MODULE.bazel)
- [BUILD](https://github.com/redpanda-data/redpanda/blob/dev/BUILD)

---

For further details, consult:
- [README.md](https://github.com/redpanda-data/redpanda/blob/dev/README.md)
- [CONTRIBUTING.md](https://github.com/redpanda-data/redpanda/blob/dev/CONTRIBUTING.md)
- [Redpanda Documentation](https://redpanda.com/documentation)
- CI/CD configs in `.github/workflows/` and `.buildkite/`

---

_Results from code search may be incomplete. For more C++ details, see the [repository code search](https://github.com/redpanda-data/redpanda/search?q=c%2B%2B)._

# Antithesis Test Packaging

Package Redpanda C++ unit tests into Docker images compatible with
[Antithesis](https://antithesis.com) testing.

## Quick Start

```bash
# Package a test (non-instrumented):
./tools/antithesis/single_binary_test_package.py //src/v/lsm/db/tests:db_bench \
    --binary-args='--smp 2 --memory 1G --num 100000 --benchmarks mixedworkload --verify'

# Package with coverage instrumentation:
./tools/antithesis/single_binary_test_package.py //src/v/lsm/db/tests:db_bench --instrumented \
    --binary-args='--smp 2 --memory 1G --num 100000 --benchmarks mixedworkload --verify'

# Run locally:
docker compose -f bazel-bin/.antithesis/db_bench/docker-compose.yaml up -d
docker compose -f bazel-bin/.antithesis/db_bench/docker-compose.yaml exec workload \
    /opt/antithesis/test/v1/quickstart/singleton_driver_db_bench.sh
docker compose -f bazel-bin/.antithesis/db_bench/docker-compose.yaml down

# Validate an image:
./tools/antithesis/single_binary_test_validate.py db_bench
./tools/antithesis/single_binary_test_validate.py db_bench --instrumented
```

The script works with any `cc_test` or `cc_binary` target. For `cc_test`
targets, Bazel-configured args (--smp, --memory, log levels) are extracted
automatically. For `cc_binary` targets, base Seastar flags are used.

## Build Modes

### Default (no instrumentation)

```bash
./tools/antithesis/single_binary_test_package.py //src/v/lsm/db/tests:db_bench
```

Packages the binary as-is. Suitable for initial Antithesis onboarding or
when coverage instrumentation is not needed.

### Instrumented (`--config=antithesis`)

```bash
./tools/antithesis/single_binary_test_package.py //src/v/lsm/db/tests:db_bench --instrumented
# or equivalently:
./tools/antithesis/single_binary_test_package.py //src/v/lsm/db/tests:db_bench --bazel-args='--config=antithesis'
```

Enables coverage instrumentation that Antithesis uses for guided state-space
exploration. This config adds:

- `-fsanitize-coverage=trace-pc-guard` — LLVM coverage callbacks (scoped to `src/v/`)
- `-g --strip=never` — debug symbols for Antithesis symbolization
- `-Wl,--build-id` — GNU build IDs for symbolization
- `--compilation_mode=opt` — optimized build
- Antithesis SDK instrumentation hooks linked into every `redpanda_cc_binary`,
  `redpanda_cc_gtest`, `redpanda_cc_btest`, and `redpanda_cc_bench` target

At runtime, the instrumentation hooks attempt to load `libvoidstar.so` via
`dlopen`. Inside the Antithesis environment this library is injected and
provides coverage feedback. Outside Antithesis, the hooks are no-ops.

## Images

The packaging script produces two Docker images:

- **Workload image** (`<name>:latest`) — contains the binary, shared libs,
  entrypoint, and singleton driver
- **Config image** (`<name>-config:latest`) — FROM scratch, contains only
  `docker-compose.yaml` at `/`. Antithesis reads this to orchestrate the workload.

## Workload Image Layout

```
/usr/bin/entrypoint.sh                                  # emits setup_complete, then sleeps
/opt/antithesis/bin/<binary>                             # the test binary
/opt/antithesis/lib/                                     # shared libraries
/opt/antithesis/test/v1/quickstart/singleton_driver_*.sh # runs the test
/symbols/<binary>                                        # symlink for Antithesis symbolization
```

The singleton driver sets `LD_LIBRARY_PATH=/opt/antithesis/lib` to ensure
the bundled shared libraries are used regardless of the host environment.

## Files

| File | Purpose |
|------|---------|
| `tools/antithesis/single_binary_test_package.py` | CLI script that generates a BUILD and invokes the macro |
| `tools/antithesis/single_binary_test_validate.py` | CLI script that validates a packaged image locally |
| `bazel/antithesis/antithesis.bzl` | Reusable Bazel macro (`antithesis_test_image`) |
| `bazel/antithesis/BUILD` | Coverage hook library (`instrumentation`, linked via `select`) |
| `bazel/antithesis/antithesis_instrumentation.cc` | Includes `antithesis_instrumentation.h` from the SDK |
| `bazel/thirdparty/antithesis_sdk.BUILD` | BUILD file for the Antithesis C++ SDK dependency |
| `src/v/base/invariants.h` | Framework-agnostic runtime invariant macros (Antithesis/sanitizer/vassert) |
| `.bazelrc` (`config:antithesis`) | Compiler/linker flags for instrumented builds |
| `bazel/BUILD` (`antithesis` flag + config_setting) | Build flag for `--config=antithesis` |
| `bazel/build.bzl` | Injects instrumentation dep into `redpanda_cc_binary` |
| `bazel/test.bzl` | Injects instrumentation dep into test/bench macros |
| `MODULE.bazel` | `http_archive` for `antithesis-sdk-cpp` v0.4.7 |

## Script Options

### single_binary_test_package.py

```
./tools/antithesis/single_binary_test_package.py TARGET [OPTIONS]

positional arguments:
  TARGET                Bazel label (e.g. //src/v/lsm/db/tests:db_bench)

options:
  --binary-args ARGS    Extra arguments passed to the binary at runtime
  --name NAME           Custom target name (default: derived from Bazel label)
  --tag TAG             Docker image tag (default: <name>:latest)
  --bazel-args ARGS     Extra arguments passed to bazel build
  --instrumented        Build with --config=antithesis for coverage instrumentation
```

### single_binary_test_validate.py

```
./tools/antithesis/single_binary_test_validate.py TARGET_NAME [OPTIONS]

positional arguments:
  TARGET_NAME           Target name (e.g. db_bench)

options:
  --tag TAG             Docker image tag (default: <target_name>:latest)
  --instrumented        Also validate instrumentation symbols
```

## Pushing to Antithesis

```bash
TENANT=<your-tenant-name>
REGISTRY=us-central1-docker.pkg.dev/molten-verve-216720/$TENANT-repository
docker tag db_bench:latest $REGISTRY/db_bench:latest
docker push $REGISTRY/db_bench:latest
docker tag db_bench-config:latest $REGISTRY/db_bench-config:latest
docker push $REGISTRY/db_bench-config:latest
```

## Antithesis Requirements Checklist

- [x] `setup_complete` event emitted via `$ANTITHESIS_OUTPUT_DIR/sdk.jsonl`
- [x] Singleton driver at `/opt/antithesis/test/v1/quickstart/`
- [x] `container_name` == `hostname` (no underscores)
- [x] `init: true` in docker-compose
- [x] Config image with `docker-compose.yaml` at `/`
- [x] No custom logging, pull policies, or internet access assumptions
- [x] `/symbols` directory for symbolization
- [x] GNU build IDs (`--build-id`) when instrumented
- [x] `LD_LIBRARY_PATH` set for bundled shared library compatibility

load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "logger",
    hdrs = ["wasm_logger.h"],
    include_prefix = "wasm/tests",
    deps = [
        "//src/v/base",
        "//src/v/wasm:wasi_logger",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "wasm_fixture",
    srcs = ["wasm_fixture.cc"],
    hdrs = ["wasm_fixture.h"],
    include_prefix = "wasm/tests",
    deps = [
        ":logger",
        "//src/v/base",
        "//src/v/model/tests:random",
        "//src/v/schema:registry",
        "//src/v/schema/tests:fake_registry",
        "//src/v/test_utils:gtest",
        "//src/v/wasm:api",
        "//src/v/wasm:wasmtime",
        "@abseil-cpp//absl/strings",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "wasm_allocator_test",
    timeout = "short",
    srcs = [
        "wasm_allocator_test.cc",
    ],
    cpu = 1,
    memory = "64MiB",
    deps = [
        "//src/v/base",
        "//src/v/test_utils:gtest",
        "//src/v/wasm:allocator",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "ffi_helpers_test",
    timeout = "short",
    srcs = [
        "ffi_helpers_test.cc",
    ],
    cpu = 1,
    memory = "32MiB",
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:random",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "//src/v/wasm:ffi",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "wasi_test",
    timeout = "moderate",
    srcs = [
        "wasi_test.cc",
    ],
    cpu = 1,
    data = [
        "//src/transform-sdk/go/transform/internal/testdata/wasi",
    ],
    env = {
        "WASI_WASM_BINARY": "$(rootpath //src/transform-sdk/go/transform/internal/testdata/wasi)",
    },
    memory = "128MiB",
    deps = [
        ":wasm_fixture",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/json",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "//src/v/wasm:wasi",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "wasm_transform_test",
    timeout = "moderate",
    srcs = [
        "wasm_transform_test.cc",
    ],
    cpu = 1,
    data = [
        "//src/transform-sdk/go/transform/internal/testdata/dynamic",
        "//src/transform-sdk/go/transform/internal/testdata/identity",
    ],
    env = {
        "IDENTITY_WASM_BINARY": "$(rootpath //src/transform-sdk/go/transform/internal/testdata/identity)",
        "DYNAMIC_WASM_BINARY": "$(rootpath //src/transform-sdk/go/transform/internal/testdata/dynamic)",
    },
    memory = "128MiB",
    deps = [
        ":wasm_fixture",
        "//src/v/base",
        "//src/v/test_utils:gtest",
        "//src/v/wasm:api",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "wasm_sr_test",
    timeout = "moderate",
    srcs = [
        "wasm_sr_test.cc",
    ],
    cpu = 1,
    data = [
        "//src/transform-sdk/go/transform/internal/testdata/schema-registry",
    ],
    env = {
        "SCHEMA-REGISTRY_WASM_BINARY": "$(rootpath //src/transform-sdk/go/transform/internal/testdata/schema-registry)",
    },
    memory = "128MiB",
    deps = [
        ":wasm_fixture",
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:streambuf",
        "//src/v/model",
        "//src/v/pandaproxy",
        "//src/v/test_utils:gtest",
        "//src/v/wasm:api",
        "@avro",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "wasm_lifecycle_test",
    timeout = "moderate",
    srcs = [
        "wasm_lifecycle_test.cc",
    ],
    cpu = 1,
    data = [
        "//src/transform-sdk/go/transform/internal/testdata/identity",
        "//src/transform-sdk/go/transform/internal/testdata/setup-panic",
        "//src/transform-sdk/go/transform/internal/testdata/transform-error",
        "//src/transform-sdk/go/transform/internal/testdata/transform-panic",
    ],
    env = {
        "IDENTITY_WASM_BINARY": "$(rootpath //src/transform-sdk/go/transform/internal/testdata/identity)",
        "SETUP-PANIC_WASM_BINARY": "$(rootpath //src/transform-sdk/go/transform/internal/testdata/setup-panic)",
        "TRANSFORM-PANIC_WASM_BINARY": "$(rootpath //src/transform-sdk/go/transform/internal/testdata/transform-panic)",
        "TRANSFORM-ERROR_WASM_BINARY": "$(rootpath //src/transform-sdk/go/transform/internal/testdata/transform-error)",
    },
    memory = "128MiB",
    deps = [
        ":wasm_fixture",
        "//src/v/base",
        "//src/v/pandaproxy",
        "//src/v/test_utils:gtest",
        "//src/v/wasm:api",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "wasm_resources_test",
    timeout = "moderate",
    srcs = [
        "wasm_resources_test.cc",
    ],
    cpu = 1,
    data = [
        "//src/transform-sdk/go/transform/internal/testdata/dynamic",
    ],
    env = {
        "DYNAMIC_WASM_BINARY": "$(rootpath //src/transform-sdk/go/transform/internal/testdata/dynamic)",
    },
    memory = "128MiB",
    deps = [
        ":wasm_fixture",
        "//src/v/base",
        "//src/v/test_utils:gtest",
        "//src/v/wasm:api",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "wasm_cache_test",
    timeout = "short",
    srcs = [
        "wasm_cache_test.cc",
    ],
    cpu = 2,
    memory = "64MiB",
    deps = [
        "//src/v/base",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "//src/v/wasm:api",
        "//src/v/wasm:cache",
        "//src/v/wasm:wasi_logger",
        "@abseil-cpp//absl/algorithm:container",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "wasm_probe_test",
    timeout = "short",
    srcs = [
        "wasm_probe_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/metrics",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "//src/v/wasm:engine_probe",
        "//src/v/wasm:logger",
        "@abseil-cpp//absl/algorithm:container",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_bench(
    name = "transform_bench",
    timeout = "moderate",
    srcs = [
        "wasm_transform_bench.cc",
    ],
    cpu = 1,
    data = [
        "//src/transform-sdk/go/transform/internal/testdata/identity",
    ],
    env = {
        "IDENTITY_WASM_BINARY": "$(rootpath //src/transform-sdk/go/transform/internal/testdata/identity)",
    },
    memory = "512MiB",
    deps = [
        ":logger",
        "//src/v/base",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/schema:registry",
        "//src/v/test_utils:random",
        "//src/v/wasm:api",
        "//src/v/wasm:logger",
        "//src/v/wasm:transform_probe",
        "//src/v/wasm:wasmtime",
        "@abseil-cpp//absl/strings",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

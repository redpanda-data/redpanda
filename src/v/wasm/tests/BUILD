load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

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
        "//src/v/test_utils:gtest",
        "//src/v/wasm:api",
        "//src/v/wasm:wasmtime",
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

# TODO(rockwood): Need to hook tinygo into the build for this to work
# redpanda_cc_gtest(
#     name = "wasi_test",
#     timeout = "short",
#     srcs = [
#         "wasi_test.cc",
#     ],
#     deps = [
#         "//src/v/base",
#         "//src/v/bytes:iobuf",
#         "//src/v/bytes:iobuf_parser",
#         "//src/v/json",
#         "//src/v/model",
#         "//src/v/test_utils:gtest",
#         "//src/v/wasm:wasi",
#         ":wasm_fixture",
#         "@googletest//:gtest",
#         "@abseil-cpp//absl/container:flat_hash_set",
#         "@seastar",
#     ],
# )

redpanda_cc_gtest(
    name = "wasm_cache_test",
    timeout = "short",
    srcs = [
        "wasm_cache_test.cc",
    ],
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

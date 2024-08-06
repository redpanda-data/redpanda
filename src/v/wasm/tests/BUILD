load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "wasm_allocator_test",
    timeout = "short",
    srcs = [
        "wasm_allocator_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/test_utils:gtest",
        "//src/v/wasm",
        "//src/v/wasm:allocator",
        "@googletest//:gtest",
        "@seastar",
    ],
)

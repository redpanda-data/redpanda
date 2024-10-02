load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "fake_registry",
    srcs = [
        "fake_registry.cc",
    ],
    hdrs = [
        "fake_registry.h",
    ],
    include_prefix = "schema/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/pandaproxy",
        "//src/v/schema:registry",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "schema_utils_test",
    timeout = "short",
    srcs = [
        "utils_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/schema:utils",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

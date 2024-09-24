load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "types_test",
    timeout = "short",
    srcs = [
        "types_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics:types",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

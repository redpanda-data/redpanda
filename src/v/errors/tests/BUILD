load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "errors_test",
    timeout = "short",
    srcs = [
        "errors_test.cc",
    ],
    deps = [
        "//src/v/errors",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

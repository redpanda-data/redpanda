load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "gtest_test",
    timeout = "short",
    srcs = [
        "gtest_test.cc",
    ],
    deps = [
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

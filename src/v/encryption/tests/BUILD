load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "mock_kms_test",
    timeout = "short",
    srcs = [
        "mock_kms_test.cc",
    ],
    deps = [
        "//src/v/encryption",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

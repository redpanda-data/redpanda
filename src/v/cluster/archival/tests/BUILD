load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "archival_policy_test",
    timeout = "short",
    srcs = [
        "archival_policy_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/storage",
        "//src/v/storage/tests:disk_log_builder",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:seastar_boost",
        "@googletest//:gtest",
        "@seastar",
    ],
)

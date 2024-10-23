load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "throttler_test",
    timeout = "short",
    srcs = [
        "throttler_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics/core:pipeline_stage",
        "//src/v/cloud_topics/core:write_pipeline",
        "//src/v/cloud_topics/core:write_request",
        "//src/v/cloud_topics/throttler",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

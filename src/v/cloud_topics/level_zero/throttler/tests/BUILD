load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "throttler_test",
    timeout = "short",
    srcs = [
        "throttler_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/cloud_topics/level_zero/pipeline:pipeline_stage",
        "//src/v/cloud_topics/level_zero/pipeline:write_pipeline",
        "//src/v/cloud_topics/level_zero/pipeline:write_request",
        "//src/v/cloud_topics/level_zero/throttler",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

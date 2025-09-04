load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "write_request_scheduler_test",
    timeout = "short",
    srcs = [
        "write_request_scheduler_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics/level_zero/pipeline:event_filter",
        "//src/v/cloud_topics/level_zero/pipeline:pipeline_stage",
        "//src/v/cloud_topics/level_zero/pipeline:write_pipeline",
        "//src/v/cloud_topics/level_zero/pipeline:write_request",
        "//src/v/cloud_topics/level_zero/write_request_scheduler",
        "//src/v/config",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "l0_object_size_distribution_test",
    timeout = "moderate",
    srcs = [
        "l0_object_size_distribution_test.cc",
    ],
    cpu = 3,
    deps = [
        "//src/v/base",
        "//src/v/cloud_io:remote_api",
        "//src/v/cloud_topics/level_zero/batcher",
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

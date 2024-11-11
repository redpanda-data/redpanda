load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "write_request_balancer_test",
    timeout = "short",
    srcs = [
        "write_request_balancer_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics/core:event_filter",
        "//src/v/cloud_topics/core:pipeline_stage",
        "//src/v/cloud_topics/core:write_pipeline",
        "//src/v/cloud_topics/core:write_request",
        "//src/v/cloud_topics/request_balancer:write_request_balancer",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

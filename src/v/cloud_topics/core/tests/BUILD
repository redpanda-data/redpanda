load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "write_request_test",
    timeout = "short",
    srcs = [
        "write_request_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/core:serializer",
        "//src/v/cloud_topics/core:write_request",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "serializer_test",
    timeout = "short",
    srcs = [
        "serializer_test.cc",
    ],
    deps = [
        "//src/v/bytes",
        "//src/v/cloud_topics/core:serializer",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "write_pipeline_test",
    timeout = "short",
    srcs = [
        "write_pipeline_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/core:write_pipeline",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "event_filter_test",
    timeout = "short",
    srcs = [
        "event_filter_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/core:event_filter",
        "//src/v/cloud_topics/core:pipeline_stage",
        "//src/v/cloud_topics/core:write_pipeline",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

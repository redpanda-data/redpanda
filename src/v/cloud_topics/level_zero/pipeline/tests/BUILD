load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "read_pipeline_test",
    timeout = "short",
    srcs = [
        "read_pipeline_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_zero/pipeline:read_pipeline",
        "//src/v/model",
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
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/cloud_topics/level_zero/pipeline:write_pipeline",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "write_request_test",
    timeout = "short",
    srcs = [
        "write_request_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_zero/pipeline:serializer",
        "//src/v/cloud_topics/level_zero/pipeline:write_request",
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
        "//src/v/cloud_topics/level_zero/pipeline:serializer",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
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
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/cloud_topics/level_zero/pipeline:event_filter",
        "//src/v/cloud_topics/level_zero/pipeline:pipeline_stage",
        "//src/v/cloud_topics/level_zero/pipeline:write_pipeline",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "circuit_breaker_test",
    timeout = "short",
    srcs = [
        "circuit_breaker_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_zero/pipeline:circuit_breaker",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_bench(
    name = "pipeline_rpbench",
    timeout = "short",
    srcs = [
        "pipeline_bench.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics/level_zero/pipeline:event_filter",
        "//src/v/cloud_topics/level_zero/pipeline:read_pipeline",
        "//src/v/cloud_topics/level_zero/pipeline:write_pipeline",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/ssx:future_util",
        "//src/v/storage",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

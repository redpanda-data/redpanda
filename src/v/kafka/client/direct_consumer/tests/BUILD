load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_cc_gtest(
    name = "data_queue_test",
    timeout = "short",
    srcs = [
        "data_queue_test.cc",
    ],
    deps = [
        "//src/v/kafka/client/direct_consumer",
        "//src/v/model/tests:random",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "direct_consumer_test",
    timeout = "short",
    srcs = [
        "direct_consumer_test.cc",
    ],
    deps = [
        "//src/v/kafka/client:brokers",
        "//src/v/kafka/client:cluster",
        "//src/v/kafka/client:types",
        "//src/v/kafka/client/direct_consumer",
        "//src/v/kafka/client/test:cluster_mock",
        "//src/v/kafka/protocol",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "direct_consumer_fixture",
    srcs = [
        "direct_consumer_fixture.cc",
    ],
    hdrs = [
        "direct_consumer_fixture.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cluster/tests:cluster_test_fixture",
        "//src/v/container:chunked_vector",
        "//src/v/kafka/client:brokers",
        "//src/v/kafka/client:cluster",
        "//src/v/kafka/client:types",
        "//src/v/kafka/client/direct_consumer",
        "//src/v/kafka/protocol",
        "//src/v/kafka/server/tests:kafka_test_utils",
        "//src/v/model/tests:random",
        "//src/v/ssx:single_fiber_executor",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "direct_consumer_fixture_test",
    timeout = "moderate",
    srcs = [
        "direct_consumer_fixture_test.cc",
    ],
    cpu = 1,
    deps = [
        ":direct_consumer_fixture",
        "//src/v/model",
        "//src/v/redpanda/tests:fixture",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "fetcher_test",
    timeout = "short",
    srcs = [
        "fetcher_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/container:chunked_vector",
        "//src/v/kafka/client:types",
        "//src/v/kafka/client/direct_consumer",
        "//src/v/kafka/protocol",
        "//src/v/kafka/server/tests:kafka_test_utils",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/test_utils:gtest",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

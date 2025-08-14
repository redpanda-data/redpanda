load("//bazel:test.bzl", "redpanda_cc_gtest")

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
        "//src/v/model/tests:random",
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
    deps = [
        "//src/v/base",
        "//src/v/cluster/tests:cluster_test_fixture",
        "//src/v/container:chunked_vector",
        "//src/v/kafka/client:brokers",
        "//src/v/kafka/client:cluster",
        "//src/v/kafka/client/direct_consumer",
        "//src/v/kafka/protocol",
        "//src/v/kafka/server/tests:kafka_test_utils",
        "//src/v/model",
        "//src/v/redpanda/tests:fixture",
        "//src/v/test_utils:gtest",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

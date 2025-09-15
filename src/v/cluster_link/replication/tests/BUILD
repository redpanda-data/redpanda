load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_cc_gtest(
    name = "partition_replicator_test",
    timeout = "short",
    srcs = [
        "partition_replicator_tests.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cluster_link/replication:partition_replicator",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/ssx:future_util",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
        "//src/v/utils:mutex",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "link_replication_mgr_test",
    timeout = "short",
    srcs = [
        "link_replication_mgr_tests.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cluster_link/replication:link_replication_mgr",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "partition_data_queue_test",
    timeout = "short",
    srcs = [
        "partition_data_queue_tests.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cluster_link/replication:partition_data_queue",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "mux_consumer_fixture_test",
    timeout = "short",
    srcs = [
        "mux_consumer_fixture_tests.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cluster_link/replication:mux_remote_consumer",
        "//src/v/kafka/client/direct_consumer/tests:direct_consumer_fixture",
        "//src/v/model",
        "//src/v/redpanda/tests:fixture",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_test_cc_library(
    name = "deps_test_impl",
    srcs = [
        "deps_test_impl.cc",
    ],
    hdrs = [
        "deps_test_impl.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cluster_link/replication:deps",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "partition_replicator_fixture_test",
    timeout = "short",
    srcs = [
        "partition_replicator_fixture_tests.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cluster_link/replication:deps_impl",
        "//src/v/cluster_link/replication:mux_remote_consumer",
        "//src/v/cluster_link/replication:partition_replicator",
        "//src/v/kafka/client/direct_consumer/tests:direct_consumer_fixture",
        "//src/v/model",
        "//src/v/redpanda/tests:fixture",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

load("//bazel:test.bzl", "redpanda_cc_gtest")

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

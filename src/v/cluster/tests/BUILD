load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "utils",
    hdrs = [
        "utils.h",
    ],
    include_prefix = "cluster/tests",
    deps = [
        "//src/v/cluster",
        "//src/v/test_utils:seastar_boost",
    ],
)

redpanda_test_cc_library(
    name = "leader_balancer_test_utils",
    hdrs = [
        "leader_balancer_constraints_utils.h",
        "leader_balancer_test_utils.h",
    ],
    include_prefix = "cluster/tests",
    deps = [
        "//src/v/cluster",
    ],
)

redpanda_test_cc_library(
    name = "topic_table_fixture",
    hdrs = [
        "topic_table_fixture.h",
    ],
    include_prefix = "cluster/tests",
    deps = [
        ":utils",
        "//src/v/base",
        "//src/v/cluster",
    ],
)

redpanda_cc_btest(
    name = "topic_table_test",
    timeout = "short",
    srcs = [
        "topic_table_test.cc",
    ],
    deps = [
        ":topic_table_fixture",
        ":utils",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_test_cc_library(
    name = "raft_fixiture_retry_policy",
    hdrs = [
        "raft_fixture_retry_policy.h",
    ],
    include_prefix = "cluster/tests",
    deps = [
        "//src/v/cluster",
        "//src/v/raft",
        "//src/v/raft/tests:raft_fixture",
    ],
)

redpanda_cc_gtest(
    name = "partition_properties_stm_test",
    timeout = "short",
    srcs = [
        "partition_properties_stm_test.cc",
    ],
    deps = [
        ":raft_fixiture_retry_policy",
        ":utils",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:random",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/raft",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/random:generators",
        "//src/v/serde",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "leader_balancer_test",
    timeout = "short",
    srcs = [
        "leader_balancer_test.cc",
    ],
    deps = [
        ":leader_balancer_test_utils",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:to_string",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@boost//:test",
    ],
)

redpanda_cc_btest(
    name = "leader_balancer_constraints_test",
    timeout = "short",
    srcs = [
        "leader_balancer_constraints_test.cc",
    ],
    deps = [
        ":leader_balancer_test_utils",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@boost//:test",
    ],
)

redpanda_cc_btest(
    name = "metrics_reporter_test",
    timeout = "short",
    srcs = [
        "metrics_reporter_test.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
    ],
)

redpanda_cc_gtest(
    name = "plugin_table_test",
    timeout = "short",
    srcs = [
        "plugin_table_test.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "//src/v/utils:uuid",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "plugin_frontend_validation_test",
    timeout = "short",
    srcs = [
        "plugin_frontend_validation_test.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/test_utils:gtest",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@boost//:range",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "namespaced_cache_test",
    timeout = "short",
    srcs = [
        "namespaced_cache_test.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/container:intrusive",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "cluster_recovery_table_test",
    timeout = "short",
    srcs = [
        "cluster_recovery_table_test.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "partition_leaders_table_test",
    timeout = "short",
    srcs = [
        "partition_leaders_table_test.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/raft:fundamental",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "shard_placement_table_test",
    timeout = "moderate",
    srcs = [
        "shard_placement_table_test.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/container:chunked_hash_map",
        "//src/v/features",
        "//src/v/ssx:event",
        "//src/v/storage",
        "//src/v/storage:resources",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
        "//src/v/utils:prefix_logger",
        "@seastar",
    ],
)

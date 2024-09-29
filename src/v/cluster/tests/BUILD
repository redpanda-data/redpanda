load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "utils",
    hdrs = [
        "utils.h",
    ],
    include_prefix = "cluster/tests",
    deps = [
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
    ],
)

redpanda_test_cc_library(
    name = "topic_table_fixture",
    hdrs = [
        "topic_table_fixture.h",
    ],
    include_prefix = "cluster/tests",
    deps = [
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

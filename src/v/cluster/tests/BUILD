load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_btest", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "randoms",
    hdrs = [
        "randoms.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cluster",
        "//src/v/model/tests:random",
        "//src/v/storage/tests:random",
    ],
)

redpanda_test_cc_library(
    name = "topic_properties_generator",
    hdrs = [
        "topic_properties_generator.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/model/tests:random",
    ],
)

redpanda_test_cc_library(
    name = "partition_allocator_fixture",
    hdrs = [
        "partition_allocator_fixture.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cluster",
    ],
)

redpanda_test_cc_library(
    name = "cluster_test_fixture",
    hdrs = [
        "cluster_test_fixture.h",
        "utils.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/redpanda:application",
        "//src/v/redpanda/tests:fixture",
        "//src/v/resource_mgmt:cpu_scheduling",
        "//src/v/test_utils:async",
        "//src/v/test_utils:test_macros",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "partition_balancer_planner_fixture",
    hdrs = [
        "partition_balancer_planner_fixture.h",
    ],
    deps = [
        ":utils",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/features",
        "//src/v/model",
        "//src/v/random:generators",
        "@boost//:test",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "rebalancing_tests_fixture",
    hdrs = [
        "rebalancing_tests_fixture.h",
    ],
    deps = [
        ":cluster_test_fixture",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/model/tests:random",
    ],
)

redpanda_test_cc_library(
    name = "tx_compaction_utils_gtest",
    hdrs = [
        "tx_compaction_utils.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cluster",
        "//src/v/storage/tests:batch_generators",
        "//src/v/storage/tests:storage_test_fixture_gtest",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
    ],
)

redpanda_test_cc_library(
    name = "rm_stm_test_fixture",
    hdrs = [
        "rm_stm_test_fixture.h",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/raft/tests:simple_raft_fixture",
    ],
)

redpanda_test_cc_library(
    name = "utils",
    hdrs = [
        "utils.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cluster",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/test_utils:async",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "leader_balancer_test_utils",
    hdrs = [
        "leader_balancer_constraints_utils.h",
        "leader_balancer_simulation_utils.h",
        "leader_balancer_test_utils.h",
    ],
    deps = [
        "//src/v/cluster",
    ],
)

redpanda_test_cc_library(
    name = "topic_table_fixture",
    hdrs = [
        "topic_table_fixture.h",
    ],
    deps = [
        ":utils",
        "//src/v/base",
        "//src/v/cluster",
        "@boost//:test",
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
    name = "raft_fixture_retry_policy",
    hdrs = [
        "raft_fixture_retry_policy.h",
    ],
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
        ":raft_fixture_retry_policy",
        ":utils",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:random",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/raft",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/random:generators",
        "//src/v/serde",
        "//src/v/test_utils:gtest",
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

redpanda_cc_gtest(
    name = "leader_balancer_greedy_test",
    timeout = "short",
    srcs = [
        "leader_balancer_greedy_test.cc",
    ],
    deps = [
        ":leader_balancer_test_utils",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@googletest//:gtest",
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

redpanda_cc_gtest(
    name = "leader_balancer_simulation_test",
    timeout = "short",
    srcs = [
        "leader_balancer_simulation_test.cc",
    ],
    deps = [
        ":leader_balancer_test_utils",
        "//src/v/cluster",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
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
        "//src/v/config",
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
        "//src/v/config",
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
        "//src/v/random:generators",
        "//src/v/ssx:event",
        "//src/v/ssx:future_util",
        "//src/v/storage",
        "//src/v/storage:resources",
        "//src/v/test_utils:gtest",
        "//src/v/utils:prefix_logger",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "partition_allocator_tests",
    timeout = "short",
    srcs = [
        "partition_allocator_tests.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/cluster:topic_memory_per_partition_default",
        "//src/v/cluster/tests:partition_allocator_fixture",
        "//src/v/config",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/raft:fundamental",
        "//src/v/random:generators",
        "//src/v/resource_mgmt:memory_groups",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "partition_balancer_planner_test",
    timeout = "short",
    srcs = [
        "partition_balancer_planner_test.cc",
    ],
    deps = [
        ":partition_balancer_planner_fixture",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:stable_iterator",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "partition_balancer_simulator_test",
    timeout = "long",
    srcs = [
        "partition_balancer_simulator_test.cc",
    ],
    deps = [
        ":partition_balancer_planner_fixture",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/test_utils:seastar_boost",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "simple_batch_builder_test",
    timeout = "short",
    srcs = [
        "simple_batch_builder_test.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/reflection:adl",
        "//src/v/test_utils:logs",
        "//src/v/test_utils:seastar_boost",
        "//src/v/test_utils:test_env",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "cluster_utils_tests",
    timeout = "short",
    srcs = [
        "cluster_utils_tests.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:unresolved_address",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "notification_latch_test",
    timeout = "short",
    srcs = [
        "notification_latch_test.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/cluster:offsets_lookup",
        "//src/v/model",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "commands_serialization_test",
    timeout = "short",
    srcs = [
        "commands_serialization_test.cc",
    ],
    deps = [
        ":utils",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/cluster/tests:topic_table_fixture",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/raft:fundamental",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "topic_updates_dispatcher_test",
    timeout = "short",
    srcs = [
        "topic_updates_dispatcher_test.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/cluster/tests:topic_table_fixture",
        "//src/v/model",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "topic_table_partition_generator_test",
    timeout = "short",
    srcs = [
        "topic_table_partition_generator_test.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/cluster/tests:topic_table_fixture",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "feature_barrier_test",
    timeout = "short",
    srcs = [
        "feature_barrier_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "tm_coordinator_mapper_tests",
    timeout = "short",
    srcs = [
        "tm_coordinator_mapper_tests.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "rm_stm_compatibility_test",
    timeout = "short",
    srcs = [
        "rm_stm_compatibility_test.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "producer_state_tests",
    timeout = "short",
    srcs = [
        "producer_state_tests.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/model/tests:random",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "client_quota_store_test",
    timeout = "short",
    srcs = [
        "client_quota_store_test.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:to_string",
        "@boost//:algorithm",
        "@boost//:range",
        "@boost//:test",
        "@fmt",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "crash_reporter_test",
    timeout = "short",
    srcs = [
        "crash_reporter_test.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/storage/tests:kvstore_fixture",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "create_partitions_test",
    timeout = "short",
    srcs = [
        "create_partitions_test.cc",
    ],
    cpu = 1,
    deps = [
        ":rebalancing_tests_fixture",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "decommissioning_test",
    timeout = "moderate",
    srcs = [
        "decommissioning_tests.cc",
    ],
    cpu = 1,
    memory = "1GiB",
    deps = [
        ":rebalancing_tests_fixture",
        "//src/v/cluster",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@boost//:test",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "replicas_rebalancing_test",
    timeout = "moderate",
    srcs = [
        "replicas_rebalancing_tests.cc",
    ],
    cpu = 1,
    deps = [
        ":rebalancing_tests_fixture",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/ssx:future_util",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/container:node_hash_map",
        "@boost//:test",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "manual_log_deletion_test",
    timeout = "short",
    srcs = [
        "manual_log_deletion_test.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/cluster:state_machine_registry",
        "//src/v/config",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/test_utils:gtest",
        "//src/v/utils:to_string",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "log_eviction_stm_test",
    timeout = "short",
    srcs = [
        "log_eviction_stm_test.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/test_utils:gtest",
        "//src/v/utils:to_string",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_bench(
    name = "partition_allocator_b_rpbench",
    srcs = ["allocation_bench.cc"],
    deps = [
        ":partition_allocator_fixture",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/raft",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_bench(
    name = "partition_balancer_rpbench",
    srcs = ["partition_balancer_bench.cc"],
    deps = [
        ":partition_balancer_planner_fixture",
        "@seastar//:benchmark",
    ],
)

redpanda_test_cc_library(
    name = "health_monitor_test_utils",
    hdrs = [
        "health_monitor_test_utils.h",
    ],
    deps = [
        "//src/v/cluster",
    ],
)

redpanda_cc_btest(
    name = "health_monitor_test",
    timeout = "short",
    srcs = ["health_monitor_test.cc"],
    deps = [
        ":cluster_test_fixture",
        ":health_monitor_test_utils",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "autocreate_test",
    timeout = "short",
    srcs = ["autocreate_test.cc"],
    deps = [
        ":cluster_test_fixture",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "cluster_tests",
    timeout = "short",
    srcs = ["cluster_tests.cc"],
    deps = [
        ":cluster_test_fixture",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/features",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "configuration_change_test",
    timeout = "short",
    srcs = ["configuration_change_test.cc"],
    deps = [
        ":cluster_test_fixture",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/features",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:unresolved_address",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "controller_api_tests",
    timeout = "short",
    srcs = ["controller_api_tests.cc"],
    deps = [
        ":cluster_test_fixture",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/ssx:future_util",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:unresolved_address",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "idempotency_tests",
    timeout = "short",
    srcs = ["idempotency_tests.cc"],
    deps = [
        ":rm_stm_test_fixture",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/raft",
        "//src/v/ssx:future_util",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:unresolved_address",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "metadata_dissemination_test",
    timeout = "short",
    srcs = ["metadata_dissemination_test.cc"],
    deps = [
        ":cluster_test_fixture",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:unresolved_address",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "tm_stm_tests",
    timeout = "short",
    srcs = ["tm_stm_tests.cc"],
    deps = [
        ":cluster_test_fixture",
        ":raft_fixture_retry_policy",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/test_utils:gtest",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_bench(
    name = "health_report_rpbench",
    srcs = ["health_bench.cc"],
    deps = [
        ":health_monitor_test_utils",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/random:generators",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_bench(
    name = "leader_balancer_bench_rpbench",
    srcs = ["leader_balancer_bench.cc"],
    deps = [
        ":leader_balancer_test_utils",
        "//src/v/cluster",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_bench(
    name = "health_monitor_rpbench",
    srcs = ["health_monitor_bench.cc"],
    deps = [
        "//src/v/cluster",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_gtest(
    name = "topic_properties_test",
    timeout = "short",
    srcs = [
        "topic_properties_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cluster:topic_configuration",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "types_test",
    timeout = "short",
    srcs = [
        "types_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cluster",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "id_allocator_stm_test",
    timeout = "short",
    srcs = [
        "id_allocator_stm_test.cc",
    ],
    deps = [
        ":raft_fixture_retry_policy",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/test_utils:scoped_config",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "data_migration_table_test",
    timeout = "short",
    srcs = [
        "data_migration_table_test.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "controller_state_test",
    timeout = "short",
    srcs = [
        "controller_state_test.cc",
    ],
    deps = [
        ":cluster_test_fixture",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/features",
        "//src/v/model",
        "//src/v/security",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:unresolved_address",
        "@boost//:test",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "ephemeral_credential_test",
    timeout = "short",
    srcs = [
        "ephemeral_credential_test.cc",
    ],
    deps = [
        ":cluster_test_fixture",
        "//src/v/cluster",
        "//src/v/features",
        "//src/v/model",
        "//src/v/security",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "local_monitor_fixture",
    hdrs = [
        "local_monitor_fixture.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cluster",
        "//src/v/features",
        "//src/v/storage:disk",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "local_monitor_test",
    timeout = "short",
    srcs = [
        "local_monitor_test.cc",
    ],
    deps = [
        ":local_monitor_fixture",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/random:generators",
        "//src/v/redpanda/tests:fixture",
        "//src/v/storage",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "security_frontend_test",
    timeout = "short",
    srcs = [
        "security_frontend_test.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/cluster/tests:cluster_test_fixture",
        "//src/v/container:zip",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/security",
        "//src/v/test_utils:seastar_boost",
        "@boost//:range",
        "@boost//:test",
    ],
)

redpanda_cc_btest(
    name = "serialization_rt_test",
    timeout = "short",
    srcs = [
        "serialization_rt_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/cluster/tests:randoms",
        "//src/v/cluster/tests:topic_properties_generator",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/pandaproxy/schema_registry/test:random",
        "//src/v/raft",
        "//src/v/raft:fundamental",
        "//src/v/random:generators",
        "//src/v/reflection:adl",
        "//src/v/security/tests:utils",
        "//src/v/storage",
        "//src/v/test_utils:random",
        "//src/v/test_utils:random_bytes",
        "//src/v/test_utils:rpc",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:tristate",
        "//src/v/v8_engine",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "tx_topic_test",
    timeout = "short",
    srcs = [
        "tx_topic_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/kafka/protocol",
        "//src/v/model",
        "//src/v/redpanda:application",
        "//src/v/redpanda/tests:fixture",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "distributed_kv_stm_test",
    timeout = "short",
    srcs = [
        "distributed_kv_stm_tests.cc",
    ],
    cpu = 3,
    deps = [
        ":raft_fixture_retry_policy",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/raft",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/serde",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "rm_stm_test",
    timeout = "short",
    srcs = [
        "rm_stm_tests.cc",
    ],
    cpu = 1,
    deps = [
        ":randoms",
        ":rm_stm_test_fixture",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/finjector",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/raft",
        "//src/v/raft/tests:raft_fixture_base",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/storage:record_batch_builder",
        "//src/v/storage/tests:batch_generators",
        "//src/v/storage/tests:disk_log_builder",
        "//src/v/test_utils:random",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:directory_walker",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_test_cc_library(
    name = "tx_compaction_utils",
    hdrs = [
        "tx_compaction_utils.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cluster",
        "//src/v/storage/tests:batch_generators",
        "//src/v/test_utils:seastar_boost",
    ],
)

redpanda_cc_btest(
    name = "tx_compaction_tests",
    timeout = "moderate",
    srcs = [
        "tx_compaction_tests.cc",
    ],
    deps = [
        ":rm_stm_test_fixture",
        ":tx_compaction_utils",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/test_utils:scoped_config",
        "//src/v/test_utils:seastar_boost",
        "//src/v/test_utils:test_env",
        "@boost//:test",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "epoch_service_test",
    timeout = "short",
    srcs = [
        "epoch_service_test.cc",
    ],
    cpu = 2,
    deps = [
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/ssx:abort_source",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "controller_forced_reconfiguration_test",
    timeout = "moderate",
    srcs = [
        "controller_forced_reconfiguration_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/cluster/tests:cluster_test_fixture",
        "//src/v/config",
        "//src/v/container:zip",
        "//src/v/model",
        "//src/v/raft:fundamental",
        "//src/v/random:generators",
        "//src/v/redpanda/tests:fixture",
        "//src/v/security",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "partition_balancer_autodecom_test",
    timeout = "short",
    srcs = [
        "partition_balancer_autodecom_test.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "//src/v/utils:to_string",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "client_quota_serde_test",
    timeout = "short",
    srcs = [
        "client_quota_serde_test.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/serde",
        "//src/v/serde:set",
        "//src/v/serde:sstring",
        "//src/v/test_utils:gtest",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
    ],
)

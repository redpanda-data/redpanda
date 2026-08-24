load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "test_deps",
    srcs = [
        "deps.cc",
    ],
    hdrs = [
        "deps.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cluster:cluster_link_errc",
        "//src/v/cluster:cluster_link_rpc_types",
        "//src/v/cluster:cluster_link_table",
        "//src/v/cluster:types",
        "//src/v/cluster/cluster_link/tests:test_lib",
        "//src/v/cluster_link:impl",
        "//src/v/cluster_link:utils",
        "//src/v/cluster_link/replication/tests:deps_test_impl",
        "//src/v/config",
        "//src/v/container:chunked_vector",
        "//src/v/features",
        "//src/v/kafka/client/test:cluster_mock",
        "//src/v/kafka/data/rpc",
        "//src/v/kafka/data/rpc/test:test_deps",
        "//src/v/model",
        "//src/v/pandaproxy/schema_registry:types",
        "//src/v/schema/tests:fake_registry",
        "//src/v/security",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "producer_id_barrier_test",
    timeout = "short",
    srcs = [
        "producer_id_barrier_test.cc",
    ],
    deps = [
        "//src/v/cluster_link:producer_id_barrier",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "link_test",
    timeout = "short",
    srcs = [
        "link_test.cc",
    ],
    deps = [
        ":test_deps",
        "//src/v/cluster:cluster_link_table",
        "//src/v/cluster/cluster_link/tests:test_lib",
        "//src/v/cluster_link:impl",
        "//src/v/cluster_link/replication/tests:deps_test_impl",
        "//src/v/kafka/protocol:find_coordinator",
        "//src/v/kafka/protocol:list_groups",
        "//src/v/kafka/protocol:list_offset",
        "//src/v/kafka/protocol:offset_fetch",
        "//src/v/model",
        "//src/v/schema/tests:fake_registry",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "errc_test",
    timeout = "short",
    srcs = [
        "errc_test.cc",
    ],
    deps = [
        "//src/v/cluster_link:errc",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "base_task_test",
    timeout = "short",
    srcs = [
        "base_task_test.cc",
    ],
    deps = [
        "//src/v/cluster_link:impl",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "task_manager_integration_test",
    timeout = "short",
    srcs = [
        "task_manager_integration_test.cc",
    ],
    flaky = True,
    deps = [
        ":test_deps",
        "//src/v/cluster:cluster_link_table",
        "//src/v/cluster:fwd",
        "//src/v/cluster_link:impl",
        "//src/v/cluster_link/model",
        "//src/v/kafka/data/rpc",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "utils_test",
    timeout = "short",
    srcs = [
        "utils_test.cc",
    ],
    deps = [
        "//src/v/cluster_link:utils",
        "//src/v/cluster_link/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "source_topic_syncer_test",
    timeout = "short",
    srcs = [
        "source_topic_syncer_test.cc",
        "topic_properties_syncer_test.cc",
    ],
    deps = [
        ":test_deps",
        "//src/v/cluster:cluster_link_table",
        "//src/v/cluster_link:source_topic_syncer",
        "//src/v/cluster_link/model",
        "//src/v/kafka/server:topic_config_utils",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "topic_reconciler_test",
    timeout = "short",
    srcs = [
        "topic_reconciler_test.cc",
    ],
    deps = [
        ":test_deps",
        "//src/v/cluster:cluster_link_table",
        "//src/v/cluster_link:impl",
        "//src/v/cluster_link/utils:topic_properties_utils",
        "//src/v/config",
        "//src/v/features",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "group_mirroring_task",
    timeout = "short",
    srcs = [
        "group_mirroring_task_test.cc",
    ],
    deps = [
        ":test_deps",
        "//src/v/cluster:cluster_link_table",
        "//src/v/cluster:fwd",
        "//src/v/cluster_link:group_mirroring_task",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "security_migrator_test",
    timeout = "short",
    srcs = [
        "security_migrator_test.cc",
    ],
    deps = [
        ":test_deps",
        "//src/v/cluster:cluster_link_table",
        "//src/v/cluster_link:security_migrator",
        "//src/v/kafka/server:security",
        "//src/v/security",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "roles_migrator_test",
    timeout = "short",
    srcs = [
        "roles_migrator_test.cc",
    ],
    deps = [
        ":test_deps",
        "//src/v/cluster_link:roles_migrator",
        "//src/v/kafka/protocol:describe_redpanda_roles",
        "//src/v/kafka/server:roles",
        "//src/v/security",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "role_reconcile_test",
    timeout = "short",
    srcs = [
        "role_reconcile_test.cc",
    ],
    deps = [
        "//src/v/cluster_link:role_reconcile",
        "//src/v/security",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
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
        "//src/v/cluster:fwd",
        "//src/v/cluster:inventory_service",
        "//src/v/cluster:leader_balancer_strategy",
        "//src/v/cluster:node_status_backend",
        "//src/v/cluster:partition_properties_stm",
        "//src/v/cluster:self_test",
        "//src/v/cluster:topic_metrics_watcher",
        "//src/v/cluster:topic_table",
        "//src/v/cluster:tx_manager_migrator_rpc",
        "//src/v/cluster_link",
        "//src/v/cluster_link/replication:deps",
        "//src/v/cluster_link/replication:mux_remote_consumer",
        "//src/v/cluster_link/replication:partition_replicator",
        "//src/v/cluster_link/replication/tests:deps_test_impl",
        "//src/v/kafka/client/direct_consumer/tests:direct_consumer_fixture",
        "//src/v/model",
        "//src/v/redpanda/tests:fixture",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

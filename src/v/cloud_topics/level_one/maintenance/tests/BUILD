load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_cc_gtest(
    name = "keyed_priority_queue_test",
    timeout = "short",
    srcs = [
        "keyed_priority_queue_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cloud_topics/level_one/maintenance:keyed_priority_queue",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "worker_manager_test",
    timeout = "short",
    srcs = [
        "worker_manager_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/maintenance:meta",
        "//src/v/cloud_topics/level_one/maintenance:scheduler_probe",
        "//src/v/cloud_topics/level_one/maintenance:scheduling_policies",
        "//src/v/cloud_topics/level_one/maintenance:worker",
        "//src/v/cloud_topics/level_one/maintenance/compaction:compaction_queue",
        "//src/v/cloud_topics/level_one/maintenance/leveling:leveling_queue",
        "//src/v/cluster:fwd",
        "//src/v/cluster:members_table",
        "//src/v/cluster:partition_leaders_table",
        "//src/v/cluster:topic_table",
        "//src/v/config",
        "//src/v/model",
        "//src/v/ssx:semaphore",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "log_info_collector_test",
    timeout = "short",
    srcs = [
        "log_info_collector_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/frontend_reader/tests:l1_reader_fixture",
        "//src/v/cloud_topics/level_one/maintenance:log_info_collector",
        "//src/v/cloud_topics/level_one/maintenance:meta",
        "//src/v/cloud_topics/level_one/maintenance:scheduling_policies",
        "//src/v/cloud_topics/level_one/metastore:offset_interval_map",
        "//src/v/cluster:fwd",
        "//src/v/cluster:members_table",
        "//src/v/cluster:partition_leaders_table",
        "//src/v/cluster:partition_properties_stm",
        "//src/v/cluster:shard_table",
        "//src/v/cluster:state_machine_registry",
        "//src/v/cluster:topic_configuration",
        "//src/v/cluster:topic_table",
        "//src/v/config",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "log_collector_test",
    timeout = "short",
    srcs = [
        "log_collector_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/maintenance:log_collector",
        "//src/v/cluster:commands",
        "//src/v/cluster:data_migrated_resources",
        "//src/v/cluster:fwd",
        "//src/v/cluster:members_table",
        "//src/v/cluster:partition_leaders_table",
        "//src/v/cluster:topic_table",
        "//src/v/cluster:types",
        "//src/v/config",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "scheduler_fixture",
    hdrs = [
        "scheduler_fixture.h",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/frontend_reader/tests:l1_reader_fixture",
        "//src/v/cloud_topics/level_one/maintenance:log_info_collector",
        "//src/v/cloud_topics/level_one/maintenance:scheduler",
        "//src/v/cloud_topics/level_one/maintenance:scheduling_policies",
        "//src/v/cloud_topics/level_one/maintenance:worker",
        "//src/v/cluster:topic_configuration",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/storage/tests:batch_generators",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "scheduler_test",
    timeout = "short",
    srcs = [
        "scheduler_test.cc",
    ],
    cpu = 1,
    deps = [
        ":scheduler_fixture",
        "//src/v/cluster:fwd",
        "//src/v/cluster:members_table",
        "//src/v/cluster:partition_leaders_table",
        "//src/v/cluster:partition_properties_stm",
        "//src/v/cluster:shard_table",
        "//src/v/cluster:state_machine_registry",
        "//src/v/cluster:topic_table",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/storage/tests:batch_generators",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "scheduler_multithread_test",
    timeout = "short",
    srcs = [
        "scheduler_multithread_test.cc",
    ],
    cpu = 8,
    deps = [
        ":scheduler_fixture",
        "//src/v/cluster:fwd",
        "//src/v/cluster:members_table",
        "//src/v/cluster:partition_leaders_table",
        "//src/v/cluster:partition_properties_stm",
        "//src/v/cluster:shard_table",
        "//src/v/cluster:state_machine_registry",
        "//src/v/cluster:topic_table",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "compaction_e2e_test",
    timeout = "short",
    srcs = [
        "compaction_e2e_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_topics/level_zero/stm:ctp_stm",
        "//src/v/cluster:cluster_utils",
        "//src/v/cluster:controller_log_limiter",
        "//src/v/cluster:data_migration_table",
        "//src/v/cluster:feature_backend",
        "//src/v/cluster:leader_balancer_strategy",
        "//src/v/cluster:node_status_backend",
        "//src/v/cluster:self_test",
        "//src/v/cluster:topic_metrics_watcher",
        "//src/v/cluster:types",
        "//src/v/model",
        "//src/v/redpanda/tests:fixture",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_btest", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "simple_raft_fixture",
    hdrs = [
        "simple_raft_fixture.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/config",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/rpc",
        "//src/v/storage",
        "//src/v/test_utils:async",
        "//src/v/test_utils:test_env",
        "//src/v/utils:unresolved_address",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "simple_record_fixture",
    hdrs = [
        "simple_record_fixture.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/storage:record_batch_builder",
    ],
)

redpanda_test_cc_library(
    name = "raft_fixture_retry_policy",
    hdrs = [
        "raft_fixture_retry_policy.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":raft_fixture",
        "//src/v/raft",
    ],
)

redpanda_test_cc_library(
    name = "failure_injectable_log",
    srcs = ["failure_injectable_log.cc"],
    hdrs = [
        "failure_injectable_log.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/storage",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "raft_fixture_base",
    srcs = ["raft_fixture_base.cc"],
    hdrs = [
        "raft_fixture_base.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":failure_injectable_log",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/config",
        "//src/v/features",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/rpc",
        "//src/v/ssx:future_util",
        "//src/v/ssx:sformat",
        "//src/v/storage",
        "//src/v/test_utils:random_bytes",
        "//src/v/test_utils:test_env",
        "//src/v/utils:prefix_logger",
        "//src/v/utils:unresolved_address",
        "@abseil-cpp//absl/container:node_hash_map",
        "@boost//:range",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "raft_fixture",
    srcs = ["raft_fixture.cc"],
    hdrs = [
        "raft_fixture.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":failure_injectable_log",
        ":raft_fixture_base",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/config",
        "//src/v/features",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/rpc",
        "//src/v/ssx:future_util",
        "//src/v/ssx:sformat",
        "//src/v/storage",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random_bytes",
        "//src/v/utils:prefix_logger",
        "//src/v/utils:unresolved_address",
        "@abseil-cpp//absl/container:node_hash_map",
        "@boost//:range",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "stm_raft_fixture",
    hdrs = [
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:random",
        "//src/v/config",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/rpc",
        "//src/v/serde",
        "//src/v/storage",
        "//src/v/test_utils:random",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "jitter_test",
    timeout = "short",
    srcs = [
        "jitter_tests.cc",
    ],
    deps = [
        "//src/v/config",
        "//src/v/raft",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "bootstrap_configuration_test",
    timeout = "short",
    srcs = [
        "bootstrap_configuration_test.cc",
    ],
    deps = [
        ":simple_record_fixture",
        "//src/v/config",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:random",
        "//src/v/test_utils:seastar_boost",
        "//src/v/test_utils:test_env",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "heartbeat_test",
    timeout = "short",
    srcs = [
        "heartbeats_test.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf_parser",
        "//src/v/config",
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/test_utils:random",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "consensus_utils_test",
    timeout = "short",
    srcs = [
        "consensus_utils_test.cc",
    ],
    deps = [
        ":simple_record_fixture",
        "//src/v/config",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:random",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:unresolved_address",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "group_configuration_test",
    timeout = "short",
    srcs = [
        "group_configuration_tests.cc",
    ],
    deps = [
        ":simple_record_fixture",
        "//src/v/base",
        "//src/v/raft",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "snapshot_recovery_test",
    timeout = "short",
    srcs = [
        "snapshot_recovery_test.cc",
    ],
    cpu = 1,
    deps = [
        ":raft_fixture",
        ":raft_fixture_retry_policy",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/test_utils:gtest",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "foreign_entry_test",
    timeout = "short",
    srcs = [
        "foreign_entry_test.cc",
    ],
    deps = [
        "//src/v/container:chunked_circular_buffer",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:random",
        "//src/v/test_utils:seastar_boost",
        "//src/v/test_utils:test_env",
        "//src/v/utils:copy_range",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "configuration_serialization_test",
    timeout = "short",
    srcs = [
        "configuration_serialization_test.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf_parser",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/raft",
        "//src/v/raft:broker_compat",
        "//src/v/random:generators",
        "//src/v/reflection:adl",
        "//src/v/test_utils:random",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "type_serialization_tests",
    timeout = "short",
    srcs = [
        "type_serialization_tests.cc",
    ],
    deps = [
        "//src/v/compression",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/reflection:adl",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:random",
        "//src/v/test_utils:rpc",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "term_assigning_reader_test",
    timeout = "short",
    srcs = [
        "term_assigning_reader_test.cc",
    ],
    deps = [
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/raft",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "membership_test",
    timeout = "short",
    srcs = [
        "membership_test.cc",
    ],
    deps = [
        ":raft_fixture",
        ":raft_fixture_retry_policy",
        "//src/v/raft",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "leadership_test",
    timeout = "short",
    srcs = [
        "leadership_test.cc",
    ],
    deps = [
        ":raft_fixture",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "state_removal_test",
    timeout = "short",
    srcs = [
        "state_removal_test.cc",
    ],
    deps = [
        ":raft_fixture",
        ":raft_fixture_retry_policy",
        "//src/v/raft",
        "//src/v/raft/tests:stm_raft_fixture",
        "//src/v/test_utils:gtest",
        "//src/v/utils:directory_walker",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "offset_monitor_test",
    timeout = "short",
    srcs = [
        "offset_monitor_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/raft",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "mutex_buffer_test",
    timeout = "short",
    srcs = [
        "mutex_buffer_test.cc",
    ],
    deps = [
        "//src/v/raft",
        "//src/v/ssx:mutex",
        "//src/v/ssx:sformat",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "configuration_manager_test",
    timeout = "short",
    srcs = [
        "configuration_manager_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/config",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/test_utils:random",
        "//src/v/test_utils:seastar_boost",
        "//src/v/test_utils:test_env",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "coordinated_recovery_throttle_test",
    timeout = "short",
    srcs = [
        "coordinated_recovery_throttle_test.cc",
    ],
    deps = [
        "//src/v/config",
        "//src/v/raft",
        "//src/v/ssx:when_all",
        "//src/v/test_utils:seastar_boost",
        "@boost//:range",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "basic_raft_fixture_test",
    timeout = "moderate",
    srcs = [
        "basic_raft_fixture_test.cc",
    ],
    deps = [
        "//src/v/model",
        "//src/v/raft",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/raft/tests:raft_fixture_retry_policy",
        "//src/v/raft/tests:stm_raft_fixture",
        "//src/v/random:generators",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:gtest",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "compaction_recovery_test",
    timeout = "short",
    srcs = [
        "compaction_recovery_test.cc",
    ],
    deps = [
        "//src/v/config",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/raft/tests:raft_fixture_retry_policy",
        "//src/v/random:generators",
        "//src/v/serde",
        "//src/v/storage",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random_bytes",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "coordinated_compaction_test",
    timeout = "short",
    srcs = [
        "coordinated_compaction_test.cc",
    ],
    deps = [
        "//src/v/model",
        "//src/v/raft",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/raft/tests:raft_fixture_retry_policy",
        "//src/v/storage",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:gtest",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "stm_manager_test",
    timeout = "moderate",
    srcs = [
        "stm_manager_test.cc",
    ],
    deps = [
        "//src/v/container:chunked_circular_buffer",
        "//src/v/raft/tests:raft_fixture_retry_policy",
        "//src/v/raft/tests:stm_raft_fixture",
        "//src/v/raft/tests:stm_test_fixture",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar//:testing",
    ],
)

redpanda_test_cc_library(
    name = "stm_test_fixture",
    hdrs = [
        "stm_test_fixture.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":raft_fixture",
        ":raft_fixture_retry_policy",
        "//src/v/base",
        "//src/v/bytes:random",
        "//src/v/config",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/rpc",
        "//src/v/serde",
        "//src/v/storage",
        "//src/v/test_utils:random",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "raft_reconfiguration_test",
    timeout = "long",
    srcs = [
        "raft_reconfiguration_test.cc",
    ],
    cpu = 4,
    memory = "6GiB",
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/raft/tests:raft_fixture_retry_policy",
        "//src/v/random:generators",
        "//src/v/serde",
        "//src/v/ssx:future_util",
        "//src/v/ssx:watchdog",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
        "//src/v/test_utils:random_bytes",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "persisted_stm_test",
    timeout = "long",
    srcs = [
        "persisted_stm_test.cc",
    ],
    deps = [
        "//src/v/container:chunked_circular_buffer",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/raft/tests:raft_fixture_retry_policy",
        "//src/v/raft/tests:stm_raft_fixture",
        "//src/v/raft/tests:stm_test_fixture",
        "//src/v/random:generators",
        "//src/v/serde",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "replication_monitor_tests",
    timeout = "short",
    srcs = [
        "replication_monitor_tests.cc",
    ],
    deps = [
        "//src/v/model",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/raft/tests:stm_raft_fixture",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "mux_state_machine_test",
    timeout = "short",
    srcs = [
        "mux_state_machine_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/raft/tests:raft_fixture_retry_policy",
        "//src/v/reflection:adl",
        "//src/v/storage:record_batch_builder",
        "//src/v/storage/tests:disk_log_builder",
        "//src/v/test_utils:gtest",
        "@boost//:range",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "buffered_protocol_test",
    timeout = "short",
    srcs = [
        "buffered_protocol_test.cc",
    ],
    deps = [
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "bootstrap_partition_state_test",
    timeout = "short",
    srcs = ["bootstrap_partition_state_test.cc"],
    deps = [
        "//src/v/base",
        "//src/v/features",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/storage",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:test_env",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_bench(
    name = "heartbeat_bench_rpbench",
    srcs = ["heartbeat_bench.cc"],
    deps = [
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/test_utils:random",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_bench(
    name = "raft_replicate_rpbench",
    srcs = ["raft_replicate_benchmark.cc"],
    deps = [
        "//src/v/raft",
        "//src/v/raft/tests:raft_fixture_base",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

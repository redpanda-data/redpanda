load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "simple_raft_fixture_btest",
    hdrs = [
        "simple_raft_fixture.h",
    ],
    include_prefix = "raft/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/config",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/rpc",
        "//src/v/storage",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:unresolved_address",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "simple_raft_fixture_gtest",
    hdrs = [
        "simple_raft_fixture.h",
    ],
    include_prefix = "raft/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/config",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/rpc",
        "//src/v/storage",
        "//src/v/test_utils:gtest",
        "//src/v/utils:unresolved_address",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "simple_record_fixture",
    hdrs = [
        "simple_record_fixture.h",
    ],
    include_prefix = "raft/tests",
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
    name = "raft_fixiture_retry_policy",
    hdrs = [
        "raft_fixture_retry_policy.h",
    ],
    include_prefix = "raft/tests",
    visibility = ["//visibility:public"],
    deps = [
        ":raft_fixture",
        "//src/v/raft",
    ],
)

redpanda_test_cc_library(
    name = "raft_fixture",
    srcs = ["raft_fixture.cc"],
    hdrs = [
        "raft_fixture.h",
    ],
    include_prefix = "raft/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/bytes:random",
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
        "//src/v/utils:prefix_logger",
        "//src/v/utils:unresolved_address",
        "@abseil-cpp//absl/container:node_hash_map",
        "@boost//:range",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "stm_raft_fixture_btest",
    hdrs = [
        "raft_group_fixture.h",
    ],
    include_prefix = "raft/tests",
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
        "//src/v/test_utils:seastar_boost",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "stm_raft_fixture_gtest",
    hdrs = [
        "raft_group_fixture.h",
        "stm_test_fixture.h",
    ],
    include_prefix = "raft/tests",
    visibility = ["//visibility:public"],
    deps = [
        ":raft_fixiture_retry_policy",
        ":raft_fixture",
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
        "//src/v/test_utils:gtest",
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
    tags = ["exclusive"],
    deps = [
        ":simple_record_fixture",
        "//src/v/config",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/resource_mgmt:io_priority",
        "//src/v/storage",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:random",
        "//src/v/test_utils:seastar_boost",
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
    tags = ["exclusive"],
    deps = [
        ":simple_record_fixture",
        "//src/v/config",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/resource_mgmt:io_priority",
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

redpanda_cc_btest(
    name = "group_configuration_test",
    timeout = "short",
    srcs = [
        "group_configuration_tests.cc",
    ],
    tags = ["exclusive"],
    deps = [
        ":simple_record_fixture",
        "//src/v/config",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/resource_mgmt:io_priority",
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
    name = "snapshot_recovery_test",
    timeout = "short",
    srcs = [
        "snapshot_recovery_test.cc",
    ],
    cpu = 1,
    tags = ["exclusive"],
    deps = [
        ":raft_fixiture_retry_policy",
        ":raft_fixture",
        "//src/v/config",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/resource_mgmt:io_priority",
        "//src/v/storage",
        "//src/v/test_utils:gtest",
        "//src/v/utils:unresolved_address",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "foreign_entry_test",
    timeout = "short",
    srcs = [
        "foreign_entry_test.cc",
    ],
    tags = ["exclusive"],
    deps = [
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/resource_mgmt:io_priority",
        "//src/v/storage",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:fixture",
        "//src/v/test_utils:random",
        "//src/v/test_utils:seastar_boost",
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

redpanda_cc_btest(
    name = "membership_test",
    timeout = "short",
    srcs = [
        "membership_test.cc",
    ],
    tags = ["exclusive"],
    deps = [
        "//src/v/model",
        "//src/v/raft",
        "//src/v/raft/tests:stm_raft_fixture_btest",
        "//src/v/storage",
        "//src/v/test_utils:fixture",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "leadership_test",
    timeout = "short",
    srcs = [
        "leadership_test.cc",
    ],
    tags = ["exclusive"],
    deps = [
        "//src/v/finjector",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/raft/tests:stm_raft_fixture_btest",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "state_removal_test",
    timeout = "short",
    srcs = [
        "state_removal_test.cc",
    ],
    tags = ["exclusive"],
    deps = [
        "//src/v/raft",
        "//src/v/raft/tests:stm_raft_fixture_btest",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:directory_walker",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "append_entries_test",
    timeout = "short",
    srcs = [
        "append_entries_test.cc",
    ],
    tags = ["exclusive"],
    deps = [
        "//src/v/bytes",
        "//src/v/bytes:random",
        "//src/v/finjector",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/raft",
        "//src/v/raft/tests:stm_raft_fixture_btest",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/storage:record_batch_builder",
        "//src/v/storage/tests:disk_log_builder",
        "//src/v/test_utils:fixture",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "offset_monitor_test",
    timeout = "short",
    srcs = [
        "offset_monitor_test.cc",
    ],
    tags = ["exclusive"],
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
    tags = ["exclusive"],
    deps = [
        "//src/v/raft",
        "//src/v/ssx:sformat",
        "//src/v/test_utils:fixture",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:mutex",
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
    tags = ["exclusive"],
    deps = [
        "//src/v/base",
        "//src/v/config",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/test_utils:fixture",
        "//src/v/test_utils:random",
        "//src/v/test_utils:seastar_boost",
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
        "//src/v/test_utils:fixture",
        "//src/v/test_utils:seastar_boost",
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
    tags = ["exclusive"],
    deps = [
        "//src/v/model",
        "//src/v/raft",
        "//src/v/raft/tests:raft_fixiture_retry_policy",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/raft/tests:stm_raft_fixture_gtest",
        "//src/v/random:generators",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:fixture",
        "//src/v/test_utils:gtest",
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
    tags = ["exclusive"],
    deps = [
        "//src/v/raft/tests:raft_fixiture_retry_policy",
        "//src/v/raft/tests:stm_raft_fixture_gtest",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar//:testing",
    ],
)

redpanda_test_cc_library(
    name = "stm_test_fixture",
    hdrs = [
        "raft_group_fixture.h",
        "stm_test_fixture.h",
    ],
    include_prefix = "raft/tests",
    visibility = ["//visibility:public"],
    deps = [
        ":raft_fixiture_retry_policy",
        ":raft_fixture",
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
        "//src/v/test_utils:seastar_boost",
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
    memory = "2GiB",
    tags = ["exclusive"],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/raft/tests:raft_fixiture_retry_policy",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/random:generators",
        "//src/v/serde",
        "//src/v/ssx:future_util",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:fixture",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "persisted_stm_test",
    timeout = "short",
    srcs = [
        "persisted_stm_test.cc",
    ],
    tags = ["exclusive"],
    deps = [
        "//src/v/model",
        "//src/v/raft",
        "//src/v/raft/tests:raft_fixiture_retry_policy",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/raft/tests:stm_raft_fixture_gtest",
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
    tags = ["exclusive"],
    deps = [
        "//src/v/model",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/raft/tests:stm_raft_fixture_gtest",
        "//src/v/test_utils:fixture",
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
    tags = ["exclusive"],
    deps = [
        "//src/v/base",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/raft/tests:raft_fixiture_retry_policy",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/raft/tests:simple_raft_fixture_gtest",
        "//src/v/reflection:adl",
        "//src/v/storage:record_batch_builder",
        "//src/v/storage/tests:disk_log_builder",
        "//src/v/test_utils:fixture",
        "//src/v/test_utils:gtest",
        "@boost//:range",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

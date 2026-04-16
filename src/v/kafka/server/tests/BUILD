load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_btest", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "config_response_utils_test_help",
    hdrs = [
        "config_response_utils_test_help.h",
    ],
    implementation_deps = [
    ],
    deps = [
        "//src/v/kafka/protocol:describe_configs",
        "//src/v/kafka/server",
    ],
)

redpanda_test_cc_library(
    name = "topic_properties_helpers",
    hdrs = [
        "topic_properties_helpers.h",
    ],
    implementation_deps = [
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/redpanda/tests:fixture",
    ],
)

redpanda_test_cc_library(
    name = "kafka_test_utils",
    srcs = [
        "delete_records_utils.cc",
        "list_offsets_utils.cc",
        "offset_for_leader_epoch_utils.cc",
        "produce_consume_utils.cc",
    ],
    hdrs = [
        "delete_records_utils.h",
        "list_offsets_utils.h",
        "offset_for_leader_epoch_utils.h",
        "produce_consume_utils.h",
    ],
    implementation_deps = [
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:chunked_vector",
        "//src/v/kafka/client:transport",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:delete_records",
        "//src/v/kafka/protocol:fetch",
        "//src/v/kafka/protocol:list_offset",
        "//src/v/kafka/protocol:offset_for_leader_epoch",
        "//src/v/kafka/protocol:produce",
        "//src/v/kafka/protocol/schemata:delete_records_request",
        "//src/v/kafka/protocol/schemata:delete_records_response",
        "//src/v/kafka/protocol/schemata:list_offset_request",
        "//src/v/kafka/protocol/schemata:list_offset_response",
        "//src/v/kafka/protocol/schemata:offset_for_leader_epoch_request",
        "//src/v/kafka/protocol/schemata:offset_for_leader_epoch_response",
        "//src/v/kafka/protocol/schemata:produce_request",
        "//src/v/model",
        "//src/v/storage:record_batch_builder",
        "//src/v/utils:to_string",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "atomic_token_bucket_test",
    timeout = "short",
    srcs = [
        "atomic_token_bucket_test.cc",
    ],
    deps = [
        "//src/v/kafka/server",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:to_string",
        "@boost//:test",
    ],
)

redpanda_cc_btest(
    name = "error_mapping_test",
    timeout = "short",
    srcs = [
        "error_mapping_test.cc",
    ],
    deps = [
        "//src/v/kafka/server",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
    ],
)

# TODO does this really not depend on kafka server?
redpanda_cc_btest(
    name = "timeouts_conversion_test",
    timeout = "short",
    srcs = [
        "timeouts_conversion_test.cc",
    ],
    deps = [
        "//src/v/kafka/protocol",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
    ],
)

redpanda_cc_btest(
    name = "types_conversion_tests",
    timeout = "short",
    srcs = [
        "types_conversion_tests.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/kafka/protocol/schemata:create_topics_request",
        "//src/v/kafka/server",
        "//src/v/kafka/server:topic_config_utils",
        "//src/v/model",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "topic_utils_test",
    timeout = "short",
    srcs = [
        "topic_utils_test.cc",
    ],
    deps = [
        "//src/v/kafka/protocol/schemata:create_topics_response",
        "//src/v/kafka/server",
        "//src/v/kafka/server:topic_config_utils",
        "//src/v/model",
        "//src/v/test_utils:seastar_boost",
        "@boost//:range",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "handler_interface_test",
    timeout = "short",
    srcs = [
        "handler_interface_test.cc",
    ],
    deps = [
        "//src/v/kafka/server",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "validator_tests",
    timeout = "short",
    srcs = [
        "validator_tests.cc",
    ],
    deps = [
        "//src/v/kafka/protocol/schemata:create_topics_request",
        "//src/v/kafka/server",
        "//src/v/kafka/server:topic_config_utils",
        "//src/v/pandaproxy/schema_registry:subject_name_strategy",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "config_utils_test",
    timeout = "short",
    srcs = [
        "config_utils_test.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/kafka/server",
        "//src/v/kafka/server:topic_config_utils",
        "//src/v/model",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "config_response_utils_test",
    timeout = "short",
    srcs = [
        "config_response_utils_test.cc",
    ],
    deps = [
        "//src/v/kafka/server",
        "//src/v/kafka/server/tests:config_response_utils_test_help",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:to_string",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "fetch_memory_units_test",
    timeout = "short",
    srcs = [
        "fetch_memory_units_test.cc",
    ],
    cpu = 2,
    deps = [
        "//src/v/base",
        "//src/v/kafka/server",
        "//src/v/ssx:semaphore",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "consumer_group_recovery_test",
    timeout = "short",
    srcs = [
        "consumer_group_recovery_test.cc",
    ],
    deps = [
        "//src/v/container:chunked_circular_buffer",
        "//src/v/container:chunked_hash_map",
        "//src/v/kafka/protocol",
        "//src/v/kafka/server",
        "//src/v/model",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/container:node_hash_map",
        "@abseil-cpp//absl/strings",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "member_test",
    timeout = "short",
    srcs = [
        "member_test.cc",
    ],
    deps = [
        "//src/v/container:chunked_vector",
        "//src/v/kafka/protocol",
        "//src/v/kafka/server",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:to_string",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "consumer_groups_test",
    timeout = "short",
    srcs = [
        "consumer_groups_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cluster",
        "//src/v/container:chunked_vector",
        "//src/v/kafka/client:transport",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:describe_groups",
        "//src/v/kafka/protocol:find_coordinator",
        "//src/v/kafka/protocol:join_group",
        "//src/v/kafka/protocol:offset_commit",
        "//src/v/kafka/protocol/schemata:join_group_request",
        "//src/v/kafka/server",
        "//src/v/model",
        "//src/v/model:timeout_clock",
        "//src/v/redpanda/tests:fixture",
        "//src/v/security",
        "//src/v/test_utils:scoped_config",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:base64",
        "//src/v/utils:to_string",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "group_test",
    timeout = "short",
    srcs = [
        "group_test.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/container:chunked_vector",
        "//src/v/kafka/protocol",
        "//src/v/kafka/server",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:to_string",
        "@boost//:test",
        "@boost//:uuid",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "read_write_roundtrip_test",
    timeout = "short",
    srcs = [
        "read_write_roundtrip_test.cc",
    ],
    deps = [
        "//src/v/kafka/protocol",
        "//src/v/random:generators",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:to_string",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "delete_topics_test",
    timeout = "short",
    srcs = [
        "delete_topics_test.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/container:chunked_vector",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:create_topics",
        "//src/v/kafka/protocol:delete_topics",
        "//src/v/kafka/protocol:metadata",
        "//src/v/kafka/protocol/schemata:create_topics_request",
        "//src/v/kafka/server",
        "//src/v/kafka/server:topic_config_utils",
        "//src/v/redpanda:application",
        "//src/v/redpanda/tests:fixture",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "offset_fetch_test",
    timeout = "short",
    srcs = [
        "offset_fetch_test.cc",
    ],
    deps = [
        "//src/v/kafka/protocol:offset_fetch",
        "//src/v/kafka/server",
        "//src/v/redpanda/tests:fixture",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "api_versions_test",
    timeout = "short",
    srcs = [
        "api_versions_test.cc",
    ],
    deps = [
        "//src/v/kafka/server",
        "//src/v/redpanda/tests:fixture",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "usage_manager_test",
    timeout = "short",
    srcs = [
        "usage_manager_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/kafka/server",
        "//src/v/storage/tests:kvstore_fixture",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "create_topics_test",
    timeout = "short",
    srcs = [
        "create_topics_test.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/container:chunked_vector",
        "//src/v/features:enterprise_feature_messages",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:create_topics",
        "//src/v/kafka/protocol:metadata",
        "//src/v/kafka/server",
        "//src/v/kafka/server:topic_config_utils",
        "//src/v/kafka/server/tests:topic_properties_helpers",
        "//src/v/model",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "find_coordinator_test",
    timeout = "short",
    srcs = [
        "find_coordinator_test.cc",
    ],
    deps = [
        "//src/v/kafka/protocol:find_coordinator",
        "//src/v/redpanda/tests:fixture",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "list_offsets_test",
    timeout = "short",
    srcs = [
        "list_offsets_test.cc",
    ],
    deps = [
        "//src/v/container:chunked_vector",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:list_offset",
        "//src/v/kafka/protocol:produce",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/redpanda/tests:fixture",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "topic_recreate_test",
    timeout = "short",
    srcs = [
        "topic_recreate_test.cc",
    ],
    deps = [
        "//src/v/container:chunked_vector",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:create_topics",
        "//src/v/kafka/protocol:delete_topics",
        "//src/v/kafka/protocol:metadata",
        "//src/v/kafka/protocol:produce",
        "//src/v/kafka/server",
        "//src/v/kafka/server:topic_config_utils",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/redpanda:application",
        "//src/v/redpanda/tests:fixture",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "fetch_session_test",
    timeout = "short",
    srcs = [
        "fetch_session_test.cc",
    ],
    deps = [
        "//src/v/kafka/protocol:fetch",
        "//src/v/kafka/protocol/schemata:fetch_request",
        "//src/v/kafka/server",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/test_utils:seastar_boost",
        "@boost//:range",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "create_partition_test",
    timeout = "short",
    srcs = [
        "create_partition_test.cc",
    ],
    deps = [
        "//src/v/config",
        "//src/v/features:enterprise_feature_messages",
        "//src/v/kafka/protocol",
        "//src/v/kafka/server:topic_config_utils",
        "//src/v/kafka/server/tests:topic_properties_helpers",
        "//src/v/model",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "group_metadata_serialization_test",
    timeout = "short",
    srcs = [
        "group_metadata_serialization_test.cc",
    ],
    deps = [
        "//src/v/bytes",
        "//src/v/kafka/protocol",
        "//src/v/kafka/server",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/reflection:adl",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:random_bytes",
        "//src/v/test_utils:seastar_boost",
        "@boost//:range",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "partition_reassignments_test",
    timeout = "short",
    srcs = [
        "partition_reassignments_test.cc",
    ],
    deps = [
        "//src/v/config",
        "//src/v/container:chunked_vector",
        "//src/v/kafka/protocol:alter_partition_reassignments",
        "//src/v/kafka/protocol:list_partition_reassignments",
        "//src/v/kafka/protocol:metadata",
        "//src/v/kafka/protocol/schemata:alter_partition_reassignments_request",
        "//src/v/kafka/protocol/schemata:list_partition_reassignments_request",
        "//src/v/kafka/server",
        "//src/v/kafka/server:topic_config_utils",
        "//src/v/model",
        "//src/v/redpanda/tests:fixture",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "replicated_partition_test",
    timeout = "short",
    srcs = [
        "replicated_partition_test.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/kafka/data:partition_proxy",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/redpanda/tests:fixture",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "metadata_test",
    timeout = "short",
    srcs = [
        "metadata_test.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/kafka/client:transport",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:create_topics",
        "//src/v/kafka/protocol:metadata",
        "//src/v/kafka/protocol:sasl_authenticate",
        "//src/v/kafka/protocol:sasl_handshake",
        "//src/v/kafka/server",
        "//src/v/kafka/server:security",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/redpanda/tests:fixture",
        "//src/v/security",
        "//src/v/test_utils:random_bytes",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "quota_manager_test",
    timeout = "short",
    srcs = [
        "quota_manager_test.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/kafka/server",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "client_quota_translator_test",
    timeout = "short",
    srcs = [
        "client_quota_translator_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/kafka/server",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "alter_config_test",
    timeout = "moderate",
    srcs = [
        "alter_config_test.cc",
    ],
    cpu = "1",
    deps = [
        "//src/v/config",
        "//src/v/container:chunked_vector",
        "//src/v/features:enterprise_feature_messages",
        "//src/v/kafka/client",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:alter_configs",
        "//src/v/kafka/protocol:create_topics",
        "//src/v/kafka/protocol:describe_configs",
        "//src/v/kafka/protocol:incremental_alter_configs",
        "//src/v/kafka/protocol:metadata",
        "//src/v/kafka/server",
        "//src/v/kafka/server:topic_config_utils",
        "//src/v/kafka/server/tests:topic_properties_helpers",
        "//src/v/model",
        "//src/v/test_utils:scoped_config",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "produce_consume_test",
    timeout = "moderate",
    srcs = [
        "produce_consume_test.cc",
    ],
    cpu = "1",
    deps = [
        ":kafka_test_utils",
        "//src/v/container:chunked_vector",
        "//src/v/kafka/client:transport",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:fetch",
        "//src/v/kafka/protocol:offset_for_leader_epoch",
        "//src/v/kafka/protocol:produce",
        "//src/v/kafka/server",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/redpanda/tests:fixture",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:scoped_config",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "group_tx_compaction_test",
    timeout = "short",
    srcs = [
        "group_tx_compaction_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/config",
        "//src/v/kafka/server",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/raft",
        "//src/v/redpanda/tests:fixture",
        "//src/v/ssx:semaphore",
        "//src/v/storage",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "fetch_metadata_cache_test",
    timeout = "short",
    srcs = [
        "fetch_metadata_cache_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/base",
        "//src/v/kafka/server",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/redpanda/tests:fixture",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_btest(
    name = "fetch_test",
    timeout = "short",
    srcs = [
        "fetch_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/cluster/tests:cluster_test_fixture",
        "//src/v/container:chunked_vector",
        "//src/v/kafka/protocol",
        "//src/v/kafka/server",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/redpanda/tests:fixture",
        "//src/v/storage/tests:disk_log_builder",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "datalake_throttle_manager_test",
    timeout = "short",
    srcs = [
        "datalake_throttle_manager_test.cc",
    ],
    cpu = 3,
    deps = [
        "//src/v/config",
        "//src/v/kafka/server:datalake_throttle_manager",
        "//src/v/random:generators",
        "//src/v/ssx:future_util",
        "//src/v/test_utils:gtest",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "write_at_offset_stm_test",
    timeout = "moderate",
    srcs = [
        "write_at_offset_stm_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/config",
        "//src/v/kafka/data:log_reader_config",
        "//src/v/kafka/server:write_at_offset_stm",
        "//src/v/model/tests:random",
        "//src/v/raft",
        "//src/v/raft/tests:stm_test_fixture",
        "//src/v/redpanda/tests:fixture",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "@googletest//:gtest",
    ],
)

redpanda_cc_bench(
    name = "kafka_fetch_rpbench",
    srcs = [
        "fetch_bench.cc",
    ],
    cpu = 2,
    memory = "4GiB",
    deps = [
        ":kafka_test_utils",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/kafka/server",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/redpanda/tests:fixture",
        "//src/v/test_utils:async",
        "//src/v/test_utils:scoped_config",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_bench(
    name = "kafka_fetch_plan_rpbench",
    srcs = [
        "fetch_plan_bench.cc",
    ],
    memory = "4GiB",
    deps = [
        "//src/v/base",
        "//src/v/kafka/client:types",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:fetch",
        "//src/v/kafka/protocol/schemata:fetch_request",
        "//src/v/kafka/server",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/redpanda/tests:fixture",
        "//src/v/security",
        "@fmt",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_bench(
    name = "kafka_produce_partition_rpbench",
    srcs = [
        "produce_partition_bench.cc",
    ],
    memory = "4GiB",
    redirect_stderr = True,
    deps = [
        "//src/v/container:chunked_vector",
        "//src/v/kafka/protocol/schemata:produce_request",
        "//src/v/kafka/server",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/redpanda/tests:fixture",
        "//src/v/test_utils:async",
        "@boost//:test",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_bench(
    name = "quota_manager_rpbench",
    srcs = [
        "quota_manager_bench.cc",
    ],
    cpu = 2,
    duration = 1,
    memory = "4GiB",
    runs = 1,
    deps = [
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/kafka/server",
        "@fmt",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_btest(
    name = "describe_user_scram_credentials_test",
    timeout = "short",
    srcs = [
        "describe_user_scram_credentials_test.cc",
    ],
    env = {"RP_FIXTURE_ENV": "1"},
    deps = [
        "//src/v/cluster",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:describe_user_scram_credentials",
        "//src/v/kafka/server",
        "//src/v/kafka/server:security",
        "//src/v/model",
        "//src/v/redpanda/tests:fixture",
        "//src/v/security",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "alter_user_scram_credentials_test",
    timeout = "short",
    srcs = [
        "alter_user_scram_credentials_test.cc",
    ],
    env = {"RP_FIXTURE_ENV": "1"},
    deps = [
        "//src/v/cluster",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:alter_user_scram_credentials",
        "//src/v/kafka/server",
        "//src/v/kafka/server:security",
        "//src/v/model",
        "//src/v/redpanda/tests:fixture",
        "//src/v/security",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "describe_acls_test",
    timeout = "short",
    srcs = [
        "describe_acls_test.cc",
    ],
    env = {"RP_FIXTURE_ENV": "1"},
    deps = [
        "//src/v/cluster",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:describe_acls",
        "//src/v/kafka/server",
        "//src/v/model",
        "//src/v/redpanda/tests:fixture",
        "//src/v/security",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "produce_invalid_batch_test",
    timeout = "short",
    srcs = [
        "produce_invalid_batch_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/config",
        "//src/v/container:chunked_vector",
        "//src/v/kafka/client:transport",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:metadata",
        "//src/v/kafka/protocol:produce",
        "//src/v/model",
        "//src/v/model/tests:raw_record_batch_factory",
        "//src/v/redpanda/tests:fixture",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:scoped_config",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "in_flight_requests_test",
    timeout = "short",
    srcs = [
        "in_flight_requests_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/kafka/server",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

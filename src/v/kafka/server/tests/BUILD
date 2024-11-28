load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "config_response_utils_test_help",
    hdrs = [
        "config_response_utils_test_help.h",
    ],
    implementation_deps = [
    ],
    include_prefix = "kafka/server/tests",
    deps = [
        "//src/v/kafka/protocol:describe_configs",
        "//src/v/kafka/server",
    ],
)

redpanda_test_cc_library(
    name = "client_quota_test_helpers",
    hdrs = [
        "client_quota_test_helpers.h",
    ],
    implementation_deps = [
    ],
    include_prefix = "kafka/server/tests",
    deps = [
        "//src/v/base",
    ],
)

redpanda_test_cc_library(
    name = "topic_properties_helpers",
    hdrs = [
        "topic_properties_helpers.h",
    ],
    implementation_deps = [
    ],
    include_prefix = "kafka/server/tests",
    deps = [
        "//src/v/cluster",
        "//src/v/redpanda/tests:fixture_btest",
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
    include_prefix = "kafka/server/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:fragmented_vector",
        "//src/v/kafka/client",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:delete_records",
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
        "//src/v/pandaproxy:subject_name_strategy",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "fetch_unit_test",
    timeout = "short",
    srcs = [
        "fetch_unit_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/kafka/server",
        "//src/v/ssx:semaphore",
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
    name = "consumer_group_recovery_test",
    timeout = "short",
    srcs = [
        "consumer_group_recovery_test.cc",
    ],
    deps = [
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
    tags = ["exclusive"],
    deps = [
        "//src/v/container:fragmented_vector",
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
    name = "group_test",
    timeout = "short",
    srcs = [
        "group_test.cc",
    ],
    tags = ["exclusive"],
    deps = [
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/container:fragmented_vector",
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
    tags = ["exclusive"],
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
    tags = ["exclusive"],
    deps = [
        "//src/v/container:fragmented_vector",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:create_topics",
        "//src/v/kafka/protocol:delete_topics",
        "//src/v/kafka/protocol:metadata",
        "//src/v/kafka/protocol/schemata:create_topics_request",
        "//src/v/kafka/server",
        "//src/v/redpanda:application",
        "//src/v/redpanda/tests:fixture_btest",
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
    tags = ["exclusive"],
    deps = [
        "//src/v/kafka/protocol:offset_fetch",
        "//src/v/redpanda/tests:fixture_btest",
        "//src/v/resource_mgmt:io_priority",
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
    tags = ["exclusive"],
    deps = [
        "//src/v/kafka/server",
        "//src/v/redpanda/tests:fixture_btest",
        "//src/v/resource_mgmt:io_priority",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "usage_manager_test",
    timeout = "short",
    srcs = [
        "usage_manager_test.cc",
    ],
    tags = ["exclusive"],
    deps = [
        "//src/v/base",
        "//src/v/kafka/server",
        "//src/v/storage/tests:kvstore_fixture",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "create_topics_test",
    timeout = "short",
    srcs = [
        "create_topics_test.cc",
    ],
    tags = ["exclusive"],
    deps = [
        "//src/v/config",
        "//src/v/container:fragmented_vector",
        "//src/v/features:enterprise_feature_messages",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:create_topics",
        "//src/v/kafka/protocol:metadata",
        "//src/v/kafka/server",
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
    tags = ["exclusive"],
    deps = [
        "//src/v/kafka/protocol:find_coordinator",
        "//src/v/redpanda/tests:fixture_btest",
        "//src/v/resource_mgmt:io_priority",
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
    tags = ["exclusive"],
    deps = [
        "//src/v/container:fragmented_vector",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:list_offset",
        "//src/v/kafka/protocol:produce",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/redpanda/tests:fixture_btest",
        "//src/v/resource_mgmt:io_priority",
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
    tags = ["exclusive"],
    deps = [
        "//src/v/container:fragmented_vector",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:create_topics",
        "//src/v/kafka/protocol:delete_topics",
        "//src/v/kafka/protocol:metadata",
        "//src/v/kafka/protocol:produce",
        "//src/v/kafka/server",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/redpanda:application",
        "//src/v/redpanda/tests:fixture_btest",
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
    tags = ["exclusive"],
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
    tags = ["exclusive"],
    deps = [
        "//src/v/config",
        "//src/v/features:enterprise_feature_messages",
        "//src/v/kafka/protocol",
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
    tags = ["exclusive"],
    deps = [
        "//src/v/bytes",
        "//src/v/bytes:random",
        "//src/v/kafka/protocol",
        "//src/v/kafka/server",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/reflection:adl",
        "//src/v/storage:record_batch_builder",
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
    tags = ["exclusive"],
    deps = [
        "//src/v/config",
        "//src/v/container:fragmented_vector",
        "//src/v/kafka/protocol:alter_partition_reassignments",
        "//src/v/kafka/protocol:list_partition_reassignments",
        "//src/v/kafka/protocol:metadata",
        "//src/v/kafka/protocol/schemata:alter_partition_reassignments_request",
        "//src/v/kafka/protocol/schemata:list_partition_reassignments_request",
        "//src/v/kafka/server",
        "//src/v/model",
        "//src/v/redpanda/tests:fixture_btest",
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
    tags = ["exclusive"],
    deps = [
        "//src/v/cluster",
        "//src/v/kafka/data:partition_proxy",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/redpanda/tests:fixture_btest",
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
    tags = ["exclusive"],
    deps = [
        "//src/v/cluster",
        "//src/v/kafka/client",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:create_topics",
        "//src/v/kafka/protocol:metadata",
        "//src/v/kafka/protocol:sasl_authenticate",
        "//src/v/kafka/protocol:sasl_handshake",
        "//src/v/kafka/server",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/redpanda/tests:fixture_btest",
        "//src/v/security",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/algorithm:container",
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
    tags = ["exclusive"],
    deps = [
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/kafka/server",
        "//src/v/kafka/server/tests:client_quota_test_helpers",
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
    tags = ["exclusive"],
    deps = [
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/kafka/server",
        "//src/v/kafka/server/tests:client_quota_test_helpers",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "alter_config_test",
    timeout = "short",
    srcs = [
        "alter_config_test.cc",
    ],
    cpu = "1",
    tags = ["exclusive"],
    deps = [
        "//src/v/config",
        "//src/v/container:fragmented_vector",
        "//src/v/features:enterprise_feature_messages",
        "//src/v/kafka/client",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:alter_configs",
        "//src/v/kafka/protocol:create_topics",
        "//src/v/kafka/protocol:describe_configs",
        "//src/v/kafka/protocol:incremental_alter_configs",
        "//src/v/kafka/protocol:metadata",
        "//src/v/kafka/server",
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
    tags = ["exclusive"],
    deps = [
        ":kafka_test_utils",
        "//src/v/container:fragmented_vector",
        "//src/v/kafka/client",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:fetch",
        "//src/v/kafka/protocol:produce",
        "//src/v/kafka/server",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/redpanda/tests:fixture_btest",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "group_tx_compaction_test",
    timeout = "moderate",
    srcs = [
        "group_tx_compaction_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/config",
        "//src/v/kafka/server",
        "//src/v/model/tests:random",
        "//src/v/raft",
        "//src/v/redpanda/tests:fixture_gtest",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "fetch_test",
    timeout = "short",
    srcs = [
        "fetch_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/kafka/protocol",
        "//src/v/kafka/server",
        "//src/v/model",
        "//src/v/redpanda/tests:fixture_btest",
        "//src/v/resource_mgmt:io_priority",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

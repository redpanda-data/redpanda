load(
    "//bazel:test.bzl",
    "redpanda_cc_btest",
    "redpanda_cc_gtest",
    "redpanda_test_cc_library",
)

redpanda_test_cc_library(
    name = "s3_imposter_gtest",
    srcs = [
        "s3_imposter.cc",
    ],
    hdrs = [
        "s3_imposter.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/test_utils:gtest",
    ],
    include_prefix = "cloud_storage/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_storage_clients",
        "//src/v/config",
        "//src/v/http/tests:utils",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@boost//:algorithm",
        "@boost//:core",
        "@boost//:property_tree",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "s3_imposter_btest",
    srcs = [
        "s3_imposter.cc",
    ],
    hdrs = [
        "s3_imposter.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/test_utils:seastar_boost",
    ],
    include_prefix = "cloud_storage/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_storage_clients",
        "//src/v/config",
        "//src/v/http/tests:utils",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@boost//:algorithm",
        "@boost//:core",
        "@boost//:property_tree",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "read_replica_e2e_fixture_btest",
    hdrs = [
        "read_replica_e2e_fixture.h",
    ],
    implementation_deps = [
    ],
    include_prefix = "cloud_storage/tests",
    deps = [
        "//src/v/cloud_storage/tests:s3_imposter_btest",
        "//src/v/redpanda/tests:fixture_btest",
        "//src/v/test_utils:scoped_config",
    ],
)

redpanda_test_cc_library(
    name = "read_replica_e2e_fixture_gtest",
    hdrs = [
        "read_replica_e2e_fixture.h",
    ],
    implementation_deps = [
    ],
    include_prefix = "cloud_storage/tests",
    deps = [
        "//src/v/cloud_storage/tests:s3_imposter_gtest",
        "//src/v/redpanda/tests:fixture_gtest",
        "//src/v/test_utils:scoped_config",
    ],
)

redpanda_test_cc_library(
    name = "common_btest",
    srcs = ["util.cc"],
    hdrs = [
        "cache_test_fixture.h",
        "cloud_storage_fixture.h",
        "common_def.h",
        "manual_fixture.h",
        "produce_utils.h",
        "util.h",
    ],
    include_prefix = "cloud_storage/tests",
    deps = [
        ":s3_imposter_btest",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/bytes:streambuf",
        "//src/v/cloud_storage",
        "//src/v/cluster",
        "//src/v/cluster/cloud_metadata/tests:manual_mixin",
        "//src/v/kafka/server/tests:kafka_test_utils",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/redpanda/tests:fixture_btest",
        "//src/v/storage",
        "//src/v/storage:record_batch_utils",
        "//src/v/storage/tests:disk_log_builder",
        "//src/v/test_utils:scoped_config",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:lazy_abort_source",
        "//src/v/utils:stream_provider",
        "@boost//:numeric_conversion",
        "@boost//:test",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "common_gtest",
    srcs = ["util.cc"],
    hdrs = [
        "cache_test_fixture.h",
        "cloud_storage_fixture.h",
        "common_def.h",
        "manual_fixture.h",
        "produce_utils.h",
        "util.h",
    ],
    include_prefix = "cloud_storage/tests",
    deps = [
        ":s3_imposter_gtest",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/bytes:streambuf",
        "//src/v/cloud_storage",
        "//src/v/cluster",
        "//src/v/cluster/cloud_metadata/tests:manual_mixin",
        "//src/v/kafka/server/tests:kafka_test_utils",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/redpanda/tests:fixture_gtest",
        "//src/v/storage",
        "//src/v/storage:record_batch_utils",
        "//src/v/storage/tests:disk_log_builder",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "//src/v/utils:lazy_abort_source",
        "//src/v/utils:stream_provider",
        "@boost//:numeric_conversion",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "segment_meta_cstore_test",
    timeout = "short",
    srcs = [
        "segment_meta_cstore_test.cc",
    ],
    deps = [
        ":common_btest",
        "//src/v/base",
        "//src/v/cloud_storage",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/utils:delta_for",
        "//src/v/utils:human",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "segment_chunk_test",
    timeout = "short",
    srcs = [
        "segment_chunk_test.cc",
    ],
    deps = [
        "//src/v/cloud_storage",
        "//src/v/test_utils:seastar_boost",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "partition_manifest_test",
    timeout = "short",
    srcs = [
        "partition_manifest_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/bytes:iostream",
        "//src/v/cloud_storage",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:tracking_allocator",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "directory_walker_test",
    timeout = "short",
    srcs = [
        "directory_walker_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/cloud_storage",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "remote_segment_index_test",
    timeout = "short",
    srcs = [
        "remote_segment_index_test.cc",
    ],
    deps = [
        ":common_btest",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/cloud_storage",
        "//src/v/model",
        "//src/v/random:generators",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "recovery_request_test",
    timeout = "short",
    srcs = [
        "recovery_request_test.cc",
    ],
    deps = [
        "//src/v/cloud_storage",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "materialized_manifest_cache_test",
    timeout = "short",
    srcs = [
        "materialized_manifest_cache_test.cc",
    ],
    deps = [
        ":common_btest",
        ":s3_imposter_btest",
        "//src/v/bytes:iostream",
        "//src/v/cloud_storage",
        "//src/v/model",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:retry_chain_node",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "remote_path_provider_test",
    timeout = "short",
    srcs = [
        "remote_path_provider_test.cc",
    ],
    cpu = 1,
    tags = ["exclusive"],
    deps = [
        "//src/v/cloud_storage",
        "//src/v/cloud_storage:topic_mount_manifest_path",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/test_utils:gtest",
        "//src/v/utils:uuid",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "topic_mount_manifest_path_test",
    timeout = "short",
    srcs = [
        "topic_mount_manifest_path_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cloud_storage:topic_mount_manifest_path",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "//src/v/utils:uuid",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "topic_mount_handler_test",
    timeout = "short",
    srcs = [
        "topic_mount_handler_test.cc",
    ],
    cpu = 1,
    tags = ["exclusive"],
    deps = [
        ":s3_imposter_gtest",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage_clients",
        "//src/v/cluster:topic_configuration",
        "//src/v/config",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "//src/v/utils:retry_chain_node",
        "//src/v/utils:uuid",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "cache_test",
    timeout = "short",
    srcs = [
        "cache_test.cc",
        "cache_test_fixture.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/cloud_storage",
        "//src/v/config",
        "//src/v/random:generators",
        "//src/v/test_utils:iostream",
        "//src/v/test_utils:scoped_config",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:directory_walker",
        "//src/v/utils:file_io",
        "//src/v/utils:human",
        "@boost//:filesystem",
        "@boost//:test",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "tx_range_manifest_test",
    timeout = "short",
    srcs = [
        "tx_range_manifest_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/bytes:iostream",
        "//src/v/cloud_storage",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "topic_mount_manifest_test",
    timeout = "short",
    srcs = [
        "topic_mount_manifest_test.cc",
    ],
    deps = [
        "//src/v/cloud_storage",
        "//src/v/cloud_storage:remote_label",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "topic_manifest_test",
    timeout = "short",
    srcs = [
        "topic_manifest_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/bytes:iostream",
        "//src/v/bytes:streambuf",
        "//src/v/cloud_storage",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/test_utils:random",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "remote_file_test",
    timeout = "short",
    srcs = [
        "remote_file_test.cc",
    ],
    cpu = 1,
    tags = ["exclusive"],
    deps = [
        "//src/v/bytes:iostream",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage/tests:common_btest",
        "//src/v/cloud_storage/tests:s3_imposter_btest",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:lazy_abort_source",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "remote_segment_test",
    timeout = "short",
    srcs = [
        "remote_segment_test.cc",
    ],
    cpu = 1,
    tags = ["exclusive"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage/tests:common_btest",
        "//src/v/model",
        "//src/v/storage",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:lazy_abort_source",
        "//src/v/utils:retry_chain_node",
        "//src/v/utils:stream_provider",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "remote_partition_test",
    timeout = "short",
    srcs = [
        "remote_partition_test.cc",
    ],
    cpu = 1,
    tags = ["exclusive"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/bytes:iostream",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage/tests:common_btest",
        "//src/v/cloud_storage/tests:s3_imposter_btest",
        "//src/v/cloud_storage_clients",
        "//src/v/config",
        "//src/v/model",
        "//src/v/ssx:future_util",
        "//src/v/storage",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:retry_chain_node",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "async_manifest_view_test",
    timeout = "short",
    srcs = [
        "async_manifest_view_test.cc",
    ],
    cpu = 1,
    tags = ["exclusive"],
    deps = [
        "//src/v/bytes:iostream",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage/tests:common_btest",
        "//src/v/cloud_storage/tests:s3_imposter_btest",
        "//src/v/model",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:retry_chain_node",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "anomalies_detector_test",
    timeout = "short",
    srcs = [
        "anomalies_detector_test.cc",
    ],
    cpu = 1,
    tags = ["exclusive"],
    deps = [
        "//src/v/bytes:iostream",
        "//src/v/cloud_storage",
        "//src/v/config",
        "//src/v/hashing:xx",
        "//src/v/http/tests:utils",
        "//src/v/test_utils:scoped_config",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "partition_manifest_downloader_test",
    timeout = "short",
    srcs = [
        "partition_manifest_downloader_test.cc",
    ],
    cpu = 1,
    tags = ["exclusive"],
    deps = [
        "//src/v/bytes:iostream",
        "//src/v/bytes:streambuf",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage/tests:s3_imposter_gtest",
        "//src/v/cloud_storage_clients",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "remote_test",
    timeout = "short",
    srcs = [
        "remote_test.cc",
    ],
    cpu = 1,
    tags = ["exclusive"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/bytes:iostream",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage/tests:common_gtest",
        "//src/v/cloud_storage/tests:s3_imposter_gtest",
        "//src/v/cloud_storage_clients",
        "//src/v/config",
        "//src/v/model",
        "//src/v/storage",
        "//src/v/test_utils:gtest",
        "//src/v/utils:lazy_abort_source",
        "//src/v/utils:retry_chain_node",
        "//src/v/utils:stream_provider",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "topic_manifest_downloader_test",
    timeout = "short",
    srcs = [
        "topic_manifest_downloader_test.cc",
    ],
    cpu = 1,
    tags = ["exclusive"],
    deps = [
        "//src/v/bytes:iostream",
        "//src/v/bytes:streambuf",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage/tests:s3_imposter_gtest",
        "//src/v/cloud_storage_clients",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "topic_namespace_override_recovery_test",
    timeout = "short",
    srcs = [
        "topic_namespace_override_recovery_test.cc",
    ],
    cpu = 1,
    tags = ["exclusive"],
    deps = [
        "//src/v/cloud_storage",
        "//src/v/cloud_storage/tests:common_gtest",
        "//src/v/cloud_storage/tests:s3_imposter_gtest",
        "//src/v/cluster",
        "//src/v/cluster/tests:topic_properties_generator",
        "//src/v/config",
        "//src/v/kafka/server/tests:kafka_test_utils",
        "//src/v/model",
        "//src/v/redpanda:application",
        "//src/v/redpanda/tests:fixture_gtest",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "//src/v/utils:retry_chain_node",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "remote_partition_fuzz_test",
    timeout = "moderate",
    srcs = [
        "remote_partition_fuzz_test.cc",
    ],
    cpu = 1,
    tags = ["exclusive"],
    deps = [
        "//src/v/cloud_storage",
        "//src/v/cloud_storage/tests:common_btest",
        "//src/v/cloud_storage/tests:s3_imposter_btest",
        "//src/v/model",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

load(
    "//bazel:test.bzl",
    "redpanda_cc_bench",
    "redpanda_cc_btest",
    "redpanda_cc_fuzz_test",
    "redpanda_cc_gtest",
    "redpanda_test_cc_library",
)

redpanda_test_cc_library(
    name = "read_replica_e2e_fixture",
    hdrs = [
        "read_replica_e2e_fixture.h",
    ],
    implementation_deps = [
    ],
    deps = [
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/redpanda/tests:fixture",
        "//src/v/test_utils:scoped_config",
    ],
)

redpanda_test_cc_library(
    name = "common",
    srcs = ["util.cc"],
    hdrs = [
        "cloud_storage_fixture.h",
        "common_def.h",
        "manual_fixture.h",
        "produce_utils.h",
        "util.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/bytes:streambuf",
        "//src/v/cloud_io:cache",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage:types",
        "//src/v/cluster",
        "//src/v/cluster/cloud_metadata/tests:manual_mixin",
        "//src/v/container:chunked_vector",
        "//src/v/json",
        "//src/v/kafka/server/tests:kafka_test_utils",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/redpanda/tests:fixture",
        "//src/v/storage",
        "//src/v/storage:disk",
        "//src/v/storage:record_batch_utils",
        "//src/v/storage/tests:disk_log_builder",
        "//src/v/test_utils:scoped_config",
        "//src/v/test_utils:test_macros",
        "//src/v/test_utils:tmp_dir",
        "//src/v/utils:lazy_abort_source",
        "//src/v/utils:stream_provider",
        "@boost//:numeric_conversion",
        "@rapidjson",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "segment_meta_cstore_test",
    timeout = "moderate",
    srcs = [
        "segment_meta_cstore_test.cc",
    ],
    deps = [
        ":common",
        "//src/v/base",
        "//src/v/cloud_storage:segment_meta_cstore",
        "//src/v/cloud_storage:types",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/test_utils:seastar_boost",
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
        "//src/v/cloud_storage:types",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:to_string",
        "//src/v/utils:tracking_allocator",
        "@boost//:test",
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
        ":common",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/cloud_storage",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/test_utils:seastar_boost",
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
        ":common",
        "//src/v/bytes:iostream",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage:types",
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
    deps = [
        "//src/v/cloud_storage",
        "//src/v/cloud_storage:topic_mount_manifest_path",
        "//src/v/cloud_storage:topic_path_utils",
        "//src/v/cloud_storage:types",
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
    deps = [
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_io/tests:scoped_remote",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage:types",
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
        "//src/v/cloud_storage:types",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_bench(
    name = "tx_range_manifest_rpbench",
    srcs = ["tx_range_manifest_bench.cc"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/cloud_storage",
        "//src/v/model",
        "//src/v/random:generators",
        "@boost//:test",
        "@seastar",
        "@seastar//:benchmark",
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
        "//src/v/cloud_storage:types",
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
        "//src/v/cloud_storage:topic_path_utils",
        "//src/v/cloud_storage:types",
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
    deps = [
        "//src/v/bytes:iostream",
        "//src/v/cloud_io:cache",
        "//src/v/cloud_io/tests:cloud_io_cache_fixture",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_io/tests:scoped_remote",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage/tests:common",
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
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/cloud_io:cache",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage:types",
        "//src/v/cloud_storage/tests:common",
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
    timeout = "moderate",
    srcs = [
        "remote_partition_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/bytes:iostream",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage:types",
        "//src/v/cloud_storage/tests:common",
        "//src/v/cloud_storage_clients",
        "//src/v/config",
        "//src/v/model",
        "//src/v/ssx:future_util",
        "//src/v/storage",
        "//src/v/test_utils:scoped_config",
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
    deps = [
        "//src/v/bytes:iostream",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage:types",
        "//src/v/cloud_storage/tests:common",
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
    deps = [
        "//src/v/bytes:iobuf_parser",
        "//src/v/bytes:iostream",
        "//src/v/cloud_io/tests:scoped_remote",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage:types",
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
    deps = [
        "//src/v/bytes:iostream",
        "//src/v/bytes:streambuf",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_io/tests:scoped_remote",
        "//src/v/cloud_storage",
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
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/bytes:iostream",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_io/tests:scoped_remote",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage:types",
        "//src/v/cloud_storage/tests:common",
        "//src/v/cloud_storage_clients",
        "//src/v/config",
        "//src/v/model",
        "//src/v/storage",
        "//src/v/test_utils:gtest",
        "//src/v/utils:lazy_abort_source",
        "//src/v/utils:retry_chain_node",
        "//src/v/utils:stream_provider",
        "@boost//:range",
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
    deps = [
        ":common",
        "//src/v/bytes:iostream",
        "//src/v/bytes:streambuf",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_io/tests:scoped_remote",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage:topic_path_utils",
        "//src/v/cloud_storage:types",
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
    deps = [
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage:types",
        "//src/v/cloud_storage/tests:common",
        "//src/v/cluster",
        "//src/v/cluster/tests:topic_properties_generator",
        "//src/v/config",
        "//src/v/kafka/server/tests:kafka_test_utils",
        "//src/v/model",
        "//src/v/redpanda:application",
        "//src/v/redpanda/tests:fixture",
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
    timeout = "long",
    srcs = [
        "remote_partition_fuzz_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cloud_storage",
        "//src/v/cloud_storage:types",
        "//src/v/cloud_storage/tests:common",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_bench(
    name = "cloud_storage_rpbench",
    srcs = [
        "segment_meta_cstore_bench.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cloud_storage:segment_meta_cstore",
        "//src/v/config",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/ssx:sformat",
        "//src/v/utils:delta_for",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_fuzz_test(
    name = "segment_meta_cstore_fuzz_rpfixture",
    timeout = "short",
    srcs = ["segment_meta_cstore_fuzz.cc"],
    args = [
        "-max_total_time=30",
        "-rss_limit_mb=8192",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/cloud_storage:segment_meta_cstore",
        "//src/v/cloud_storage:types",
        "//src/v/model",
        "//src/v/reflection:to_tuple",
        "//src/v/serde",
        "@abseil-cpp//absl/container:btree",
        "@fmt",
    ],
)

redpanda_cc_gtest(
    name = "cloud_storage_e2e_test",
    timeout = "moderate",
    srcs = ["cloud_storage_e2e_test.cc"],
    cpu = 1,
    deps = [
        ":common",
        ":read_replica_e2e_fixture",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cluster",
        "//src/v/cluster/cloud_metadata/tests:manual_mixin",
        "//src/v/kafka/client:transport",
        "//src/v/kafka/data:partition_proxy",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:fetch",
        "//src/v/kafka/protocol:find_coordinator",
        "//src/v/kafka/server/tests:kafka_test_utils",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/redpanda/tests:fixture",
        "//src/v/storage",
        "//src/v/storage:disk",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "@boost//:algorithm",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "delete_records_e2e_test",
    timeout = "moderate",
    srcs = [
        "delete_records_e2e_test.cc",
    ],
    cpu = 1,
    deps = [
        ":common",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_storage",
        "//src/v/cluster",
        "//src/v/kafka/server/tests:kafka_test_utils",
        "//src/v/model",
        "//src/v/redpanda/tests:fixture",
        "//src/v/test_utils:scoped_config",
        "//src/v/test_utils:seastar_boost",
        "@boost//:algorithm",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "read_replica_test",
    timeout = "short",
    srcs = [
        "read_replica_test.cc",
    ],
    cpu = 1,
    deps = [
        ":common",
        ":read_replica_e2e_fixture",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage:types",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/kafka/server/tests:kafka_test_utils",
        "//src/v/model",
        "//src/v/redpanda/tests:fixture",
        "//src/v/storage",
        "//src/v/test_utils:scoped_config",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "segment_chunk_hydration_test",
    timeout = "moderate",
    srcs = [
        "segment_chunk_hydration_test.cc",
    ],
    cpu = 1,
    deps = [
        ":common",
        ":read_replica_e2e_fixture",
        "//src/v/bytes:iostream",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage:types",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/test_utils:scoped_config",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:lazy_abort_source",
        "//src/v/utils:stream_provider",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "topic_recovery_service_test",
    timeout = "short",
    srcs = [
        "topic_recovery_service_test.cc",
    ],
    cpu = 1,
    deps = [
        ":common",
        ":read_replica_e2e_fixture",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_storage",
        "//src/v/cluster",
        "//src/v/cluster/cloud_metadata/tests:manual_mixin",
        "//src/v/container:chunked_vector",
        "//src/v/redpanda/tests:fixture",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:memory_data_source",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

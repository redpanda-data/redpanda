load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_btest", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "storage_e2e_fixture",
    hdrs = [
        "storage_e2e_fixture.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/kafka/server/tests:kafka_test_utils",
        "//src/v/redpanda/tests:fixture",
        "//src/v/storage",
        "//src/v/test_utils:scoped_config",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "manual_mixin",
    hdrs = [
        "manual_mixin.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/test_utils:scoped_config",
    ],
)

redpanda_test_cc_library(
    name = "kvstore_fixture",
    hdrs = [
        "kvstore_fixture.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/config",
        "//src/v/features",
        "//src/v/random:generators",
        "//src/v/storage",
        "@googletest//:gtest",
    ],
)

redpanda_test_cc_library(
    name = "random",
    hdrs = [
        "randoms.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/storage",
    ],
)

redpanda_test_cc_library(
    name = "disk_log_builder_fixture",
    hdrs = [
        "disk_log_builder_fixture.h",
    ],
    deps = [
        ":disk_log_builder",
        "@googletest//:gtest",
    ],
)

redpanda_test_cc_library(
    name = "disk_log_builder",
    srcs = [
        "utils/disk_log_builder.cc",
    ],
    hdrs = [
        "utils/disk_log_builder.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/compaction:types",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/features",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/ssx:sformat",
        "//src/v/storage",
        "//src/v/test_utils:test_env",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "common",
    hdrs = [
        "common.h",
    ],
    deps = [
        "//src/v/storage",
    ],
)

redpanda_test_cc_library(
    name = "batch_generators",
    hdrs = [
        "batch_generators.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/compression",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/hashing:crc32c",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/reflection:adl",
        "//src/v/test_utils:test_macros",
        "//src/v/utils:vint",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "storage_test_fixture_gtest",
    hdrs = [
        "storage_test_fixture.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":batch_generators",
        "//src/v/config",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/features",
        "//src/v/storage",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:test_env",
    ],
)

redpanda_test_cc_library(
    name = "storage_test_fixture",
    hdrs = [
        "storage_test_fixture.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":batch_generators",
        "//src/v/config",
        "//src/v/features",
        "//src/v/storage",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:test_env",
        "@googletest//:gtest",
    ],
)

redpanda_test_cc_library(
    name = "log_gap_analysis",
    srcs = [
        "utils/log_gap_analysis.cc",
    ],
    hdrs = [
        "utils/log_gap_analysis.h",
    ],
    deps = [
        "//src/v/model",
        "//src/v/model:batch_compression",
    ],
)

redpanda_cc_gtest(
    name = "segment_offset_tracker_test",
    timeout = "short",
    srcs = [
        "segment_offset_tracker_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/features",
        "//src/v/model",
        "//src/v/storage",
        "//src/v/storage:resources",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "log_segment_appender_test",
    timeout = "short",
    srcs = [
        "log_segment_appender_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/config",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/storage:chunk_cache",
        "//src/v/storage:resources",
        "//src/v/storage:segment_appender",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random_bytes",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "log_manager_test",
    timeout = "short",
    srcs = [
        "log_manager_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/storage:logger",
        "//src/v/storage:record_batch_utils",
        "//src/v/storage:segment_appender",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random_bytes",
        "//src/v/test_utils:test_env",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "index_state_test",
    timeout = "short",
    srcs = [
        "index_state_test.cc",
    ],
    deps = [
        "//src/v/bytes",
        "//src/v/random:generators",
        "//src/v/serde",
        "//src/v/storage:index_state",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "batch_cache_test",
    timeout = "short",
    srcs = [
        "batch_cache_test.cc",
    ],
    deps = [
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/storage:batch_cache",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random_bytes",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "batch_cache_reclaim_test",
    timeout = "short",
    srcs = [
        "batch_cache_reclaim_test.cc",
    ],
    target_compatible_with = select({
        "@seastar//:use_system_allocator": ["@platforms//:incompatible"],
        "//conditions:default": [],
    }),
    deps = [
        "//src/v/model",
        "//src/v/storage:batch_cache",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "kvstore_test",
    timeout = "short",
    srcs = [
        "kvstore_test.cc",
    ],
    deps = [
        ":kvstore_fixture",
        "//src/v/config",
        "//src/v/random:generators",
        "//src/v/reflection:adl",
        "//src/v/storage",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random_bytes",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "appender_chunk_manipulations_test",
    timeout = "short",
    srcs = [
        "appender_chunk_manipulations_test.cc",
    ],
    deps = [
        "//src/v/config",
        "//src/v/random:generators",
        "//src/v/storage:segment_appender",
        "//src/v/test_utils:gtest",
        "@fmt",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "backlog_controller_test",
    timeout = "short",
    srcs = [
        "backlog_controller_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/config",
        "//src/v/storage",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "concat_segment_reader_test",
    timeout = "short",
    srcs = [
        "concat_segment_reader_test.cc",
    ],
    deps = [
        ":disk_log_builder",
        "//src/v/bytes:iostream",
        "//src/v/model/tests:random",
        "//src/v/storage",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "timequery_test",
    timeout = "short",
    srcs = [
        "timequery_test.cc",
    ],
    deps = [
        ":disk_log_builder_fixture",
        "//src/v/config",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "snapshot_test",
    timeout = "short",
    srcs = [
        "snapshot_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random_bytes",
        "@googletest//:gtest",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "record_batch_builder_test",
    timeout = "short",
    srcs = [
        "record_batch_builder_test.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/reflection:adl",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/container:btree",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "produce_consume_test",
    timeout = "short",
    srcs = [
        "produce_consume_test.cc",
    ],
    cpu = 1,
    deps = [
        ":disk_log_builder",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@boost//:range",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "offset_translator_state_test",
    timeout = "short",
    srcs = [
        "offset_translator_state_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/model",
        "//src/v/storage",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "offset_to_filepos_test",
    timeout = "short",
    srcs = [
        "offset_to_filepos_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/storage",
        "//src/v/test_utils:archival",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "offset_index_utils_tests",
    timeout = "short",
    srcs = [
        "offset_index_utils_tests.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:tmpbuf_file",
        "//src/v/utils:file_io",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "offset_translator_tests",
    timeout = "moderate",
    srcs = [
        "offset_translator_tests.cc",
    ],
    cpu = 8,
    deps = [
        "//src/v/container:chunked_circular_buffer",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random_bytes",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "scoped_file_tracker_test",
    timeout = "short",
    srcs = [
        "scoped_file_tracker_test.cc",
    ],
    deps = [
        "//src/v/storage:scoped_file_tracker",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/container:btree",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "segment_deduplication_test",
    timeout = "short",
    srcs = [
        "segment_deduplication_test.cc",
    ],
    deps = [
        ":disk_log_builder",
        ":disk_log_builder_fixture",
        "//src/v/compaction:key_offset_map",
        "//src/v/config",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/storage:chunk_cache",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "segment_concatenation_test",
    timeout = "short",
    srcs = [
        "segment_concatenation_test.cc",
    ],
    cpu = 1,
    deps = [
        ":disk_log_builder",
        ":disk_log_builder_fixture",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/ssx:mutex",
        "//src/v/storage",
        "//src/v/storage:scoped_file_tracker",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "readers_cache_test",
    timeout = "short",
    srcs = [
        "readers_cache_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/config",
        "//src/v/features",
        "//src/v/model",
        "//src/v/storage",
        "//src/v/storage:resources",
        "//src/v/test_utils:gtest",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "storage_e2e_single_thread",
    timeout = "long",
    srcs = [
        "storage_e2e_test.cc",
    ],
    cpu = 1,
    deps = [
        ":batch_generators",
        ":common",
        ":disk_log_builder",
        ":disk_log_builder_fixture",
        ":log_gap_analysis",
        ":storage_test_fixture",
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/compression",
        "//src/v/config",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/features",
        "//src/v/finjector",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/reflection:adl",
        "//src/v/resource_mgmt:memory_groups",
        "//src/v/ssx:future_util",
        "//src/v/storage",
        "//src/v/storage:batch_cache",
        "//src/v/storage:record_batch_builder",
        "//src/v/storage:resources",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
        "//src/v/test_utils:random_bytes",
        "//src/v/test_utils:scoped_config",
        "//src/v/utils:directory_walker",
        "//src/v/utils:to_string",
        "//src/v/utils:tristate",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "segment_size_jitter_test",
    timeout = "short",
    srcs = [
        "segment_size_jitter_test.cc",
    ],
    deps = [
        "//src/v/storage",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "segment_set_test",
    timeout = "short",
    srcs = [
        "segment_set_test.cc",
    ],
    deps = [
        "//src/v/storage",
        "//src/v/storage:resources",
        "//src/v/test_utils:gtest",
        "//src/v/utils:directory_walker",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "log_segment_reader_test",
    timeout = "short",
    srcs = [
        "log_segment_reader_test.cc",
    ],
    deps = [
        "//src/v/container:chunked_circular_buffer",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/storage:logger",
        "//src/v/storage:parser_utils",
        "//src/v/storage:record_batch_utils",
        "//src/v/storage:segment_appender",
        "//src/v/storage/tests:disk_log_builder",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "offset_assignment_test",
    timeout = "short",
    srcs = [
        "offset_assignment_test.cc",
    ],
    deps = [
        "//src/v/model/tests:random",
        "//src/v/storage",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "log_truncate_test",
    timeout = "short",
    srcs = [
        "log_truncate_test.cc",
    ],
    deps = [
        "//src/v/container:chunked_circular_buffer",
        "//src/v/model",
        "//src/v/storage",
        "//src/v/storage/tests:disk_log_builder",
        "//src/v/storage/tests:storage_test_fixture",
        "//src/v/test_utils:gtest",
        "@boost//:range",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "compaction_index_format_test",
    timeout = "short",
    srcs = [
        "compaction_index_format_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf_parser",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/reflection:adl",
        "//src/v/storage",
        "//src/v/storage:logger",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
        "//src/v/test_utils:random_bytes",
        "//src/v/test_utils:tmpbuf_file",
        "//src/v/utils:vint",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "disk_log_builder_test",
    timeout = "short",
    srcs = [
        "disk_log_builder_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/storage",
        "//src/v/storage/tests:disk_log_builder_fixture",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "log_retention_test",
    timeout = "short",
    srcs = [
        "log_retention_test.cc",
    ],
    deps = [
        "//src/v/model",
        "//src/v/storage/tests:disk_log_builder",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "half_page_concurrent_dispatch_test",
    timeout = "short",
    srcs = [
        "half_page_concurrent_dispatch_test.cc",
    ],
    deps = [
        "//src/v/storage",
        "//src/v/storage:record_batch_builder",
        "//src/v/storage/tests:disk_log_builder",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:test_env",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "file_sanitizer_test",
    timeout = "short",
    srcs = [
        "file_sanitizer_test.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/storage",
        "//src/v/storage:file_sanitizer_types",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "compaction_reducer_test",
    timeout = "short",
    srcs = [
        "compaction_reducer_test.cc",
    ],
    deps = [
        "//src/v/compaction:key",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/storage:logger",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random_bytes",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "compaction_reducer_fixture_test",
    timeout = "short",
    srcs = [
        "compaction_reducer_fixture_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/compaction:key_offset_map",
        "//src/v/model",
        "//src/v/storage",
        "//src/v/storage/tests:batch_generators",
        "//src/v/storage/tests:storage_test_fixture",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "batch_consumer_utils_test",
    timeout = "short",
    srcs = [
        "batch_consumer_utils_test.cc",
    ],
    deps = [
        "//src/v/bytes:iostream",
        "//src/v/model/tests:random",
        "//src/v/storage",
        "//src/v/storage/tests:disk_log_builder",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "log_replayer_test",
    timeout = "short",
    srcs = [
        "log_replayer_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/features",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/storage:logger",
        "//src/v/storage:record_batch_utils",
        "//src/v/storage:resources",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "storage_e2e_fixture_test",
    timeout = "moderate",
    srcs = [
        "storage_e2e_fixture_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/container:chunked_circular_buffer",
        "//src/v/kafka/server/tests:kafka_test_utils",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/redpanda/tests:fixture",
        "//src/v/storage",
        "//src/v/storage/tests:storage_e2e_fixture",
        "//src/v/test_utils:scoped_config",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "compaction_e2e_test",
    timeout = "moderate",
    srcs = [
        "compaction_e2e_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/cluster:features",
        "//src/v/cluster/tests:tx_compaction_utils_gtest",
        "//src/v/container:chunked_vector",
        "//src/v/features",
        "//src/v/kafka/server/tests:kafka_test_utils",
        "//src/v/model",
        "//src/v/model:batch_compression",
        "//src/v/random:generators",
        "//src/v/redpanda/tests:fixture",
        "//src/v/storage",
        "//src/v/storage/tests:manual_mixin",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "//src/v/utils:adjustable_semaphore",
        "//src/v/utils:directory_walker",
        "@abseil-cpp//absl/container:btree",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "compaction_e2e_multinode_test",
    timeout = "moderate",
    srcs = [
        "compaction_e2e_multinode_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/cluster/tests:cluster_test_fixture",
        "//src/v/cluster/tests:tx_compaction_utils",
        "//src/v/kafka/server/tests:kafka_test_utils",
        "//src/v/model",
        "//src/v/storage/tests:manual_mixin",
        "//src/v/test_utils:scoped_config",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "compaction_fuzz_test",
    timeout = "moderate",
    srcs = [
        "compaction_fuzz_test.cc",
    ],
    deps = [
        ":disk_log_builder",
        "//src/v/base",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/container:btree",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "segment_appender_test",
    timeout = "short",
    srcs = [
        "segment_appender_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iostream",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/storage:segment_appender",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random_bytes",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_bench(
    name = "storage_rpbench",
    srcs = ["compaction_idx_bench.cc"],
    deps = [
        "//src/v/compaction:key",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/storage:logger",
        "//src/v/test_utils:random_bytes",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:node_hash_map",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_bench(
    name = "segment_appender_rpbench",
    timeout = "moderate",
    srcs = ["segment_appender_bench.cc"],
    deps = [
        "//src/v/model",
        "//src/v/ssx:future_util",
        "//src/v/storage",
        "//src/v/storage:segment_appender",
        "//src/v/test_utils:tmpbuf_file",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

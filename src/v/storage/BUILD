load("//bazel:build.bzl", "redpanda_cc_library")

package(
    default_visibility = [
        "//src/v/storage/tests:__pkg__",
    ],
)

redpanda_cc_library(
    name = "config",
    hdrs = [
        "config.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "parser_utils",
    hdrs = [
        "parser_utils.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf_parser",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "record_batch_builder",
    srcs = [
        "record_batch_builder.cc",
    ],
    hdrs = [
        "record_batch_builder.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":parser_utils",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/compression",
        "//src/v/model",
        "//src/v/model:batch_compression",
        "//src/v/utils:vint",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "logger",
    srcs = [
        "logger.cc",
    ],
    hdrs = [
        "logger.h",
    ],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "index_state",
    srcs = [
        "index_state.cc",
    ],
    hdrs = [
        "index_state.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf_parser",
        "//src/v/hashing:crc32c",
        "//src/v/hashing:xx",
        "//src/v/reflection:adl",
        "//src/v/serde:bool_class",
        "//src/v/serde:iobuf",
        "//src/v/serde:optional",
        "//src/v/serde:vector",
        "//src/v/utils:to_string",
        "@fmt",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/serde",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "batch_cache",
    srcs = [
        "batch_cache.cc",
    ],
    hdrs = [
        "batch_cache.h",
    ],
    implementation_deps = [
        "//src/v/bytes:iobuf_parser",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/ssx:future_util",
        "//src/v/utils:to_string",
        "@fmt",
    ],
    visibility = [
        "//src/v/cloud_topics/batch_cache:__pkg__",
        "//src/v/resource_mgmt/tests:__pkg__",
        "//src/v/storage/tests:__pkg__",
    ],
    deps = [
        "//src/v/base",
        "//src/v/container:intrusive",
        "//src/v/model",
        "//src/v/resource_mgmt:available_memory",
        "//src/v/ssx:async_algorithm",
        "//src/v/ssx:semaphore",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "chunk_cache",
    srcs = [
        "chunk_cache.cc",
    ],
    hdrs = [
        "chunk_cache.h",
        "segment_appender_chunk.h",
    ],
    implementation_deps = [
        "//src/v/config",
        "//src/v/resource_mgmt:memory_groups",
        "@boost//:iterator",
    ],
    visibility = [
        "//src/v/storage/tests:__pkg__",
        # TODO(bazel) chunk cache could probably be instantiated in
        # log_manager.cc instead of in redpanda::application.
        "//src/v/redpanda:__pkg__",
    ],
    deps = [
        "//src/v/base",
        "//src/v/container:intrusive",
        "//src/v/metrics",
        "//src/v/ssx:semaphore",
        "//src/v/utils:named_type",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "resources",
    srcs = [
        "storage_resources.cc",
    ],
    hdrs = [
        "storage_resources.h",
    ],
    implementation_deps = [
        ":chunk_cache",
        ":logger",
        "@seastar",
    ],
    visibility = [
        "//src/v/cluster/archival/tests:__pkg__",
        "//src/v/cluster/tests:__pkg__",
        "//src/v/debug_bundle/tests:__pkg__",
        "//src/v/storage/tests:__pkg__",
    ],
    deps = [
        "//src/v/base",
        "//src/v/config",
        "//src/v/ssx:semaphore",
        "//src/v/utils:adjustable_semaphore",
    ],
)

redpanda_cc_library(
    name = "record_batch_utils",
    srcs = [
        "record_batch_utils.cc",
    ],
    hdrs = [
        "record_batch_utils.h",
    ],
    implementation_deps = [
        "//src/v/reflection:adl",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/model",
    ],
)

redpanda_cc_library(
    name = "segment_appender",
    srcs = [
        "segment_appender.cc",
    ],
    hdrs = [
        "segment_appender.h",
    ],
    implementation_deps = [
        ":logger",
        "//src/v/config",
        "//src/v/reflection:adl",
        "@fmt",
    ],
    deps = [
        ":chunk_cache",
        ":config",
        ":record_batch_utils",
        ":resources",
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/ssx:semaphore",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "storage",
    srcs = [
        "api.cc",
        "backlog_controller.cc",
        "compacted_index_chunk_reader.cc",
        "compaction_controller.cc",
        "compaction_reducers.cc",
        "disk_log_appender.cc",
        "disk_log_impl.cc",
        "fs_utils.cc",
        "kvstore.cc",
        "lock_manager.cc",
        "log_manager.cc",
        "log_reader.cc",
        "log_replayer.cc",
        "node.cc",
        "offset_to_filepos.cc",
        "offset_translator.cc",
        "offset_translator_state.cc",
        "parser.cc",
        "probe.cc",
        "readers_cache.cc",
        "segment.cc",
        "segment_deduplication_utils.cc",
        "segment_index.cc",
        "segment_reader.cc",
        "segment_set.cc",
        "segment_utils.cc",
        "snapshot.cc",
        "spill_key_index.cc",
        "types.cc",
    ],
    hdrs = [
        "api.h",
        "backlog_controller.h",
        "batch_consumer_utils.h",
        "compacted_index.h",
        "compacted_index_chunk_reader.h",
        "compacted_index_reader.h",
        "compacted_index_writer.h",
        "compacted_offset_list.h",
        "compaction_controller.h",
        "compaction_reducers.h",
        "config.h",
        "directories.h",
        "disk_log_appender.h",
        "disk_log_impl.h",
        "exceptions.h",
        "failure_probes.h",
        "file_sanitizer.h",
        "fs_utils.h",
        "fwd.h",
        "kvstore.h",
        "lock_manager.h",
        "log.h",
        "log_appender.h",
        "log_housekeeping_meta.h",
        "log_manager.h",
        "log_reader.h",
        "log_replayer.h",
        "node.h",
        "ntp_config.h",
        "offset_assignment.h",
        "offset_to_filepos.h",
        "offset_translator.h",
        "offset_translator_state.h",
        "parser.h",
        "parser_errc.h",
        "probe.h",
        "readers_cache.h",
        "readers_cache_probe.h",
        "segment.h",
        "segment_deduplication_utils.h",
        "segment_index.h",
        "segment_reader.h",
        "segment_set.h",
        "segment_utils.h",
        "snapshot.h",
        "spill_key_index.h",
        "translating_reader.h",
        "types.h",
        "version.h",
    ],
    implementation_deps = [
        ":log_manager_probe",
        "//src/v/ssx:abort_source",
        "//src/v/syschecks",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":batch_cache",
        ":chunk_cache",
        ":compaction_key",
        ":disk",
        ":file_sanitizer_types",
        ":index_state",
        ":logger",
        ":parser_utils",
        ":record_batch_builder",
        ":record_batch_utils",
        ":resources",
        ":scoped_file_tracker",
        ":segment_appender",
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/bytes:iostream",
        "//src/v/compaction",
        "//src/v/compaction:key",
        "//src/v/compaction:key_offset_map",
        "//src/v/compaction:types",
        "//src/v/compaction:utils",
        "//src/v/compression",
        "//src/v/config",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/container:intrusive",
        "//src/v/features",
        "//src/v/finjector",
        "//src/v/hashing:crc32c",
        "//src/v/hashing:secure",
        "//src/v/hashing:xx",
        "//src/v/json",
        "//src/v/metrics",
        "//src/v/model",
        "//src/v/model:batch_compression",
        "//src/v/model:batch_utils",
        "//src/v/raft:fundamental",
        "//src/v/random:generators",
        "//src/v/random:time_jitter",
        "//src/v/reflection:adl",
        "//src/v/resource_mgmt:available_memory",
        "//src/v/resource_mgmt:memory_groups",
        "//src/v/serde",
        "//src/v/serde:bool_class",
        "//src/v/serde:optional",
        "//src/v/serde:vector",
        "//src/v/ssx:async_algorithm",
        "//src/v/ssx:async_clear",
        "//src/v/ssx:future_util",
        "//src/v/ssx:semaphore",
        "//src/v/ssx:sformat",
        "//src/v/ssx:watchdog",
        "//src/v/ssx:when_all",
        "//src/v/strings:static_str",
        "//src/v/strings:string_switch",
        "//src/v/utils:adjustable_semaphore",
        "//src/v/utils:directory_walker",
        "//src/v/utils:file_io",
        "//src/v/utils:filtered_lower_bound",
        "//src/v/utils:functional",
        "//src/v/utils:human",
        "//src/v/utils:moving_average",
        "//src/v/utils:mutex",
        "//src/v/utils:named_type",
        "//src/v/utils:notification_list",
        "//src/v/utils:null_output_stream",
        "//src/v/utils:prefix_logger",
        "//src/v/utils:stream_provider",
        "//src/v/utils:to_string",
        "//src/v/utils:tracking_allocator",
        "//src/v/utils:tristate",
        "//src/v/utils:vint",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@abseil-cpp//absl/container:node_hash_map",
        "@abseil-cpp//absl/hash",
        "@boost//:algorithm",
        "@boost//:container",
        "@boost//:intrusive",
        "@boost//:iterator",
        "@boost//:lexical_cast",
        "@boost//:range",
        "@fmt",
        "@roaring",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "file_sanitizer_types",
    srcs = [
        "file_sanitizer_types.cc",
    ],
    hdrs = [
        "file_sanitizer_types.h",
    ],
    implementation_deps = [
        ":logger",
        "//src/v/strings:string_switch",
        "//src/v/utils:file_io",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/json",
        "//src/v/model",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "scoped_file_tracker",
    hdrs = [
        "scoped_file_tracker.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@abseil-cpp//absl/container:btree",
    ],
)

redpanda_cc_library(
    name = "log_manager_probe",
    srcs = [
        "log_manager_probe.cc",
    ],
    hdrs = [
        "log_manager_probe.h",
    ],
    implementation_deps = [
        "//src/v/config",
        "@seastar",
    ],
    deps = [
        "//src/v/metrics",
    ],
)

redpanda_cc_library(
    name = "disk",
    srcs = [
        "disk.cc",
    ],
    hdrs = [
        "disk.h",
    ],
    implementation_deps = [
        "//src/v/utils:human",
        "@fmt",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/serde",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "compaction_key",
    srcs = [
        "compaction_key.cc",
    ],
    hdrs = [
        "compaction_key.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes",
        "//src/v/compaction:key",
        "//src/v/model",
        "@seastar",
    ],
)

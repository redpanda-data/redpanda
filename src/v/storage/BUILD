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
    include_prefix = "storage",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "parser_utils",
    srcs = [
        "parser_utils.cc",
    ],
    hdrs = [
        "parser_utils.h",
    ],
    include_prefix = "storage",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf_parser",
        "//src/v/compression",
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
    include_prefix = "storage",
    visibility = ["//visibility:public"],
    deps = [
        ":parser_utils",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/compression",
        "//src/v/model",
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
    include_prefix = "storage",
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
    include_prefix = "storage",
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/container:fragmented_vector",
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
        "//src/v/ssx:future_util",
        "//src/v/utils:to_string",
        "@fmt",
    ],
    include_prefix = "storage",
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
    name = "compaction",
    hdrs = [
        "compaction.h",
    ],
    include_prefix = "storage",
    deps = [
        "//src/v/bytes",
    ],
)

redpanda_cc_library(
    name = "key_offset_map",
    srcs = [
        "key_offset_map.cc",
    ],
    hdrs = [
        "key_offset_map.h",
    ],
    include_prefix = "storage",
    deps = [
        ":compaction",
        "//src/v/container:fragmented_vector",
        "//src/v/hashing:secure",
        "//src/v/model",
        "//src/v/utils:tracking_allocator",
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
    include_prefix = "storage",
    visibility = [
        "//src/v/storage/tests:__pkg__",
        # TODO(bazel) chunk cache could probably be instantiated in
        # log_manager.cc instead of in redpanda::application.
        "//src/v/redpanda:__pkg__",
    ],
    deps = [
        "//src/v/base",
        "//src/v/container:intrusive",
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
    include_prefix = "storage",
    visibility = [
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
    include_prefix = "storage",
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
    include_prefix = "storage",
    deps = [
        ":chunk_cache",
        ":config",
        ":record_batch_utils",
        ":resources",
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/container:fragmented_vector",
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
        "file_sanitizer_types.cc",
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
        "file_sanitizer_types.h",
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
        "scoped_file_tracker.h",
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
        ":logger",
        "//src/v/syschecks",
    ],
    include_prefix = "storage",
    visibility = ["//visibility:public"],
    deps = [
        ":batch_cache",
        ":chunk_cache",
        ":compaction",
        ":index_state",
        ":key_offset_map",
        ":parser_utils",
        ":record_batch_builder",
        ":record_batch_utils",
        ":resources",
        ":segment_appender",
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/bytes:iostream",
        "//src/v/compression",
        "//src/v/config",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:fragmented_vector",
        "//src/v/container:intrusive",
        "//src/v/features",
        "//src/v/finjector",
        "//src/v/hashing:crc32c",
        "//src/v/hashing:secure",
        "//src/v/hashing:xx",
        "//src/v/json",
        "//src/v/metrics",
        "//src/v/model",
        "//src/v/raft:fundamental",
        "//src/v/random:fast_prng",
        "//src/v/random:generators",
        "//src/v/random:time_jitter",
        "//src/v/reflection:adl",
        "//src/v/resource_mgmt:available_memory",
        "//src/v/resource_mgmt:io_priority",
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
        "//src/v/strings:string_switch",
        "//src/v/utils:adjustable_semaphore",
        "//src/v/utils:directory_walker",
        "//src/v/utils:file_io",
        "//src/v/utils:filtered_lower_bound",
        "//src/v/utils:human",
        "//src/v/utils:moving_average",
        "//src/v/utils:mutex",
        "//src/v/utils:named_type",
        "//src/v/utils:notification_list",
        "//src/v/utils:prefix_logger",
        "//src/v/utils:stream_provider",
        "//src/v/utils:to_string",
        "//src/v/utils:tracking_allocator",
        "//src/v/utils:tristate",
        "//src/v/utils:vint",
        "@abseil-cpp//absl/algorithm:container",
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

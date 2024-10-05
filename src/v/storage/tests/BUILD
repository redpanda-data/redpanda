load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "kvstore_fixture",
    hdrs = [
        "kvstore_fixture.h",
    ],
    include_prefix = "storage/tests",
    deps = [
        "//src/v/config",
        "//src/v/features",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/test_utils:seastar_boost",
    ],
)

redpanda_test_cc_library(
    name = "disk_log_builder_fixture",
    hdrs = [
        "disk_log_builder_fixture.h",
    ],
    include_prefix = "storage/tests",
    deps = [
        ":disk_log_builder",
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
    include_prefix = "storage/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/features",
        "//src/v/model/tests:random",
        "//src/v/storage",
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

redpanda_cc_btest(
    name = "log_segment_appender_test",
    timeout = "short",
    srcs = [
        "log_segment_appender_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/bytes:random",
        "//src/v/config",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/storage:chunk_cache",
        "//src/v/storage:resources",
        "//src/v/storage:segment_appender",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "log_manager_test",
    timeout = "short",
    srcs = [
        "log_manager_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:random",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/storage:logger",
        "//src/v/storage:record_batch_utils",
        "//src/v/storage:segment_appender",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
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
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "batch_cache_test",
    timeout = "short",
    srcs = [
        "batch_cache_test.cc",
    ],
    deps = [
        "//src/v/bytes:random",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/storage:batch_cache",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "batch_cache_reclaim_test",
    timeout = "short",
    srcs = [
        "batch_cache_reclaim_test.cc",
    ],
    target_compatible_with = select({
        "@seastar//:use_system_allocator": [],
        "//conditions:default": ["@platforms//:incompatible"],
    }),
    deps = [
        "//src/v/model",
        "//src/v/storage:batch_cache",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "key_offset_map_test",
    timeout = "short",
    srcs = [
        "key_offset_map_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/storage:key_offset_map",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_btest(
    name = "kvstore_test",
    timeout = "short",
    srcs = [
        "kvstore_test.cc",
    ],
    deps = [
        ":kvstore_fixture",
        "//src/v/bytes:random",
        "//src/v/config",
        "//src/v/random:generators",
        "//src/v/reflection:adl",
        "//src/v/storage",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "appender_chunk_manipulations_test",
    timeout = "short",
    srcs = [
        "appender_chunk_manipulations.cc",
    ],
    deps = [
        "//src/v/config",
        "//src/v/random:generators",
        "//src/v/storage:segment_appender",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "backlog_controller_test",
    timeout = "short",
    srcs = [
        "backlog_controller_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/storage",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
    ],
)

redpanda_cc_btest(
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
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
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
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "snapshot_test",
    timeout = "short",
    srcs = [
        "snapshot_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:random",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/test_utils:seastar_boost",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
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
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/container:btree",
        "@boost//:test",
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
        "//src/v/storage",
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
        "//src/v/config",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/storage:chunk_cache",
        "//src/v/storage:key_offset_map",
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

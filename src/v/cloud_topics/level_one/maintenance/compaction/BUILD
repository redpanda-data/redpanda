load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/cloud_topics/level_one:__subpackages__"])

redpanda_cc_library(
    name = "compaction_filter",
    srcs = [
        "compaction_filter.cc",
    ],
    hdrs = [
        "compaction_filter.h",
    ],
    visibility = [
        "//src/v/cloud_topics/level_one:__subpackages__",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/metastore:offset_interval_set",
        "//src/v/compaction:filter",
        "//src/v/compaction:key_offset_map",
        "//src/v/compaction:utils",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "compaction_source_and_sink",
    srcs = [
        "compaction_sink.cc",
        "compaction_source.cc",
    ],
    hdrs = [
        "compaction_sink.h",
        "compaction_source.h",
    ],
    visibility = [
        "//src/v/cloud_topics/level_one:__subpackages__",
    ],
    deps = [
        ":compaction_filter",
        "//src/v/bytes",
        "//src/v/bytes:iostream",
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_topics:log_reader_config",
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/frontend_reader:level_one_reader_probe",
        "//src/v/cloud_topics/level_one/frontend_reader:reader",
        "//src/v/cloud_topics/level_one/maintenance:logger",
        "//src/v/cloud_topics/level_one/maintenance:meta",
        "//src/v/cloud_topics/level_one/maintenance:worker_probe",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/cloud_topics/level_one/metastore:extent_metadata_reader",
        "//src/v/cloud_topics/level_one/metastore:offset_interval_set",
        "//src/v/cloud_topics/level_one/metastore:retry",
        "//src/v/compaction:key",
        "//src/v/compaction:key_offset_map",
        "//src/v/compaction:reducer",
        "//src/v/compaction:utils",
        "//src/v/config",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/model:batch_compression",
        "//src/v/ssx:future_util",
        "@seastar",
    ],
)

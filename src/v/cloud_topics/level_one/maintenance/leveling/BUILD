load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/cloud_topics/level_one:__subpackages__"])

redpanda_cc_library(
    name = "leveling_queue",
    hdrs = [
        "leveling_queue.h",
    ],
    include_prefix = "cloud_topics/level_one/maintenance/leveling",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics/level_one/maintenance:keyed_priority_queue",
        "//src/v/cloud_topics/level_one/maintenance:meta",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/model",
    ],
)

redpanda_cc_library(
    name = "leveling_source",
    srcs = [
        "leveling_source.cc",
    ],
    hdrs = [
        "leveling_source.h",
    ],
    include_prefix = "cloud_topics/level_one/maintenance/leveling",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_io:admission_control_types",
        "//src/v/cloud_topics:log_reader_config",
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/frontend_reader:reader",
        "//src/v/cloud_topics/level_one/maintenance:meta",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/cloud_topics/level_one/metastore:extent_metadata_reader",
        "//src/v/cloud_topics/level_one/metastore:leveling_range_builder",
        "//src/v/compaction:reducer",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/utils:prefix_logger",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "leveling_sink",
    srcs = [
        "leveling_sink.cc",
    ],
    hdrs = [
        "leveling_sink.h",
    ],
    include_prefix = "cloud_topics/level_one/maintenance/leveling",
    visibility = ["//visibility:public"],
    deps = [
        ":leveling_source",
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/maintenance:l1_object_sink",
        "//src/v/cloud_topics/level_one/maintenance:worker_probe",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/cloud_topics/level_one/metastore:retry",
        "//src/v/compaction:reducer",
        "//src/v/config",
        "//src/v/model",
        "//src/v/utils:prefix_logger",
        "@seastar",
    ],
)

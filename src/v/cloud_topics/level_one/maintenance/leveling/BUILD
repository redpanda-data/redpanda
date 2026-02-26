load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/cloud_topics/level_one:__subpackages__"])

redpanda_cc_library(
    name = "leveling_source_and_sink",
    srcs = [
        "leveling_sink.cc",
        "leveling_source.cc",
    ],
    hdrs = [
        "leveling_sink.h",
        "leveling_source.h",
    ],
    deps = [
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_topics:log_reader_config",
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/frontend_reader:reader",
        "//src/v/cloud_topics/level_one/maintenance:logger",
        "//src/v/cloud_topics/level_one/maintenance:meta",
        "//src/v/cloud_topics/level_one/maintenance:object_sink",
        "//src/v/cloud_topics/level_one/maintenance:worker_probe",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/cloud_topics/level_one/metastore:offset_interval_set",
        "//src/v/cloud_topics/level_one/metastore:retry",
        "//src/v/compaction:reducer",
        "//src/v/config",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/ssx:future_util",
        "@seastar",
    ],
)

load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "reader",
    srcs = [
        "reader.cc",
    ],
    hdrs = [
        "reader.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes:iostream",
        "//src/v/cloud_topics:log_reader_config",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/config",
        "//src/v/model",
        "//src/v/model:timeout_clock",
        "@seastar",
    ],
)

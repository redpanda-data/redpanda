load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "frontend_reader",
    srcs = ["reader.cc"],
    hdrs = ["reader.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_topics:data_plane_api",
        "//src/v/cloud_topics:log_reader_config",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/frontend_reader:reader",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/cloud_topics/level_one/metastore:replicated_metastore",
        "//src/v/cloud_topics/level_zero/frontend_reader",
        "//src/v/cloud_topics/level_zero/stm:ctp_stm_api",
        "//src/v/cluster",
        "//src/v/model",
    ],
)

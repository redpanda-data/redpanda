load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "errc",
    srcs = [
        "errc.cc",
    ],
    hdrs = [
        "errc.h",
    ],
    include_prefix = "cloud_topics/frontend",
    visibility = ["//visibility:public"],
    deps = [
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "frontend",
    srcs = [
        "frontend.cc",
    ],
    hdrs = [
        "frontend.h",
    ],
    implementation_deps = [
        "//src/v/cloud_storage:types",
        "//src/v/cloud_topics:logger",
        "//src/v/cluster",
    ],
    include_prefix = "cloud_topics/frontend",
    visibility = ["//visibility:public"],
    deps = [
        ":errc",
        ":level_zero_log_reader",
        "//src/v/base",
        "//src/v/cloud_topics:data_plane_api",
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/cloud_topics/level_zero/stm:placeholder",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/storage",
        "//src/v/storage:record_batch_builder",
        "@boost//:outcome",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "level_zero_log_reader",
    srcs = [
        "level_zero_log_reader.cc",
    ],
    hdrs = [
        "level_zero_log_reader.h",
    ],
    implementation_deps = [
        "//src/v/cloud_topics:data_plane_api",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/level_zero/stm:placeholder",
        "//src/v/cluster",
        "//src/v/config",
    ],
    include_prefix = "cloud_topics/frontend",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/model",
        "//src/v/storage",
        "@boost//:outcome",
        "@seastar",
    ],
)

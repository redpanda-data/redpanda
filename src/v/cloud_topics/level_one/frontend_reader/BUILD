load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "level_one_reader_probe",
    srcs = [
        "level_one_reader_probe.cc",
    ],
    hdrs = [
        "level_one_reader_probe.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/config",
        "//src/v/metrics",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "l1_reader_cache",
    srcs = [
        "l1_reader_cache.cc",
    ],
    hdrs = [
        "l1_reader_cache.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/container:intrusive",
        "//src/v/model",
        "//src/v/ssx:future_util",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "reader",
    srcs = [
        "level_one_reader.cc",
    ],
    hdrs = [
        "level_one_reader.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":l1_reader_cache",
        "//src/v/bytes:iostream",
        "//src/v/cloud_topics:log_reader_config",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/frontend_reader:level_one_reader_probe",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/cloud_topics/level_one/metastore:retry",
        "//src/v/model",
        "//src/v/model:timeout_clock",
        "//src/v/utils:prefix_logger",
        "//src/v/utils:retry_chain_node",
        "@seastar",
    ],
)

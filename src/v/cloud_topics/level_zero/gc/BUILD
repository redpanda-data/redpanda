load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "level_zero_gc",
    srcs = [
        "level_zero_gc.cc",
    ],
    hdrs = [
        "level_zero_gc.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:object_utils",
        "//src/v/cluster",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_io:io_result",
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_topics:types",
        "//src/v/model",
        "@seastar",
    ],
)

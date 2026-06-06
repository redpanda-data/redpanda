load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [":__subpackages__"])

redpanda_cc_library(
    name = "metastore_sink",
    srcs = [
        "metastore_sink.cc",
    ],
    hdrs = [
        "metastore_sink.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/cluster",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "@seastar",
    ],
)

load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "topic_properties_utils",
    srcs = [
        "topic_properties_utils.cc",
    ],
    hdrs = [
        "topic_properties_utils.h",
    ],
    include_prefix = "cluster_link/utils",
    visibility = ["//src/v/cluster_link:__subpackages__"],
    deps = [
        "//src/v/kafka/server:topic_config_utils",
    ],
)

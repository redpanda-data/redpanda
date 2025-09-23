load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/cloud_topics:__subpackages__"])

redpanda_cc_library(
    name = "housekeeper",
    srcs = ["housekeeper.cc"],
    hdrs = ["housekeeper.h"],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/config",
        "//src/v/model",
        "//src/v/random:time_jitter",
        "//src/v/ssx:future_util",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)

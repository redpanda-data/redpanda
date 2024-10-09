load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "commands",
    hdrs = [
        "commands.h",
    ],
    include_prefix = "cloud_topics/dl_stm",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_topics:types",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/serde:uuid",
        "@abseil-cpp//absl/container:btree",
    ],
)

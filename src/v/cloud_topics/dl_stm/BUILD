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

redpanda_cc_library(
    name = "sorted_run",
    srcs = [
        "sorted_run.cc",
    ],
    hdrs = [
        "sorted_run.h",
    ],
    include_prefix = "cloud_topics/dl_stm",
    visibility = ["//visibility:public"],
    deps = [
        ":commands",
        "//src/v/model",
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "overlay_collection",
    srcs = [
        "overlay_collection.cc",
    ],
    hdrs = [
        "overlay_collection.h",
    ],
    include_prefix = "cloud_topics/dl_stm",
    visibility = ["//visibility:public"],
    deps = [
        ":commands",
        ":sorted_run",
        "//src/v/container:fragmented_vector",
        "//src/v/model",
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "dl_stm_state",
    srcs = [
        "dl_stm_state.cc",
    ],
    hdrs = [
        "dl_stm_state.h",
    ],
    include_prefix = "cloud_topics/dl_stm",
    visibility = ["//visibility:public"],
    deps = [
        ":commands",
        ":overlay_collection",
        ":sorted_run",
        "//src/v/container:fragmented_vector",
        "//src/v/model",
        "//src/v/serde",
        "@abseil-cpp//absl/container:btree",
    ],
)

load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "dl_stm_commands",
    hdrs = ["dl_stm_commands.h"],
    include_prefix = "cloud_topics/dl_stm",
    visibility = [":__subpackages__"],
    deps = [
        "//src/v/cloud_topics:dl_overlay",
        "//src/v/cloud_topics:dl_version",
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "dl_stm_state",
    srcs = ["dl_stm_state.cc"],
    hdrs = ["dl_stm_state.h"],
    include_prefix = "cloud_topics/dl_stm",
    visibility = [":__subpackages__"],
    deps = [
        "//src/v/cloud_topics:dl_overlay",
        "//src/v/cloud_topics:dl_snapshot",
        "//src/v/cloud_topics:dl_version",
        "//src/v/container:fragmented_vector",
        "//src/v/model",
    ],
)

redpanda_cc_library(
    name = "dl_stm",
    srcs = ["dl_stm.cc"],
    hdrs = ["dl_stm.h"],
    implementation_deps = [
        ":dl_stm_commands",
        "//src/v/serde:map",
        "//src/v/serde:uuid",
    ],
    include_prefix = "cloud_topics/dl_stm",
    visibility = ["//visibility:public"],
    deps = [
        ":dl_stm_state",
        "//src/v/raft",
    ],
)

redpanda_cc_library(
    name = "dl_stm_api",
    srcs = ["dl_stm_api.cc"],
    hdrs = ["dl_stm_api.h"],
    implementation_deps = [
        ":dl_stm",
        ":dl_stm_commands",
        "//src/v/raft",
        "//src/v/serde:uuid",
        "//src/v/storage:record_batch_builder",
    ],
    include_prefix = "cloud_topics/dl_stm",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics:dl_overlay",
        "//src/v/cloud_topics:dl_snapshot",
        "//src/v/cloud_topics:dl_version",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "dl_stm_factory",
    srcs =
        [
            "dl_stm_factory.cc",
        ],
    hdrs =
        [
            "dl_stm_factory.h",
        ],
    implementation_deps = [
        ":dl_stm",
        "//src/v/cloud_topics:logger",
    ],
    include_prefix = "cloud_topics/dl_stm",
    visibility = ["//visibility:public"],
    deps = ["//src/v/cluster:state_machine_registry"],
)

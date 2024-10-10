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
        "//src/v/container:fragmented_vector",
        "//src/v/model",
        "//src/v/serde",
        "@abseil-cpp//absl/container:btree",
    ],
)

redpanda_cc_library(
    name = "command_builder",
    srcs = [
        "command_builder.cc",
    ],
    hdrs = [
        "command_builder.h",
    ],
    include_prefix = "cloud_topics/dl_stm",
    visibility = ["//visibility:public"],
    deps = [
        ":commands",
        "//src/v/base",
        "//src/v/cloud_topics:types",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/serde:map",
        "//src/v/serde:named_type",
        "//src/v/serde:uuid",
        "//src/v/storage:record_batch_builder",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/container:btree",
    ],
)

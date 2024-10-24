load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "dl_stm",
    srcs =
        [
            "dl_stm.cc",
        ],
    hdrs =
        [
            "dl_stm.h",
        ],
    include_prefix = "cloud_topics/dl_stm",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/raft",
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

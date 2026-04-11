load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/cloud_topics:__subpackages__"])

redpanda_cc_library(
    name = "housekeeper",
    srcs = ["housekeeper.cc"],
    hdrs = ["housekeeper.h"],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/config",
        "//src/v/model",
        "//src/v/random:time_jitter",
        "//src/v/ssx:future_util",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "manager",
    srcs = ["manager.cc"],
    hdrs = ["manager.h"],
    deps = [
        ":housekeeper",
        "//src/v/base",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:state_accessors",
        "//src/v/cloud_topics/frontend",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/cloud_topics/level_zero/stm:ctp_stm",
        "//src/v/cloud_topics/level_zero/stm:ctp_stm_api",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "//src/v/ssx:work_queue",
        "//src/v/utils:retry_chain_node",
        "@seastar",
    ],
)

load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "errc",
    srcs = [
        "errc.cc",
    ],
    hdrs = [
        "errc.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "frontend",
    srcs = [
        "frontend.cc",
    ],
    hdrs = [
        "frontend.h",
    ],
    implementation_deps = [
        "//src/v/cloud_storage:types",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:topic_id_partition",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/cloud_topics/level_zero/common:producer_queue",
        "//src/v/cloud_topics/level_zero/stm:ctp_stm",
        "//src/v/cloud_topics/level_zero/stm:placeholder",
        "//src/v/cluster",
        "//src/v/kafka/server:write_at_offset_stm",
        "//src/v/ssx:future_util",
        "//src/v/storage:record_batch_builder",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":errc",
        "//src/v/base",
        "//src/v/cloud_topics:data_plane_api",
        "//src/v/cloud_topics:log_reader_config",
        "//src/v/cloud_topics:state_accessors",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_one/frontend_reader:reader",
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/cloud_topics/level_zero/frontend_reader",
        "//src/v/cloud_topics/level_zero/stm:ctp_stm_api",
        "//src/v/cloud_topics/level_zero/stm:placeholder",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/storage",
        "//src/v/utils:retry_chain_node",
        "@seastar",
    ],
)

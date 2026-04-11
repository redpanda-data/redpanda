load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    "//src/v/cloud_topics:__pkg__",
    "//src/v/cloud_topics/level_zero/frontend_reader:__pkg__",
])

redpanda_cc_library(
    name = "frontend_reader",
    srcs = [
        "level_zero_reader.cc",
    ],
    hdrs = [
        "level_zero_reader.h",
    ],
    implementation_deps = [
        "//src/v/cloud_topics:data_plane_api",
        "//src/v/cloud_topics:log_reader_config",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:state_accessors",
        "//src/v/cloud_topics:topic_id_partition",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/stm:ctp_stm_api",
        "//src/v/cloud_topics/level_zero/stm:placeholder",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/ssx:future_util",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/model",
        "//src/v/storage",
        "//src/v/utils:prefix_logger",
        "@boost//:outcome",
        "@seastar",
    ],
)

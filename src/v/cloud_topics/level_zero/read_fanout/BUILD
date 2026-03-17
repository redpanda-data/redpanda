load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    "//src/v/cloud_topics:__pkg__",
    "//src/v/cloud_topics/level_zero/read_fanout:__pkg__",
    "//src/v/cloud_topics/level_zero/read_fanout/tests:__pkg__",
])

redpanda_cc_library(
    name = "read_fanout",
    srcs = [
        "read_fanout.cc",
    ],
    hdrs = [
        "read_fanout.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/pipeline:pipeline_actor",
        "//src/v/cloud_topics/level_zero/pipeline:read_pipeline",
        "//src/v/cloud_topics/level_zero/pipeline:read_request",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/ssx:future_util",
        "//src/v/storage",
        "//src/v/utils:retry_chain_node",
        "@seastar",
    ],
)

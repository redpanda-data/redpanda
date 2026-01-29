load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    "//src/v/cloud_topics:__pkg__",
    "//src/v/cloud_topics/level_zero/read_debounce:__pkg__",
    "//src/v/cloud_topics/level_zero/read_debounce/tests:__pkg__",
])

redpanda_cc_library(
    name = "read_debounce",
    srcs = [
        "read_debounce.cc",
    ],
    hdrs = [
        "read_debounce.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/pipeline:event_filter",
        "//src/v/cloud_topics/level_zero/pipeline:read_pipeline",
        "//src/v/cloud_topics/level_zero/pipeline:read_request",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/ssx:checkpoint_mutex",
        "//src/v/ssx:future_util",
        "//src/v/storage",
        "//src/v/utils:retry_chain_node",
        "@seastar",
    ],
)

load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    ":__subpackages__",
    "//src/v/cloud_topics:__pkg__",
])

redpanda_cc_library(
    name = "read_request_scheduler",
    srcs = [
        "read_request_scheduler.cc",
    ],
    hdrs = [
        "read_request_scheduler.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/cloud_topics/level_zero/pipeline:base_pipeline",
        "//src/v/cloud_topics/level_zero/pipeline:event_filter",
        "//src/v/cloud_topics/level_zero/pipeline:read_pipeline",
        "//src/v/cloud_topics/level_zero/pipeline:read_request",
        "//src/v/config",
        "//src/v/ssx:future_util",
        "@seastar",
    ],
)

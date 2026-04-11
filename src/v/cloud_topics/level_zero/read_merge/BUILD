load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    "//src/v/cloud_topics:__pkg__",
    "//src/v/cloud_topics/level_zero/read_merge:__pkg__",
    "//src/v/cloud_topics/level_zero/read_merge/tests:__pkg__",
])

redpanda_cc_library(
    name = "read_merge",
    srcs = [
        "read_merge.cc",
    ],
    hdrs = [
        "read_merge.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/common:level_zero_probe",
        "//src/v/cloud_topics/level_zero/pipeline:read_pipeline",
        "//src/v/cloud_topics/level_zero/pipeline:read_request",
        "//src/v/config",
        "//src/v/container:chunked_hash_map",
        "//src/v/ssx:future_util",
        "@seastar",
    ],
)

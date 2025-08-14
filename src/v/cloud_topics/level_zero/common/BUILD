load("//bazel:build.bzl", "redpanda_cc_library")

package(
    default_visibility = [
        "//src/v/cloud_topics:__pkg__",
        "//src/v/cloud_topics/batch_cache:__pkg__",
        "//src/v/cloud_topics/frontend:__pkg__",
        "//src/v/cloud_topics/frontend/tests:__pkg__",
        "//src/v/cloud_topics/level_zero/batcher:__pkg__",
        "//src/v/cloud_topics/level_zero/batcher/tests:__pkg__",
        "//src/v/cloud_topics/level_zero/frontend_reader:__pkg__",
        "//src/v/cloud_topics/level_zero/pipeline:__pkg__",
        "//src/v/cloud_topics/level_zero/pipeline/tests:__pkg__",
        "//src/v/cloud_topics/level_zero/reader:__pkg__",
        "//src/v/cloud_topics/level_zero/reader/tests:__pkg__",
        "//src/v/cloud_topics/level_zero/throttler/tests:__pkg__",
    ],
)

redpanda_cc_library(
    name = "extent_meta",
    hdrs = [
        "extent_meta.h",
    ],
    deps = [
        "//src/v/cloud_topics:types",
        "//src/v/model",
        "@fmt",
    ],
)

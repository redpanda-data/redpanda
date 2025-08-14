load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    "//src/v/cloud_topics:__pkg__",
    "//src/v/cloud_topics/batch_cache/tests:__pkg__",
])

redpanda_cc_library(
    name = "batch_cache",
    srcs = [
        "batch_cache.cc",
    ],
    hdrs = [
        "batch_cache.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/cloud_topics/level_zero/stm:placeholder",
        "//src/v/config",
        "//src/v/model",
        "//src/v/ssx:future_util",
        "//src/v/ssx:sformat",
        "//src/v/storage",
        "//src/v/storage:batch_cache",
        "//src/v/storage:record_batch_utils",
        "//src/v/utils:retry_chain_node",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)

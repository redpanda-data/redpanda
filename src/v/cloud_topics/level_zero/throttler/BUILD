load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    "//src/v/cloud_topics:__pkg__",
    "//src/v/cloud_topics/level_zero/throttler/tests:__pkg__",
])

redpanda_cc_library(
    name = "throttler",
    srcs = [
        "throttler.cc",
    ],
    hdrs = [
        "throttler.h",
    ],
    implementation_deps = [
        "//src/v/cloud_io:remote",
        "//src/v/cloud_topics:logger",
        "//src/v/ssx:sformat",
        "//src/v/utils:human",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/pipeline:event_filter",
        "//src/v/cloud_topics/level_zero/pipeline:write_pipeline",
        "//src/v/cloud_topics/level_zero/pipeline:write_request",
        "//src/v/utils:token_bucket",
        "@seastar",
    ],
)

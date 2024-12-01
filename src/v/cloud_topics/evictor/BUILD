load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/cloud_topics/evictor/tests:__pkg__"])

redpanda_cc_library(
    name = "evictor",
    srcs = [
        "evictor.cc",
    ],
    hdrs = [
        "evictor.h",
    ],
    implementation_deps = [
        "//src/v/cloud_io:remote",
        "//src/v/cloud_topics:logger",
        "//src/v/ssx:sformat",
    ],
    include_prefix = "cloud_topics/evictor",
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/core:event_filter",
        "//src/v/cloud_topics/core:write_pipeline",
        "//src/v/cloud_topics/core:write_request",
        "@seastar",
    ],
)

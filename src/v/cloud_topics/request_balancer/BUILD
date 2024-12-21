load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/cloud_topics/request_balancer/tests:__pkg__"])

redpanda_cc_library(
    name = "write_request_balancer",
    srcs = [
        "write_request_balancer.cc",
    ],
    hdrs = [
        "write_request_balancer.h",
    ],
    include_prefix = "cloud_topics/request_balancer",
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/core:event_filter",
        "//src/v/cloud_topics/core:write_pipeline",
        "//src/v/cloud_topics/core:write_request",
        "//src/v/ssx:future_util",
        "@seastar",
    ],
)

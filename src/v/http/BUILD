load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//visibility:public"])

redpanda_cc_library(
    name = "http",
    srcs = [
        "chunk_encoding.cc",
        "client.cc",
        "iobuf_body.cc",
        "logger.cc",
    ],
    hdrs = [
        "chunk_encoding.h",
        "client.h",
        "iobuf_body.h",
        "logger.h",
        "probe.h",
    ],
    include_prefix = "http",
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:scattered_message",
        "//src/v/config",
        "//src/v/net",
        "//src/v/ssx:sformat",
        "//src/v/utils:prefix_logger",
        "@boost//:asio",
        "@boost//:beast",
        "@boost//:optional",
        "@seastar",
    ],
)

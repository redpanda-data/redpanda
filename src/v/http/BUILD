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
        "include/http/chunk_encoding.h",
        "include/http/client.h",
        "include/http/iobuf_body.h",
        "include/http/logger.h",
        "include/http/probe.h",
    ],
    strip_include_prefix = "include",
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

load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "parser",
    srcs = [
        "leb128.h",
        "parser.cc",
    ],
    hdrs = [
        "parser.h",
    ],
    include_prefix = "wasm/parser",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/container:fragmented_vector",
        "//src/v/strings:utf8",
        "@abseil-cpp//absl/algorithm:container",
        "@fmt",
        "@seastar",
    ],
)

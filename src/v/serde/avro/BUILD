load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "avro",
    srcs = [
        "encoder.cc",
        "parser.cc",
    ],
    hdrs = [
        "encoder.h",
        "parser.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/container:chunked_vector",
        "//src/v/utils:vint",
        "@avro",
        "@boost//:range",
        "@seastar",
    ],
)

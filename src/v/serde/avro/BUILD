load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "avro",
    srcs = [
        "parser.cc",
    ],
    hdrs = [
        "parser.h",
    ],
    include_prefix = "serde/avro",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/container:fragmented_vector",
        "//src/v/utils:vint",
        "@avro",
        "@seastar",
    ],
)

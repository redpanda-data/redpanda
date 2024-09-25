load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "protobuf",
    srcs = [
        "parser.cc",
    ],
    hdrs = [
        "parser.h",
    ],
    include_prefix = "serde/protobuf",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes:hash",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:fragmented_vector",
        "//src/v/utils:vint",
        "@protobuf",
        "@seastar",
    ],
)

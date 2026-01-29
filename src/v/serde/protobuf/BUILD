load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "protobuf",
    srcs = [
        "parser.cc",
    ],
    hdrs = [
        "parser.h",
    ],
    implementation_deps = [":wire_format"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes:hash",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "@protobuf",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "wire_format",
    srcs = [
        "wire_format.cc",
    ],
    hdrs = [
        "wire_format.h",
    ],
    implementation_deps = [
        "//src/v/utils:vint",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes:hash",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/utils:vint",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "support",
    hdrs = [
        "support.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":field_mask",
        ":wire_format",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/container:chunked_vector",
        "//src/v/utils:fixed_string",
        "@abseil-cpp//absl/strings",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "json",
    srcs = [
        "json.cc",
    ],
    hdrs = [
        "json.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/serde/json:writer",
        "//src/v/utils:base64",
        "@abseil-cpp//absl/strings",
        "@abseil-cpp//absl/strings:str_format",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":field_mask",
        "//src/v/serde/json:parser",
        "@abseil-cpp//absl/time",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "rpc",
    srcs = ["rpc.cc"],
    hdrs = [
        "rpc.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":wire_format",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/container:chunked_hash_map",
        "//src/v/serde/json:writer",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "field_mask",
    srcs = ["field_mask.cc"],
    hdrs = ["field_mask.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "base",
    srcs = ["base.cc"],
    hdrs = ["base.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":field_mask",
        "//src/v/bytes:iobuf",
        "//src/v/strings:static_str",
        "@abseil-cpp//absl/time",
        "@seastar",
    ],
)

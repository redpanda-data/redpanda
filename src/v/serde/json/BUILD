load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "writer",
    srcs = [
        "writer.cc",
    ],
    hdrs = [
        "writer.h",
    ],
    implementation_deps = [
        "//src/v/utils:base64",
        "@abseil-cpp//absl/cleanup",
        "@abseil-cpp//absl/strings",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
    ],
)

redpanda_cc_library(
    name = "parser",
    srcs = [
        "parser.cc",
    ],
    hdrs = [
        "parser.h",
    ],
    implementation_deps = [
        "//src/v/serde/json/detail:numeric",
        "//src/v/serde/json/detail:string",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:chunked_vector",
        "@fmt",
        "@seastar",
    ],
)

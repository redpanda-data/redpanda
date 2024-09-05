load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "debug_bundle",
    srcs = [
        "error.cc",
        "types.cc",
    ],
    hdrs = [
        "error.h",
        "types.h",
    ],
    include_prefix = "debug_bundle",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/container:fragmented_vector",
        "//src/v/model",
        "//src/v/strings:string_switch",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/container:btree",
        "@fmt",
        "@seastar",
    ],
)

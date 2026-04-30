load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "impl",
    hdrs = [
        "backend_decl.h",
        "backend_impl.h",
        "types.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "//src/v/ssx:future_util",
        "//src/v/utils:variant",
        "@seastar",
    ],
)

load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "arity",
    hdrs = [
        "arity.h",
    ],
    include_prefix = "reflection",
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "to_tuple",
    hdrs = [
        "to_tuple.h",
    ],
    include_prefix = "reflection",
    visibility = ["//visibility:public"],
    deps = [
        ":arity",
    ],
)

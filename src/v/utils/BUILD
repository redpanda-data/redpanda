load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "vint",
    srcs = [
        "vint.cc",
    ],
    hdrs = [
        "vint.h",
        "vint_iostream.h",
    ],
    include_prefix = "utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes",
    ],
)

redpanda_cc_library(
    name = "tristate",
    hdrs = [
        "tristate.h",
    ],
    include_prefix = "utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
    ],
)

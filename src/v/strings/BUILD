load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "utf8",
    srcs = [
        "utf8.cc",
    ],
    hdrs = [
        "utf8.h",
    ],
    include_prefix = "strings",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@boost//:locale",
    ],
)

redpanda_cc_library(
    name = "string_switch",
    hdrs = [
        "string_switch.h",
    ],
    include_prefix = "strings",
    visibility = ["//visibility:public"],
)

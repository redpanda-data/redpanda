load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "utf8",
    srcs = [
        "utf8.cc",
    ],
    hdrs = [
        "include/strings/utf8.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@boost//:locale",
    ],
)

redpanda_cc_library(
    name = "string_switch",
    hdrs = [
        "include/strings/string_switch.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
)

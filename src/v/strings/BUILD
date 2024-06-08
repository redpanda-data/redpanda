load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "strings",
    srcs = [
        "utf8.cc",
    ],
    hdrs = [
        "include/strings/string_switch.h",
        "include/strings/utf8.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@boost//:locale",
    ],
)

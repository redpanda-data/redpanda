load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "syschecks",
    srcs = [
        "pidfile.cc",
        "syschecks.cc",
    ],
    hdrs = [
        "include/syschecks/syschecks.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/ssx:sformat",
        "//src/v/version",
        "@seastar",
    ],
)

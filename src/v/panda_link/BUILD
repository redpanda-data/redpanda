load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "logger",
    srcs = [
        "logger.cc",
    ],
    hdrs = [
        "logger.h",
    ],
    include_prefix = "panda_link",
    visibility = ["//src/v/panda_link:__subpackages__"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

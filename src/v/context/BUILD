load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "context",
    srcs = ["context.cc"],
    hdrs = ["context.h"],
    defines = select({
        "@//bazel:optimized_build": [],
        "//conditions:default": ["CONTEXT_DEBUG_REF_COUNTING"],
    }),
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "context_frame",
    hdrs = [
        "abort_source.h",
        "context_frame.h",
        "deadline_timer.h",
        "linker.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":context",
        "@seastar",
    ],
)

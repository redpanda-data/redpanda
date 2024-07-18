load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "available_memory",
    srcs = [
        "available_memory.cc",
    ],
    hdrs = [
        "available_memory.h",
    ],
    include_prefix = "resource_mgmt",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/config",
        "//src/v/container:intrusive",
        "//src/v/metrics",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "memory_groups",
    srcs = [
        "memory_groups.cc",
    ],
    hdrs = [
        "memory_groups.h",
    ],
    include_prefix = "resource_mgmt",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/config",
        "@seastar",
    ],
)

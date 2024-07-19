load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "thread_worker",
    hdrs = [
        "include/ssx/thread_worker.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@boost//:lockfree",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "sformat",
    hdrs = [
        "include/ssx/sformat.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "future_util",
    hdrs = [
        "include/ssx/future-util.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/utils:functional",
    ],
)

redpanda_cc_library(
    name = "semaphore",
    hdrs = [
        "include/ssx/semaphore.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "abort_source",
    hdrs = [
        "include/ssx/abort_source.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "async_algorithm",
    hdrs = [
        "include/ssx/async_algorithm.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "async_clear",
    hdrs = [
        "include/ssx/async-clear.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@seastar",
    ],
)

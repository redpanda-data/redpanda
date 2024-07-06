load("//bazel:build.bzl", "redpanda_cc_library")
load("//bazel:test.bzl", "redpanda_cc_btest")

redpanda_cc_library(
    name = "thread_worker",
    hdrs = [
        "include/ssx/thread_worker.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
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
    ],
)

redpanda_cc_library(
    name = "future-util",
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

redpanda_cc_btest(
    name = "future-util_test",
    timeout = "short",
    srcs = [
        "tests/future_util.cc",
    ],
    deps = [
        ":future-util",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:functional",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "thread_worker_test",
    timeout = "short",
    srcs = [
        "tests/thread_worker.cc",
    ],
    deps = [
        ":thread_worker",
        "//src/v/base",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/algorithm:container",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "sformat_test",
    timeout = "short",
    srcs = [
        "tests/sformat.cc",
    ],
    deps = [
        ":sformat",
        "//src/v/base",
        "//src/v/test_utils:seastar_boost",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "abort_source_test",
    timeout = "short",
    srcs = [
        "tests/abort_source_test.cc",
    ],
    deps = [
        ":abort_source",
        "//src/v/base",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

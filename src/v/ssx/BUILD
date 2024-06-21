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
    ],
)

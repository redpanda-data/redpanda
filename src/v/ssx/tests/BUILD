load("//bazel:test.bzl", "redpanda_cc_btest")

redpanda_cc_btest(
    name = "future-util_test",
    timeout = "short",
    srcs = [
        "future_util.cc",
    ],
    deps = [
        "//src/v/ssx:future_util",
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
        "thread_worker.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/ssx:thread_worker",
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
        "sformat.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/ssx:sformat",
        "//src/v/test_utils:seastar_boost",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "abort_source_test",
    timeout = "short",
    srcs = [
        "abort_source_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/ssx:abort_source",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

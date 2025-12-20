load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "context_test",
    timeout = "short",
    srcs = [
        "context_test.cc",
    ],
    deps = [
        "//src/v/context",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "context_frame_test",
    timeout = "short",
    srcs = [
        "context_frame_test.cc",
    ],
    deps = [
        "//src/v/context",
        "//src/v/context:context_frame",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "deadline_timer_test",
    timeout = "short",
    srcs = [
        "deadline_timer_test.cc",
    ],
    deps = [
        "//src/v/context:context_frame",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "abort_source_test",
    timeout = "short",
    srcs = [
        "abort_source_test.cc",
    ],
    deps = [
        "//src/v/context:context_frame",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_bench(
    name = "context_rpbench",
    timeout = "short",
    srcs = [
        "context_bench.cc",
    ],
    deps = [
        "//src/v/context",
        "//src/v/context:context_frame",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

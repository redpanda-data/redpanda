load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "testing",
    srcs = ["common.cc"],
    hdrs = ["common.h"],
    include_prefix = "io/tests",
    visibility = ["//visibility:private"],
    deps = [
        "//src/v/base",
        "//src/v/io",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/strings",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "common_test",
    timeout = "short",
    srcs = ["common_test.cc"],
    deps = [
        ":testing",
        "//src/v/base",
        "//src/v/utils:memory_data_source",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "cache_test",
    timeout = "short",
    srcs = ["cache_test.cc"],
    deps = [
        "//src/v/io",
        "//src/v/test_utils:gtest",
    ],
)

redpanda_cc_gtest(
    name = "interval_map_test",
    timeout = "short",
    srcs = ["interval_map_test.cc"],
    deps = [
        ":testing",
        "//src/v/io",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "persistence_test",
    timeout = "moderate",
    srcs = ["persistence_test.cc"],
    deps = [
        ":testing",
        "//src/v/base",
        "//src/v/io",
        "//src/v/test_utils:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "page_test",
    timeout = "short",
    srcs = ["page_test.cc"],
    deps = [
        ":testing",
        "//src/v/io",
        "//src/v/test_utils:gtest",
    ],
)

redpanda_cc_gtest(
    name = "page_set_test",
    timeout = "short",
    srcs = ["page_set_test.cc"],
    deps = [
        ":testing",
        "//src/v/io",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "io_queue_test",
    timeout = "moderate",
    srcs = ["io_queue_test.cc"],
    # scheduler test is memory hungary since it track copies of all dispatched
    # and completed ios. scaling down cpus give each core more breathing room.
    cpu = 2,
    deps = [
        ":testing",
        "//src/v/base",
        "//src/v/io",
        "//src/v/random:generators",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "scheduler_test",
    timeout = "moderate",
    srcs = ["scheduler_test.cc"],
    # scheduler test is memory hungary since it track copies of all dispatched
    # and completed ios. scaling down cpus give each core more breathing room.
    cpu = 2,
    deps = [
        ":testing",
        "//src/v/base",
        "//src/v/io",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "@boost//:range",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "pager_test",
    timeout = "moderate",
    srcs = ["pager_test.cc"],
    deps = [
        ":testing",
        "//src/v/base",
        "//src/v/io",
        "//src/v/test_utils:gtest",
        "//src/v/utils:memory_data_source",
        "@boost//:range",
        "@seastar",
    ],
)

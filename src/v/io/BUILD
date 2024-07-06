load("//bazel:build.bzl", "redpanda_cc_library")
load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_library(
    name = "io",
    srcs = [
        "io_queue.cc",
        "logger.cc",
        "logger.h",
        "page.cc",
        "page_cache.cc",
        "page_set.cc",
        "pager.cc",
        "paging_data_source.cc",
        "persistence.cc",
        "scheduler.cc",
    ],
    hdrs = [
        "include/io/cache.h",
        "include/io/interval_map.h",
        "include/io/io_queue.h",
        "include/io/page.h",
        "include/io/page_cache.h",
        "include/io/page_set.h",
        "include/io/pager.h",
        "include/io/paging_data_source.h",
        "include/io/persistence.h",
        "include/io/scheduler.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/container:intrusive",
        "//src/v/ssx:future-util",
        "//src/v/ssx:semaphore",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@boost//:intrusive",
        "@boost//:iterator",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "testing",
    srcs = ["tests/common.cc"],
    hdrs = ["tests/common.h"],
    include_prefix = "io",
    visibility = ["//visibility:private"],
    deps = [
        ":io",
        "//src/v/base",
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
    srcs = ["tests/common_test.cc"],
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
    srcs = ["tests/cache_test.cc"],
    deps = [
        ":io",
        "//src/v/test_utils:gtest",
    ],
)

redpanda_cc_gtest(
    name = "interval_map_test",
    timeout = "short",
    srcs = ["tests/interval_map_test.cc"],
    deps = [
        ":io",
        ":testing",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "persistence_test",
    timeout = "moderate",
    srcs = ["tests/persistence_test.cc"],
    deps = [
        ":io",
        ":testing",
        "//src/v/base",
        "//src/v/test_utils:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "page_test",
    timeout = "short",
    srcs = ["tests/page_test.cc"],
    deps = [
        ":io",
        ":testing",
        "//src/v/test_utils:gtest",
    ],
)

redpanda_cc_gtest(
    name = "page_set_test",
    timeout = "short",
    srcs = ["tests/page_set_test.cc"],
    deps = [
        ":io",
        ":testing",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "io_queue_test",
    timeout = "moderate",
    srcs = ["tests/io_queue_test.cc"],
    # scheduler test is memory hungary since it track copies of all dispatched
    # and completed ios. scaling down cpus give each core more breathing room.
    args = ["-c2"],
    deps = [
        ":io",
        ":testing",
        "//src/v/base",
        "//src/v/random:generators",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "scheduler_test",
    timeout = "moderate",
    srcs = ["tests/scheduler_test.cc"],
    # scheduler test is memory hungary since it track copies of all dispatched
    # and completed ios. scaling down cpus give each core more breathing room.
    args = ["-c2"],
    deps = [
        ":io",
        ":testing",
        "//src/v/base",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "@boost//:range",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "pager_test",
    timeout = "short",
    srcs = ["tests/pager_test.cc"],
    deps = [
        ":io",
        ":testing",
        "//src/v/base",
        "//src/v/test_utils:gtest",
        "//src/v/utils:memory_data_source",
        "@boost//:range",
        "@seastar",
    ],
)

load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_cc_gtest(
    name = "keys_test",
    timeout = "short",
    srcs = [
        "keys_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/lsm/core/internal:keys",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_bench(
    name = "keys_bench_rpbench",
    srcs = [
        "keys_bench.cc",
    ],
    deps = [
        "//src/v/lsm/core/internal:keys",
        "//src/v/random:generators",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_gtest(
    name = "files_test",
    timeout = "short",
    srcs = [
        "files_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/lsm/core/internal:files",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "iterator_test_harness",
    hdrs = ["iterator_test_harness.h"],
    visibility = ["//src/v/lsm:__subpackages__"],
    deps = [
        "//src/v/base",
        "//src/v/lsm/core/internal:iterator",
        "//src/v/lsm/core/internal:keys",
        "@googletest//:gtest",
    ],
)

redpanda_test_cc_library(
    name = "throwing_iterator",
    hdrs = ["throwing_iterator.h"],
    visibility = ["//src/v/lsm:__subpackages__"],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/lsm/core/internal:iterator",
        "//src/v/lsm/core/internal:keys",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "merging_iterator_test",
    timeout = "short",
    srcs = [
        "merging_iterator_test.cc",
    ],
    deps = [
        ":iterator_test_harness",
        ":throwing_iterator",
        "//src/v/base",
        "//src/v/lsm/core/internal:iterator",
        "//src/v/lsm/core/internal:keys",
        "//src/v/lsm/core/internal:merging_iterator",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "two_level_iterator_test",
    timeout = "short",
    srcs = [
        "two_level_iterator_test.cc",
    ],
    deps = [
        ":throwing_iterator",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/lsm/core/internal:iterator",
        "//src/v/lsm/core/internal:keys",
        "//src/v/lsm/core/internal:two_level_iterator",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

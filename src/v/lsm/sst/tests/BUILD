load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "iterator_test",
    timeout = "short",
    srcs = [
        "iterator_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/lsm/core/internal:keys",
        "//src/v/lsm/core/internal/tests:iterator_test_harness",
        "//src/v/lsm/io:memory_persistence",
        "//src/v/lsm/sst:block_cache",
        "//src/v/lsm/sst:builder",
        "//src/v/lsm/sst:reader",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "sst_test",
    timeout = "short",
    srcs = [
        "sst_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/lsm/core/internal:keys",
        "//src/v/lsm/io:disk_persistence",
        "//src/v/lsm/io:memory_persistence",
        "//src/v/lsm/sst:block_cache",
        "//src/v/lsm/sst:builder",
        "//src/v/lsm/sst:reader",
        "//src/v/test_utils:gtest",
        "//src/v/utils:uuid",
        "@googletest//:gtest",
        "@seastar",
    ],
)

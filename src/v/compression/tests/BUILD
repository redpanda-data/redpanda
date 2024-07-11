load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_btest", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "lz4_buf_test",
    timeout = "short",
    srcs = [
        "lz4_buf_tests.cc",
    ],
    # the death test doesn't seem to properly record or parse the expected
    # output when there are multiple reactor threads.
    args = ["-c1"],
    deps = [
        "//src/v/base",
        "//src/v/compression",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@lz4",
    ],
)

redpanda_cc_btest(
    name = "zstd_test",
    timeout = "short",
    srcs = [
        "zstd_tests.cc",
    ],
    # with the default core/memory settings each core doesn't have enough memory
    # for this test. by using one core it will have enough memory.
    args = ["-c1"],
    deps = [
        "//src/v/base",
        "//src/v/compression",
        "//src/v/random:generators",
        "//src/v/test_utils:seastar_boost",
        "@seastar//:testing",
    ],
)

redpanda_cc_bench(
    name = "zstd_stream_bench",
    timeout = "short",
    srcs = [
        "zstd_stream_bench.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/compression",
        "//src/v/random:generators",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

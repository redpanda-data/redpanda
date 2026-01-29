load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_btest", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "lz4_buf_test",
    timeout = "short",
    srcs = [
        "lz4_buf_tests.cc",
    ],
    # the death test doesn't seem to properly record or parse the expected
    # output when there are multiple reactor threads.
    cpu = 1,
    deps = [
        "//src/v/base",
        "//src/v/compression",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@lz4",
    ],
)

redpanda_cc_gtest(
    name = "snappy_test",
    timeout = "short",
    srcs = [
        "snappy_tests.cc",
    ],
    cpu = 1,
    data = [
        "//src/v/compression/tests/snappy_payload:little_endian_compressed_data.snappy",
        "//src/v/compression/tests/snappy_payload:uncompressed_data",
    ],
    env = {"SNAPPY_PAYLOAD_PATH": "src/v/compression/tests/snappy_payload"},
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/compression",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "//src/v/utils:file_io",
        "@googletest//:gtest",
        "@seastar",
        "@snappy",
    ],
)

redpanda_cc_gtest(
    name = "gzip_stream_decompression_test",
    timeout = "short",
    srcs = [
        "gzip_stream_decompression_tests.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/compression",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
        "//src/v/utils:base64",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "zstd_test",
    timeout = "moderate",
    srcs = [
        "zstd_tests.cc",
    ],
    # with the default core/memory settings each core doesn't have enough memory
    # for this test. by using one core it will have enough memory.
    cpu = 1,
    deps = [
        "//src/v/base",
        "//src/v/compression",
        "//src/v/random:generators",
        "//src/v/test_utils:seastar_boost",
        "@seastar//:testing",
    ],
)

redpanda_cc_bench(
    name = "zstd_stream_rpbench",
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

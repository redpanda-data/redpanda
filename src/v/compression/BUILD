load("//bazel:build.bzl", "redpanda_cc_library")
load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_btest", "redpanda_cc_gtest")

redpanda_cc_library(
    name = "compression",
    srcs = [
        "async_stream_zstd.cc",
        "compression.cc",
        "internal/gzip_compressor.cc",
        "internal/lz4_frame_compressor.cc",
        "internal/snappy_java_compressor.cc",
        "lz4_decompression_buffers.cc",
        "snappy_standard_compressor.cc",
        "stream_zstd.cc",
    ],
    hdrs = [
        "include/compression/async_stream_zstd.h",
        "include/compression/compression.h",
        "include/compression/internal/gzip_compressor.h",
        "include/compression/internal/lz4_frame_compressor.h",
        "include/compression/internal/snappy_java_compressor.h",
        "include/compression/internal/zstd_compressor.h",
        "include/compression/lz4_decompression_buffers.h",
        "include/compression/snappy_standard_compressor.h",
        "include/compression/stream_zstd.h",
    ],
    exclude_layering_check = True,
    local_defines = ["ZSTD_STATIC_LINKING_ONLY"],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/model",
        "//src/v/utils:object_pool",
        "//src/v/utils:static_deleter_fn",
        "@fmt",
        "@lz4//:lz4_frame",
        "@seastar",
        "@snappy",
        "@zstd",
    ],
)

redpanda_cc_gtest(
    name = "lz4_buf_test",
    timeout = "short",
    srcs = [
        "tests/lz4_buf_tests.cc",
    ],
    # the death test doesn't seem to properly record or parse the expected
    # output when there are multiple reactor threads.
    args = ["-c1"],
    deps = [
        ":compression",
        "//src/v/base",
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
        "tests/zstd_tests.cc",
    ],
    # with the default core/memory settings each core doesn't have enough memory
    # for this test. by using one core it will have enough memory.
    args = ["-c1"],
    deps = [
        ":compression",
        "//src/v/base",
        "//src/v/random:generators",
        "//src/v/test_utils:seastar_boost",
        "@seastar//:testing",
    ],
)

redpanda_cc_bench(
    name = "zstd_stream_bench",
    timeout = "moderate",
    srcs = [
        "tests/zstd_stream_bench.cc",
    ],
    deps = [
        ":compression",
        "//src/v/base",
        "//src/v/random:generators",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

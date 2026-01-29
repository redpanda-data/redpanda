load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_btest", "redpanda_cc_fuzz_test", "redpanda_cc_gtest")

redpanda_cc_btest(
    name = "iobuf_test",
    timeout = "short",
    srcs = [
        "iobuf_tests.cc",
        "utils.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/bytes:iostream",
        "//src/v/bytes:scattered_message",
        "//src/v/bytes:streambuf",
        "//src/v/random:generators",
        "//src/v/test_utils:random_bytes",
        "//src/v/test_utils:seastar_boost",
        "@boost//:range",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "iobuf_mt_test",
    timeout = "moderate",
    srcs = [
        "iobuf_tests_mt.cc",
    ],
    cpu = 8,
    deps = [
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "@boost//:range",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "iobuf_fragment_list_test",
    timeout = "short",
    srcs = [
        "iobuf_fragment_list_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "iobuf_utils_test",
    timeout = "short",
    srcs = [
        "iobuf_utils_tests.cc",
        "utils.h",
    ],
    deps = [
        "//src/v/bytes",
        "//src/v/bytes:hash",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/random:generators",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/hash",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "bytes_test",
    timeout = "short",
    srcs = [
        "bytes_tests.cc",
    ],
    deps = [
        "//src/v/bytes",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_fuzz_test(
    name = "iobuf_fuzz",
    timeout = "short",
    srcs = ["iobuf_fuzz.cc"],
    args = [
        "-max_total_time=30",
        "-rss_limit_mb=8192",
        "-max_len=1048576",
        "-len_control=15",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:scattered_message",
    ],
)

redpanda_cc_bench(
    name = "iobuf_rpbench",
    srcs = ["iobuf_bench.cc"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/random:generators",
        "//src/v/test_utils:random_bytes",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_gtest(
    name = "ioarray_test",
    timeout = "short",
    srcs = [
        "ioarray_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:ioarray",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "iobuf_parser_test",
    timeout = "short",
    srcs = [
        "iobuf_parser_tests.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar//:testing",
    ],
)

redpanda_cc_bench(
    name = "ioarray_rpbench",
    srcs = ["ioarray_bench.cc"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:ioarray",
        "@seastar//:benchmark",
    ],
)

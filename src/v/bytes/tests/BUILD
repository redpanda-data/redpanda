load("//bazel:test.bzl", "redpanda_cc_btest")

redpanda_cc_btest(
    name = "iobuf_test",
    timeout = "short",
    srcs = [
        "iobuf_tests.cc",
        "utils.h",
    ],
    deps = [
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/bytes:iostream",
        "//src/v/bytes:random",
        "//src/v/bytes:scattered_message",
        "//src/v/bytes:streambuf",
        "//src/v/random:generators",
        "//src/v/test_utils:seastar_boost",
        "@boost//:range",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
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

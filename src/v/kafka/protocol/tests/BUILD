load("//bazel:test.bzl", "redpanda_cc_btest")

redpanda_cc_btest(
    name = "field_parser_test",
    timeout = "short",
    srcs = [
        "field_parser_test.cc",
    ],
    deps = [
        "//src/v/bytes:random",
        "//src/v/kafka/protocol",
        "//src/v/random:generators",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:base64",
        "@boost//:iterator",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "batch_reader_test",
    timeout = "short",
    srcs = [
        "batch_reader_test.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/kafka/protocol",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

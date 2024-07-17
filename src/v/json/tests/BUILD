load("//bazel:test.bzl", "redpanda_cc_btest")

redpanda_cc_btest(
    name = "json_serialization_test",
    timeout = "short",
    srcs = [
        "json_serialization_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf_parser",
        "//src/v/json",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)

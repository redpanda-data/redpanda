load("//bazel:test.bzl", "redpanda_cc_btest")

redpanda_cc_btest(
    name = "iobuf",
    timeout = "short",
    srcs = [
        "iobuf.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf_parser",
        "//src/v/json",
        "//src/v/pandaproxy",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)

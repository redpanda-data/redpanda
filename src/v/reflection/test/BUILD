load("//bazel:test.bzl", "redpanda_cc_btest")

redpanda_cc_btest(
    name = "arity_test",
    timeout = "short",
    srcs = [
        "arity_test.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/reflection:arity",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

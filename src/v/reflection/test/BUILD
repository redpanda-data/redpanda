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

redpanda_cc_btest(
    name = "collections_interop",
    timeout = "short",
    srcs = [
        "collections_interop.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/random:generators",
        "//src/v/reflection:adl",
        "//src/v/test_utils:random",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

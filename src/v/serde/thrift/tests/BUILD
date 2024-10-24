load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "compact_test",
    timeout = "short",
    srcs = [
        "compact_test.cc",
    ],
    cpu = 1,
    memory = "128MiB",
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/serde/thrift:compact",
        "//src/v/test_utils:gtest",
        "//src/v/utils:vint",
        "@googletest//:gtest",
        "@googletest//:gtest_main",
        "@seastar",
    ],
)

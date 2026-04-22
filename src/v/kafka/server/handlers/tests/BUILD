load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "produce_transform_test",
    timeout = "short",
    srcs = [
        "produce_transform_test.cc",
    ],
    cpu = 1,
    memory = "64MiB",
    deps = [
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

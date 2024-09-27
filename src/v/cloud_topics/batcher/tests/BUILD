load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "write_request_test",
    timeout = "short",
    srcs = [
        "write_request_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/batcher:write_request",
        "//src/v/cloud_topics/batcher:serializer",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@seastar",
        "@googletest//:gtest",
    ],
)


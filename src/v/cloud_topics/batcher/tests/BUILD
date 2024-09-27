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

redpanda_cc_gtest(
    name = "serializer_test",
    timeout = "short",
    srcs = [
        "serializer_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/batcher:serializer",
        "//src/v/bytes",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "@seastar",
        "@googletest//:gtest",
    ],
)

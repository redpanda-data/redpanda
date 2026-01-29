load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "producer_queue_test",
    timeout = "short",
    srcs = [
        "producer_queue_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_zero/common:producer_queue",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "object_test",
    timeout = "short",
    srcs = [
        "object_test.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf_parser",
        "//src/v/cloud_topics/level_zero/common:object",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

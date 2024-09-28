load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "range_batch_consumer_test",
    timeout = "short",
    srcs = [
        "range_batch_consumer_test.cc",
    ],
    deps = [
        "//src/v/bytes:random",
        "//src/v/cloud_topics/reconciler:range_batch_consumer",
        "//src/v/model",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

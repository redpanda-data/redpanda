load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "range_batch_consumer_test",
    timeout = "short",
    srcs = [
        "range_batch_consumer_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_zero/reconciler:range_batch_consumer",
        "//src/v/model",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random_bytes",
        "@googletest//:gtest",
    ],
)

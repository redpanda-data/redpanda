load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "reconciliation_consumer_test",
    timeout = "short",
    srcs = [
        "reconciliation_consumer_test.cc",
    ],
    deps = [
        "//src/v/bytes:iostream",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/cloud_topics/reconciler:reconciliation_consumer",
        "//src/v/model",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random_bytes",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "reconciler_test",
    timeout = "short",
    srcs = [
        "reconciler_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/common:fake_io",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/cloud_topics/level_one/metastore:simple",
        "//src/v/cloud_topics/reconciler",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random_bytes",
        "@googletest//:gtest",
        "@seastar",
    ],
)

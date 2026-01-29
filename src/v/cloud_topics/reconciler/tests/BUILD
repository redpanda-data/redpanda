load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_cc_gtest(
    name = "adaptive_interval_test",
    timeout = "short",
    srcs = [
        "adaptive_interval_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/reconciler:adaptive_interval",
        "//src/v/config",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_test_cc_library(
    name = "test_utils",
    hdrs = [
        "test_utils.h",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/common:fake_io",
        "//src/v/cloud_topics/level_one/metastore:simple",
        "//src/v/cloud_topics/reconciler",
        "//src/v/model",
        "//src/v/model/tests:random",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "reconciliation_consumer_test",
    timeout = "short",
    srcs = [
        "reconciliation_consumer_test.cc",
    ],
    deps = [
        "//src/v/bytes:iostream",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/cloud_topics/reconciler:reconciler_probe",
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
        ":test_utils",
        "//src/v/cloud_topics/level_one/common:fake_io",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/cloud_topics/level_one/metastore:simple",
        "//src/v/cloud_topics/reconciler",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random_bytes",
        "//src/v/test_utils:scoped_config",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "reconciler_metrics_test",
    timeout = "short",
    srcs = [
        "reconciler_metrics_test.cc",
    ],
    deps = [
        ":test_utils",
        "//src/v/cloud_topics/level_one/common:fake_io",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/cloud_topics/level_one/metastore:simple",
        "//src/v/cloud_topics/reconciler",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:metrics",
        "@googletest//:gtest",
        "@seastar",
    ],
)

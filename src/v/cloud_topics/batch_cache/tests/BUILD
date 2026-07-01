load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "batch_cache_test",
    timeout = "short",
    srcs = [
        "batch_cache_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iostream",
        "//src/v/cloud_topics/batch_cache",
        "//src/v/cluster:client_quota_backend",
        "//src/v/cluster:cluster_utils",
        "//src/v/cluster:controller_log_limiter",
        "//src/v/cluster:data_migration_table",
        "//src/v/cluster:feature_backend",
        "//src/v/cluster:leader_balancer_strategy",
        "//src/v/cluster:node_status_backend",
        "//src/v/cluster:self_test",
        "//src/v/cluster:topic_metrics_watcher",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/redpanda/tests:fixture",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

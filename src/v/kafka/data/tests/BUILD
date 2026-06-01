load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "migrated_partition_test",
    timeout = "short",
    srcs = ["migrated_partition_test.cc"],
    cpu = 1,
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/cluster",
        "//src/v/kafka/data:exact_offset_replicator",
        "//src/v/kafka/data:partition_proxy",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/storage",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

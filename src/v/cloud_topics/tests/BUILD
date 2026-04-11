load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "cluster_fixture",
    hdrs = ["cluster_fixture.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_topics:app",
        "//src/v/cloud_topics:test_fixture_cfg",
        "//src/v/cloud_topics/level_one/metastore:simple_stm",
        "//src/v/cluster/tests:cluster_test_fixture",
        "//src/v/kafka/server/tests:kafka_test_utils",
        "//src/v/model",
        "//src/v/raft",
    ],
)

redpanda_cc_gtest(
    name = "end_to_end_test",
    timeout = "short",
    srcs = [
        "end_to_end_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/bytes:iostream",
        "//src/v/bytes:streambuf",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_topics/level_zero/stm:ctp_stm",
        "//src/v/config",
        "//src/v/kafka/server/tests:kafka_test_utils",
        "//src/v/model",
        "//src/v/model:batch_builder",
        "//src/v/random:generators",
        "//src/v/redpanda/tests:fixture",
        "//src/v/ssx:sformat",
        "//src/v/storage",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "object_utils_test",
    timeout = "short",
    srcs = [
        "object_utils_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics:object_utils",
        "//src/v/ssx:sformat",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "leader_epoch_test",
    timeout = "moderate",
    srcs = [
        "leader_epoch_test.cc",
    ],
    cpu = 1,
    deps = [
        ":cluster_fixture",
        "//src/v/base",
        "//src/v/kafka/server/tests:kafka_test_utils",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "tailing_cache_race_test",
    timeout = "moderate",
    srcs = [
        "tailing_cache_race_test.cc",
    ],
    cpu = 1,
    deps = [
        ":cluster_fixture",
        "//src/v/base",
        "//src/v/kafka/server/tests:kafka_test_utils",
        "//src/v/ssx:sformat",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "//src/v/utils:offset_monitor",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "topic_manifest_uploader_test",
    timeout = "short",
    srcs = ["topic_manifest_uploader_test.cc"],
    cpu = 1,
    deps = [
        ":cluster_fixture",
        "//src/v/base",
        "//src/v/cloud_storage",
        "//src/v/cluster",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "cluster_recovery_test",
    timeout = "moderate",
    srcs = [
        "cluster_recovery_test.cc",
    ],
    cpu = 1,
    deps = [
        ":cluster_fixture",
        "//src/v/cloud_storage",
        "//src/v/cluster",
        "//src/v/cluster/cloud_metadata/tests:cluster_metadata_utils",
        "//src/v/kafka/server/tests:kafka_test_utils",
        "//src/v/random:generators",
        "//src/v/ssx:future_util",
        "//src/v/ssx:sformat",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "//src/v/utils:retry_chain_node",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "read_replica_test",
    timeout = "moderate",
    srcs = ["read_replica_test.cc"],
    cpu = 1,
    deps = [
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_topics/level_one/metastore:replicated_metastore",
        "//src/v/kafka/server/tests:kafka_test_utils",
        "//src/v/model",
        "//src/v/redpanda/tests:fixture",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

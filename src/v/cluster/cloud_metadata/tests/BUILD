load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "cluster_metadata_utils",
    hdrs = [
        "cluster_metadata_utils.h",
    ],
    implementation_deps = [
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cluster",
        "//src/v/cluster/tests:topic_properties_generator",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/security",
    ],
)

redpanda_test_cc_library(
    name = "manual_mixin",
    hdrs = [
        "manual_mixin.h",
    ],
    implementation_deps = [
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/test_utils:scoped_config",
    ],
)

redpanda_cc_gtest(
    name = "cluster_manifest_test",
    timeout = "short",
    srcs = [
        "cluster_manifest_test.cc",
    ],
    deps = [
        "//src/v/bytes:iostream",
        "//src/v/cluster",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "cluster_recovery_backend_test",
    timeout = "short",
    srcs = [
        "cluster_recovery_backend_test.cc",
    ],
    cpu = 1,
    deps = [
        ":cluster_metadata_utils",
        ":manual_mixin",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage:types",
        "//src/v/cluster",
        "//src/v/cluster/tests:topic_properties_generator",
        "//src/v/cluster/tests:tx_compaction_utils_gtest",
        "//src/v/config",
        "//src/v/model",
        "//src/v/redpanda:application",
        "//src/v/redpanda/tests:fixture",
        "//src/v/security",
        "//src/v/security/tests:utils",
        "//src/v/ssx:future_util",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "//src/v/utils:retry_chain_node",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "controller_snapshot_reconciliation_test",
    timeout = "short",
    srcs = [
        "controller_snapshot_reconciliation_test.cc",
    ],
    cpu = 1,
    deps = [
        ":cluster_metadata_utils",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_storage",
        "//src/v/cluster",
        "//src/v/cluster/tests:tx_compaction_utils_gtest",
        "//src/v/config",
        "//src/v/model",
        "//src/v/redpanda:application",
        "//src/v/redpanda/tests:fixture",
        "//src/v/security",
        "//src/v/security/tests:utils",
        "//src/v/ssx:future_util",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "manifest_downloads_test",
    timeout = "short",
    srcs = [
        "manifest_downloads_test.cc",
    ],
    cpu = 1,
    deps = [
        ":manual_mixin",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_storage",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/redpanda:application",
        "//src/v/redpanda/tests:fixture",
        "//src/v/test_utils:gtest",
        "//src/v/utils:retry_chain_node",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "offsets_lookup_batcher_test",
    timeout = "short",
    srcs = [
        "offsets_lookup_batcher_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cluster",
        "//src/v/cluster:offsets_lookup",
        "//src/v/cluster/tests:cluster_test_fixture",
        "//src/v/model",
        "//src/v/redpanda:application",
        "//src/v/redpanda/tests:fixture",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "offsets_lookup_test",
    timeout = "short",
    srcs = [
        "offsets_lookup_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cluster",
        "//src/v/cluster:offsets_lookup",
        "//src/v/random:generators",
        "//src/v/redpanda/tests:fixture",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "producer_id_recovery_test",
    timeout = "short",
    srcs = [
        "producer_id_recovery_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cloud_storage/tests:common",
        "//src/v/cluster",
        "//src/v/cluster:offsets_lookup",
        "//src/v/cluster/tests:tx_compaction_utils_gtest",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "uploader_test",
    timeout = "moderate",
    srcs = [
        "uploader_test.cc",
    ],
    cpu = 1,
    deps = [
        ":manual_mixin",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage:types",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/redpanda:application",
        "//src/v/redpanda/tests:fixture",
        "//src/v/storage",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "//src/v/utils:retry_chain_node",
        "@googletest//:gtest",
        "@seastar",
    ],
)

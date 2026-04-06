load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "builders",
    hdrs = [
        "builders.h",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/metastore",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "state_utils",
    srcs = [
        "state_utils.cc",
    ],
    hdrs = [
        "state_utils.h",
    ],
    implementation_deps = [
        "//src/v/cloud_topics/level_one/metastore/lsm:keys",
        "//src/v/cloud_topics/level_one/metastore/lsm:values",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_topics/level_one/metastore:state",
        "//src/v/lsm",
    ],
)

redpanda_cc_gtest(
    name = "simple_metastore_test",
    timeout = "short",
    srcs = [
        "simple_metastore_test.cc",
    ],
    deps = [
        ":builders",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/metastore:simple",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "garbage_collector_test",
    timeout = "short",
    srcs = [
        "garbage_collector_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cloud_io:remote",
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_topics:app",
        "//src/v/cloud_topics/level_one/common:fake_io",
        "//src/v/cloud_topics/level_one/metastore:garbage_collector",
        "//src/v/cloud_topics/level_one/metastore:simple_stm",
        "//src/v/cloud_topics/level_one/metastore:state_update",
        "//src/v/cloud_topics/tests:cluster_fixture",
        "//src/v/config",
        "//src/v/model",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "replicated_metastore_test",
    timeout = "moderate",
    srcs = [
        "replicated_metastore_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cloud_storage:remote_label",
        "//src/v/cloud_topics:app",
        "//src/v/cloud_topics/level_one/metastore:domain_uuid",
        "//src/v/cloud_topics/level_one/metastore:manifest_io",
        "//src/v/cloud_topics/level_one/metastore:metastore_manifest",
        "//src/v/cloud_topics/level_one/metastore:replicated_metastore",
        "//src/v/cloud_topics/level_one/metastore:state",
        "//src/v/cloud_topics/level_one/metastore/lsm:stm",
        "//src/v/cloud_topics/level_one/metastore/tests:state_utils",
        "//src/v/cloud_topics/tests:cluster_fixture",
        "//src/v/lsm",
        "//src/v/lsm/io:cloud_persistence",
        "//src/v/lsm/io:memory_persistence",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "state_test",
    timeout = "short",
    srcs = [
        "state_test.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/cloud_topics/level_one/metastore:state",
        "//src/v/serde",
        "//src/v/serde:named_type",
        "//src/v/serde:uuid",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "state_update_test",
    timeout = "short",
    srcs = [
        "state_update_test.cc",
    ],
    deps = [
        ":state_utils",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/metastore:state",
        "//src/v/cloud_topics/level_one/metastore:state_update",
        "//src/v/cloud_topics/level_one/metastore/lsm:keys",
        "//src/v/cloud_topics/level_one/metastore/lsm:state_reader",
        "//src/v/cloud_topics/level_one/metastore/lsm:state_update",
        "//src/v/cloud_topics/level_one/metastore/lsm:values",
        "//src/v/cloud_topics/level_one/metastore/lsm:write_batch_row",
        "//src/v/container:chunked_vector",
        "//src/v/lsm",
        "//src/v/lsm/io:memory_persistence",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/test_utils:gtest",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/strings",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "topic_purger_test",
    timeout = "short",
    srcs = [
        "topic_purger_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cloud_io:remote",
        "//src/v/cloud_topics/level_one/metastore:simple",
        "//src/v/cloud_topics/level_one/metastore:topic_purger",
        "//src/v/cluster",
        "//src/v/cluster:nt_revision",
        "//src/v/config",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "offset_interval_set_test",
    timeout = "short",
    srcs = [
        "offset_interval_set_test.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/cloud_topics/level_one/metastore:offset_interval_set",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "partition_validator_test",
    timeout = "short",
    srcs = [
        "partition_validator_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/metastore:partition_validator",
        "//src/v/cloud_topics/level_one/metastore/lsm:keys",
        "//src/v/cloud_topics/level_one/metastore/lsm:values",
        "//src/v/lsm",
        "//src/v/lsm/io:memory_persistence",
        "//src/v/serde",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "extent_metadata_reader_test",
    timeout = "short",
    srcs = [
        "extent_metadata_reader_test.cc",
    ],
    deps = [
        ":builders",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/metastore:extent_metadata_reader",
        "//src/v/cloud_topics/level_one/metastore:simple",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

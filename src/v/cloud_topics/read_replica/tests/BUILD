load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "db_utils",
    hdrs = [
        "db_utils.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_topics/level_one/metastore:domain_uuid",
        "//src/v/cloud_topics/level_one/metastore/lsm:state_reader",
        "//src/v/cloud_topics/level_one/metastore/lsm:state_update",
        "//src/v/cloud_topics/level_one/metastore/lsm:write_batch_row",
        "//src/v/container:chunked_hash_map",
        "//src/v/lsm",
        "//src/v/lsm/io:cloud_persistence",
        "//src/v/model",
        "//src/v/ssx:time",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "snapshot_manager_test",
    timeout = "short",
    srcs = [
        "snapshot_manager_test.cc",
    ],
    cpu = 1,
    deps = [
        ":db_utils",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_io/tests:scoped_remote",
        "//src/v/cloud_topics/level_one/metastore:domain_uuid",
        "//src/v/cloud_topics/read_replica:snapshot_manager",
        "//src/v/cloud_topics/read_replica:snapshot_metastore",
        "//src/v/lsm",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "//src/v/test_utils:tmp_dir",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "state_refresh_loop_test",
    timeout = "short",
    srcs = [
        "state_refresh_loop_test.cc",
    ],
    cpu = 1,
    deps = [
        ":db_utils",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_io/tests:scoped_remote",
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/level_one/metastore:domain_uuid",
        "//src/v/cloud_topics/level_one/metastore:manifest_io",
        "//src/v/cloud_topics/level_one/metastore:metastore_manifest",
        "//src/v/cloud_topics/level_one/metastore:state_update",
        "//src/v/cloud_topics/read_replica:snapshot_manager",
        "//src/v/cloud_topics/read_replica:state_refresh_loop",
        "//src/v/cloud_topics/read_replica:stm",
        "//src/v/lsm",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/test_utils:async",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "//src/v/test_utils:tmp_dir",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "db_domain_manager_test",
    timeout = "moderate",
    srcs = [
        "db_domain_manager_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cloud_io:remote",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_io/tests:scoped_remote",
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/domain:db_domain_manager",
        "//src/v/cloud_topics/level_one/metastore:rpc_types",
        "//src/v/cloud_topics/level_one/metastore:state_update",
        "//src/v/cloud_topics/level_one/metastore/lsm:stm",
        "//src/v/config",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/test_utils:async",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "//src/v/test_utils:tmp_dir",
        "@googletest//:gtest",
        "@seastar",
    ],
)

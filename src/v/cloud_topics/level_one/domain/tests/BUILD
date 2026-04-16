load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "entity_lock_map_test",
    timeout = "short",
    srcs = [
        "entity_lock_map_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/domain:db_domain_manager",
        "//src/v/test_utils:async",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "db_domain_manager_test",
    timeout = "moderate",
    srcs = [
        "db_domain_manager_test.cc",
    ],
    cpu = 1,
    memory = "2GiB",
    deps = [
        "//src/v/cloud_io:remote",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_io/tests:scoped_remote",
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_topics/level_one/common:file_io",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/common:object_utils",
        "//src/v/cloud_topics/level_one/domain:db_domain_manager",
        "//src/v/cloud_topics/level_one/domain:domain_manager_probe",
        "//src/v/cloud_topics/level_one/metastore:domain_uuid",
        "//src/v/cloud_topics/level_one/metastore:rpc_types",
        "//src/v/cloud_topics/level_one/metastore:state_update",
        "//src/v/cloud_topics/level_one/metastore/lsm:state_reader",
        "//src/v/cloud_topics/level_one/metastore/lsm:state_update",
        "//src/v/cloud_topics/level_one/metastore/lsm:stm",
        "//src/v/cloud_topics/level_one/metastore/lsm:write_batch_row",
        "//src/v/config",
        "//src/v/container:chunked_vector",
        "//src/v/lsm/io:cloud_persistence",
        "//src/v/lsm/io:persistence",
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

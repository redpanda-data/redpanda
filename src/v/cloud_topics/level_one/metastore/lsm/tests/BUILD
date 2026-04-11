load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "debug_reader_test",
    timeout = "short",
    srcs = [
        "debug_reader_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/metastore:state",
        "//src/v/cloud_topics/level_one/metastore/lsm:debug_reader",
        "//src/v/cloud_topics/level_one/metastore/lsm:debug_serde",
        "//src/v/cloud_topics/level_one/metastore/lsm:debug_writer",
        "//src/v/cloud_topics/level_one/metastore/lsm:keys",
        "//src/v/cloud_topics/level_one/metastore/lsm:values",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/test_utils:gtest",
        "//src/v/utils:uuid",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "debug_writer_test",
    timeout = "short",
    srcs = [
        "debug_writer_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/metastore:state",
        "//src/v/cloud_topics/level_one/metastore/lsm:debug_writer",
        "//src/v/cloud_topics/level_one/metastore/lsm:keys",
        "//src/v/cloud_topics/level_one/metastore/lsm:values",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/test_utils:gtest",
        "//src/v/utils:uuid",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "lsm_update_test",
    timeout = "short",
    srcs = [
        "lsm_update_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/metastore/lsm:lsm_update",
        "//src/v/cloud_topics/level_one/metastore/lsm:state",
        "//src/v/cloud_topics/level_one/metastore/lsm:write_batch_row",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "keys_test",
    timeout = "short",
    srcs = [
        "keys_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/metastore/lsm:keys",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "state_reader_test",
    timeout = "short",
    srcs = [
        "state_reader_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/metastore:state",
        "//src/v/cloud_topics/level_one/metastore/lsm:keys",
        "//src/v/cloud_topics/level_one/metastore/lsm:state_reader",
        "//src/v/cloud_topics/level_one/metastore/lsm:values",
        "//src/v/lsm",
        "//src/v/lsm/io:memory_persistence",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "replicated_db_test",
    timeout = "moderate",
    srcs = [
        "replicated_db_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cloud_io:remote",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_io/tests:scoped_remote",
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_topics/level_one/metastore:domain_uuid",
        "//src/v/cloud_topics/level_one/metastore/lsm:replicated_db",
        "//src/v/cloud_topics/level_one/metastore/lsm:stm",
        "//src/v/cloud_topics/level_one/metastore/lsm:write_batch_row",
        "//src/v/config",
        "//src/v/container:chunked_vector",
        "//src/v/lsm/io:cloud_persistence",
        "//src/v/lsm/io:persistence",
        "//src/v/model",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "//src/v/test_utils:tmp_dir",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "state_update_test",
    timeout = "short",
    srcs = [
        "state_update_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/metastore:state",
        "//src/v/cloud_topics/level_one/metastore:state_update",
        "//src/v/cloud_topics/level_one/metastore/lsm:keys",
        "//src/v/cloud_topics/level_one/metastore/lsm:state_reader",
        "//src/v/cloud_topics/level_one/metastore/lsm:state_update",
        "//src/v/cloud_topics/level_one/metastore/lsm:values",
        "//src/v/cloud_topics/level_one/metastore/lsm:write_batch_row",
        "//src/v/lsm",
        "//src/v/lsm/io:memory_persistence",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

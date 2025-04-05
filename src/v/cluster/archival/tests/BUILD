load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "replica_state_validator_test",
    timeout = "short",
    srcs = [
        "replica_state_validator_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cloud_storage",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/storage",
        "//src/v/storage/tests:disk_log_builder",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:seastar_boost",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "archival_metadata_stm_test",
    timeout = "short",
    srcs = [
        "archival_metadata_stm_test.cc",
    ],
    deps = [
        "//src/v/cloud_storage",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/http/tests:utils",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/raft/tests:simple_raft_fixture",
        "//src/v/storage",
        "//src/v/storage/tests:disk_log_builder",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "archival_metadata_stm_gtest",
    timeout = "short",
    srcs = [
        "archival_metadata_stm_gtest.cc",
    ],
    deps = [
        "//src/v/cloud_io:remote",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage_clients",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/http/tests:utils",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/storage",
        "//src/v/storage/tests:disk_log_builder",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:available_promise",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

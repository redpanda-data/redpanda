load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "simple_raft_fixture",
    hdrs = [
        "simple_raft_fixture.h",
    ],
    include_prefix = "raft/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/config",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/rpc",
        "//src/v/storage",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:unresolved_address",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "simple_record_fixture",
    hdrs = [
        "simple_record_fixture.h",
    ],
    include_prefix = "raft/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/storage:record_batch_builder",
    ],
)

redpanda_test_cc_library(
    name = "raft_fixiture_retry_policy",
    hdrs = [
        "raft_fixture_retry_policy.h",
    ],
    include_prefix = "raft/tests",
    deps = [
        ":raft_fixture",
        "//src/v/raft",
    ],
)

redpanda_test_cc_library(
    name = "raft_fixture",
    srcs = ["raft_fixture.cc"],
    hdrs = [
        "raft_fixture.h",
    ],
    include_prefix = "raft/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:random",
        "//src/v/config",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/rpc",
        "//src/v/storage",
        "//src/v/test_utils:gtest",
        "//src/v/utils:unresolved_address",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "jitter_test",
    timeout = "short",
    srcs = [
        "jitter_tests.cc",
    ],
    deps = [
        "//src/v/config",
        "//src/v/raft",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "bootstrap_configuration_test",
    timeout = "short",
    srcs = [
        "bootstrap_configuration_test.cc",
    ],
    deps = [
        ":simple_record_fixture",
        "//src/v/config",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/resource_mgmt:io_priority",
        "//src/v/storage",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:random",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "heartbeat_test",
    timeout = "short",
    srcs = [
        "heartbeats_test.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf_parser",
        "//src/v/config",
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/test_utils:random",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "consensus_utils_test",
    timeout = "short",
    srcs = [
        "consensus_utils_test.cc",
    ],
    deps = [
        ":simple_record_fixture",
        "//src/v/config",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/resource_mgmt:io_priority",
        "//src/v/storage",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:random",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:unresolved_address",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "group_configuration_test",
    timeout = "short",
    srcs = [
        "group_configuration_tests.cc",
    ],
    deps = [
        ":simple_record_fixture",
        "//src/v/config",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/resource_mgmt:io_priority",
        "//src/v/storage",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:random",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:unresolved_address",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

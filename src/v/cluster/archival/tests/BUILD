load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "service_fixture",
    srcs = ["service_fixture.cc"],
    hdrs = ["service_fixture.h"],
    visibility = ["//visibility:private"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage:types",
        "//src/v/cloud_storage_clients",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/http/tests:utils",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/redpanda/tests:fixture",
        "//src/v/storage",
        "//src/v/test_utils:seastar_boost",
        "@boost//:algorithm",
        "@boost//:core",
        "@boost//:property_tree",
        "@boost//:test",
        "@seastar",
    ],
)

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
        "//src/v/cloud_storage:types",
        "//src/v/cloud_storage_clients",
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
        "//src/v/cloud_storage:types",
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
        "//src/v/utils:available_promise",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "segment_reupload_test",
    timeout = "short",
    srcs = [
        "segment_reupload_test.cc",
    ],
    deps = [
        "//src/v/cloud_storage",
        "//src/v/cloud_storage:types",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/storage:record_batch_utils",
        "//src/v/storage/tests:disk_log_builder",
        "//src/v/test_utils:archival",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "ntp_archiver_test",
    timeout = "moderate",
    srcs = [
        "ntp_archiver_test.cc",
    ],
    cpu = 1,
    deps = [
        ":service_fixture",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage:types",
        "//src/v/cloud_storage/tests:common",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/ssx:sformat",
        "//src/v/storage",
        "//src/v/test_utils:scoped_config",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "ntp_archiver_reupload_test",
    timeout = "short",
    srcs = [
        "ntp_archiver_reupload_test.cc",
    ],
    cpu = 1,
    deps = [
        ":service_fixture",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage_clients",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/storage",
        "//src/v/test_utils:scoped_config",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "retention_strategy_test",
    timeout = "short",
    srcs = [
        "retention_strategy_test.cc",
    ],
    cpu = 1,
    deps = [
        ":service_fixture",
        "//src/v/cluster",
        "//src/v/storage/tests:disk_log_builder",
        "//src/v/test_utils:archival",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_test_cc_library(
    name = "async_data_uploader_fixture",
    srcs = [
        "async_data_uploader_fixture.cc",
    ],
    hdrs = [
        "async_data_uploader_fixture.h",
    ],
    visibility = ["//visibility:private"],
    deps = [
        "//src/v/bytes:iostream",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/kafka/client",
        "//src/v/kafka/server/tests:kafka_test_utils",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/redpanda/tests:fixture",
        "//src/v/storage",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "async_data_uploader_test",
    timeout = "moderate",
    srcs = [
        "async_data_uploader_test.cc",
    ],
    cpu = 1,
    deps = [
        ":async_data_uploader_fixture",
        "//src/v/bytes:iostream",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/kafka/client",
        "//src/v/kafka/server/tests:kafka_test_utils",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/redpanda/tests:fixture",
        "//src/v/storage",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_test_cc_library(
    name = "archival_service_fixture",
    hdrs = ["archival_service_fixture.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":service_fixture",
        "//src/v/cloud_storage_clients",
        "//src/v/cluster",
        "//src/v/cluster/tests:cluster_test_fixture",
        "//src/v/config",
        "//src/v/container:chunked_vector",
        "//src/v/http/tests:utils",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "archiver_manager_test",
    timeout = "long",
    srcs = [
        "archiver_manager_test.cc",
    ],
    cpu = 1,
    memory = "2048MiB",
    deps = [
        ":archival_service_fixture",
        "//src/v/cluster",
        "//src/v/cluster/tests:cluster_test_fixture",
        "//src/v/cluster/tests:utils",
        "//src/v/config",
        "//src/v/container:chunked_vector",
        "//src/v/http/tests:utils",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/redpanda/tests:fixture",
        "//src/v/storage",
        "//src/v/test_utils:scoped_config",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "upload_housekeeping_service_test",
    timeout = "short",
    srcs = [
        "upload_housekeeping_service_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:retry_chain_node",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "scrubber_scheduler_test",
    timeout = "short",
    srcs = [
        "scrubber_scheduler_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
    ],
)

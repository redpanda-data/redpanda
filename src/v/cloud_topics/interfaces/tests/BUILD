load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

package(default_visibility = [
    "//src/v/cloud_topics/L0_read_path/tests:__pkg__",
])

redpanda_test_cc_library(
    name = "common",
    hdrs = [
        "partition_data_generator.h",
    ],
    include_prefix = "cloud_topics/interfaces/tests",
    deps = [
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/kafka/server/tests:kafka_test_utils",
        "//src/v/redpanda/tests:fixture",
        "//src/v/storage",
        "//src/v/test_utils:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "cluster_partition_manager_test",
    timeout = "short",
    srcs = [
        "cluster_partition_manager_test.cc",
    ],
    cpu = 1,
    deps = [
        ":common",
        "//src/v/base",
        "//src/v/cloud_topics/interfaces:cluster_partition_manager",
        "//src/v/model",
        "//src/v/redpanda/tests:fixture",
        "//src/v/test_utils:gtest",
        "//src/v/utils:move_canary",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_test_cc_library(
    name = "mocks",
    hdrs = [
        "mocks.h",
    ],
    include_prefix = "cloud_topics/interfaces/tests",
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics/interfaces:cluster_partition_manager",
        "//src/v/cluster",
        "//src/v/test_utils:gtest",
        "@seastar",
    ],
)

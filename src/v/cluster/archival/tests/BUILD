load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_cc_gtest(
    name = "archival_policy_test",
    timeout = "short",
    srcs = [
        "archival_policy_test.cc",
    ],
    deps = [
        "//src/v/base",
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

redpanda_test_cc_library(
    name = "archival_service_fixture",
    hdrs = [
        "archival_service_fixture.h",
        "service_fixture.h",
    ],
    include_prefix = "cluster/archival/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/http/tests:utils",
        "//src/v/model",
        "//src/v/redpanda/tests:fixture_gtest",
        "//src/v/storage",
        "//src/v/storage/tests:disk_log_builder",
    ],
)

redpanda_cc_gtest(
    name = "archiver_manager_test",
    timeout = "short",
    srcs = [
        "archiver_manager_test.cc",
    ],
    deps = [
        ":archival_service_fixture",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/cluster/tests:cluster_test_fixture",
        "//src/v/cluster/tests:utils",
        "//src/v/config",
        "//src/v/container:fragmented_vector",
        "//src/v/http/tests:utils",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/storage/tests:disk_log_builder",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "//src/v/test_utils:seastar_boost",
        "@googletest//:gtest",
        "@seastar",
    ],
)

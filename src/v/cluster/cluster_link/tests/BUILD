load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "test_lib",
    srcs = [
        "utils.cc",
    ],
    hdrs = [
        "utils.h",
    ],
    implementation_deps = [
        "//src/v/cluster",
        "//src/v/cluster:commands",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cluster:table_utils",
        "//src/v/cluster_link/model",
        "//src/v/model",
    ],
)

redpanda_cc_gtest(
    name = "table_test",
    timeout = "short",
    srcs = [
        "table_test.cc",
    ],
    deps = [
        ":test_lib",
        "//src/v/cluster:cluster_link_rpc_types",
        "//src/v/cluster:cluster_link_table",
        "//src/v/cluster:commands",
        "//src/v/cluster:controller_snapshot",
        "//src/v/cluster:table_utils",
        "//src/v/cluster:types",
        "//src/v/cluster_link/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "shadow_link_write_blocking",
    timeout = "short",
    srcs = [
        "shadow_link_write_blocking.cc",
    ],
    cpu = 1,
    deps = [
        ":test_lib",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/cluster:client_quota_backend",
        "//src/v/cluster:cluster_link_table",
        "//src/v/cluster:cluster_link_types",
        "//src/v/cluster:cluster_recovery_table",
        "//src/v/cluster:commands",
        "//src/v/cluster:controller_log_limiter",
        "//src/v/cluster:data_migration_table",
        "//src/v/cluster:feature_backend",
        "//src/v/cluster:notification",
        "//src/v/cluster:plugin_backend",
        "//src/v/cluster_link/model",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/test_utils:gtest",
        "//src/v/utils:unresolved_address",
        "//src/v/utils:uuid",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "frontend_validation_test",
    timeout = "short",
    srcs = [
        "frontend_validation_test.cc",
    ],
    deps = [
        ":test_lib",
        "//src/v/cluster",
        "//src/v/cluster:client_quota_backend",
        "//src/v/cluster:cluster_link_table",
        "//src/v/cluster:cluster_link_types",
        "//src/v/cluster:cluster_recovery_table",
        "//src/v/cluster:commands",
        "//src/v/cluster:controller_log_limiter",
        "//src/v/cluster:data_migration_table",
        "//src/v/cluster:feature_backend",
        "//src/v/cluster:notification",
        "//src/v/cluster:plugin_backend",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "//src/v/utils:unresolved_address",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@boost//:range",
        "@googletest//:gtest",
        "@seastar",
    ],
)

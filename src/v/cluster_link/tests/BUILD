load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "test_deps",
    srcs = [
        "deps.cc",
    ],
    hdrs = [
        "deps.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cluster",
        "//src/v/cluster/cluster_link/tests:test_lib",
        "//src/v/cluster_link:impl",
        "//src/v/cluster_link:utils",
        "//src/v/cluster_link/replication/tests:deps_test_impl",
        "//src/v/config",
        "//src/v/kafka/client/test:cluster_mock",
        "//src/v/kafka/data/rpc",
        "//src/v/kafka/data/rpc/test:test_deps",
        "//src/v/model",
        "//src/v/security",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "link_test",
    timeout = "short",
    srcs = [
        "link_test.cc",
    ],
    deps = [
        ":test_deps",
        "//src/v/cluster",
        "//src/v/cluster/cluster_link/tests:test_lib",
        "//src/v/cluster_link:impl",
        "//src/v/cluster_link/replication/tests:deps_test_impl",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "errc_test",
    timeout = "short",
    srcs = [
        "errc_test.cc",
    ],
    deps = [
        "//src/v/cluster_link:errc",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "base_task_test",
    timeout = "short",
    srcs = [
        "base_task_test.cc",
    ],
    deps = [
        "//src/v/cluster_link:impl",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "task_manager_integration_test",
    timeout = "short",
    srcs = [
        "task_manager_integration_test.cc",
    ],
    flaky = True,
    deps = [
        ":test_deps",
        "//src/v/cluster_link:impl",
        "//src/v/cluster_link/model",
        "//src/v/kafka/data/rpc",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "utils_test",
    timeout = "short",
    srcs = [
        "utils_test.cc",
    ],
    deps = [
        "//src/v/cluster_link:utils",
        "//src/v/cluster_link/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "source_topic_syncer_test",
    timeout = "short",
    srcs = [
        "source_topic_syncer_test.cc",
        "topic_properties_syncer_test.cc",
    ],
    deps = [
        ":test_deps",
        "//src/v/cluster_link:source_topic_syncer",
        "//src/v/kafka/server:topic_config_utils",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "topic_reconciler_test",
    timeout = "short",
    srcs = [
        "topic_reconciler_test.cc",
    ],
    deps = [
        ":test_deps",
        "//src/v/cluster_link:impl",
        "//src/v/config",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "group_mirroring_task",
    timeout = "short",
    srcs = [
        "group_mirroring_task_test.cc",
    ],
    deps = [
        ":test_deps",
        "//src/v/cluster_link:group_mirroring_task",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "security_migrator_test",
    timeout = "short",
    srcs = [
        "security_migrator_test.cc",
    ],
    deps = [
        ":test_deps",
        "//src/v/cluster_link:security_migrator",
        "//src/v/kafka/server:security",
        "//src/v/security",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

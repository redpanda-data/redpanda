load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_cc_gtest(
    name = "mirroring_task_test",
    timeout = "short",
    srcs = [
        "mirroring_task_test.cc",
    ],
    cpu = 1,
    deps = [
        ":sr_sync_test_fixtures",
        "//src/v/cluster:cluster_link_table",
        "//src/v/cluster:fwd",
        "//src/v/cluster_link:impl",
        "//src/v/cluster_link/model",
        "//src/v/cluster_link/schema_registry_sync:mirroring_task",
        "//src/v/cluster_link/schema_registry_sync:source_reader",
        "//src/v/cluster_link/tests:test_deps",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/metrics",
        "//src/v/model",
        "//src/v/pandaproxy/schema_registry:types",
        "//src/v/schema:registry",
        "//src/v/schema/tests:fake_registry",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:metrics",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "sr_sync_test_fixtures",
    hdrs = ["sr_sync_test_fixtures.h"],
    visibility = ["//visibility:private"],
    deps = [
        "//src/v/cluster_link/schema_registry_sync:mirroring_task",
        "//src/v/cluster_link/schema_registry_sync:source_reader",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/pandaproxy/schema_registry:types",
        "//src/v/schema:registry",
        "//src/v/schema/tests:fake_registry",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "reconciler_test",
    timeout = "short",
    srcs = ["reconciler_test.cc"],
    cpu = 1,
    deps = [
        ":sr_sync_test_fixtures",
        "//src/v/cluster_link/schema_registry_sync:mirroring_task",
        "//src/v/cluster_link/schema_registry_sync:source_reader",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/pandaproxy/schema_registry:types",
        "//src/v/schema:registry",
        "//src/v/schema/tests:fake_registry",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "http_source_reader_test",
    timeout = "short",
    srcs = [
        "http_source_reader_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/cluster_link/model",
        "//src/v/cluster_link/schema_registry_sync:http_source_reader",
        "//src/v/http",
        "//src/v/pandaproxy/schema_registry:types",
        "//src/v/pandaproxy/schema_registry/rest_client:client",
        "//src/v/test_utils:async",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "scope_test",
    timeout = "short",
    srcs = ["scope_test.cc"],
    cpu = 1,
    deps = [
        "//src/v/cluster_link/model",
        "//src/v/cluster_link/schema_registry_sync:mirroring_task",
        "//src/v/container:chunked_hash_map",
        "//src/v/pandaproxy/schema_registry:types",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

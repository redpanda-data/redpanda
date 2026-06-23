load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "mirroring_task_test",
    timeout = "short",
    srcs = [
        "mirroring_task_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cluster:cluster_link_table",
        "//src/v/cluster:fwd",
        "//src/v/cluster_link:impl",
        "//src/v/cluster_link/model",
        "//src/v/cluster_link/schema_registry_sync:mirroring_task",
        "//src/v/cluster_link/schema_registry_sync:source_reader",
        "//src/v/cluster_link/tests:test_deps",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/pandaproxy/schema_registry:types",
        "//src/v/schema:registry",
        "//src/v/schema/tests:fake_registry",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

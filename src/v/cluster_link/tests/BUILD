load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "source_cluster",
    srcs = [
        "source_cluster.cc",
    ],
    hdrs = [
        "source_cluster.h",
    ],
    include_prefix = "cluster_link/tests",
    visibility = ["//src/v/cluster_link/tests:__subpackages__"],
    deps = [
        "//src/v/cluster",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:fragmented_vector",
        "//src/v/model",
    ],
)

redpanda_test_cc_library(
    name = "test_utils",
    srcs = [
        "test_utils.cc",
    ],
    hdrs = [
        "test_utils.h",
    ],
    include_prefix = "cluster_link/tests",
    visibility = ["//src/v/cluster_link/tests:__subpackages__"],
    deps = [
        ":source_cluster",
        "//src/v/cluster_link:impl",
        "//src/v/kafka/server",
    ],
)

redpanda_cc_gtest(
    name = "link_test",
    timeout = "short",
    srcs = [
        "link_test.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/cluster/cluster_link/tests:test_lib",
        "//src/v/cluster_link:impl",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@googletest//:gtest",
        "@seastar",
    ],
)

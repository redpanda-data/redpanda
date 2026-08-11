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
        "//src/v/cluster",
        "//src/v/cluster:table_utils",
        "//src/v/cluster_link/model",
        "//src/v/test_utils:gtest",
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
        "//src/v/model",
        "//src/v/raft",
        "//src/v/test_utils:gtest",
        "//src/v/utils:unresolved_address",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@boost//:range",
        "@googletest//:gtest",
        "@seastar",
    ],
)

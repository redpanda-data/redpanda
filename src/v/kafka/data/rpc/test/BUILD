load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "test_deps",
    srcs = [
        "deps.cc",
    ],
    hdrs = [
        "deps.h",
    ],
    include_prefix = "kafka/data/rpc/test",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/kafka/data/rpc",
        "//src/v/model/tests:random",
        "@abseil-cpp//absl/container:flat_hash_map",
    ],
)

redpanda_cc_gtest(
    name = "kafka_data_rpc_test",
    timeout = "short",
    srcs = ["kafka_data_rpc_test.cc"],
    cpu = 1,
    memory = "256MiB",
    deps = [
        ":test_deps",
        "//src/v/kafka/data/rpc",
        "//src/v/model",
        "//src/v/net",
        "//src/v/random:generators",
        "//src/v/rpc",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
        "@googletest//:gtest",
        "@seastar",
    ],
)

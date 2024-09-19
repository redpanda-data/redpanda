load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "rpc_test",
    timeout = "short",
    srcs = ["transform_rpc_test.cc"],
    cpu = 1,
    memory = "256MiB",
    deps = [
        "//src/v/bytes",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/kafka/server",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/net",
        "//src/v/random:generators",
        "//src/v/rpc",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
        "//src/v/transform/rpc",
        "//src/v/utils:unresolved_address",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@googletest//:gtest",
        "@seastar",
    ],
)

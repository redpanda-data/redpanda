load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "transform_commit_batcher_test",
    timeout = "short",
    srcs = [
        "commit_batcher_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
        "//src/v/transform:commit_batcher",
        "//src/v/transform:logger",
        "//src/v/transform/logging:logger",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/strings",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
    ],
)

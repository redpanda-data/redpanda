load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "commit_batcher_test",
    timeout = "short",
    srcs = [
        "commit_batcher_test.cc",
    ],
    cpu = 1,
    memory = "64MiB",
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
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/strings",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "memory_limiter_test",
    timeout = "short",
    srcs = [
        "memory_limiter_test.cc",
    ],
    cpu = 1,
    memory = "64MiB",
    deps = [
        "//src/v/test_utils:gtest",
        "//src/v/transform:memory_limiter",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "transfer_queue_test",
    timeout = "short",
    srcs = [
        "transfer_queue_test.cc",
    ],
    cpu = 1,
    memory = "64MiB",
    deps = [
        "//src/v/base",
        "//src/v/test_utils:gtest",
        "//src/v/transform:logger",
        "//src/v/transform:memory_limiter",
        "//src/v/transform:transfer_queue",
        "@googletest//:gtest",
        "@seastar",
    ],
)

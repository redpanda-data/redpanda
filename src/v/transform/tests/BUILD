load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

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

redpanda_test_cc_library(
    name = "test_fixture",
    srcs = [
        "test_fixture.cc",
    ],
    hdrs = [
        "test_fixture.h",
    ],
    include_prefix = "transform/tests",
    deps = [
        "//src/v/base",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/ssx:semaphore",
        "//src/v/test_utils:gtest",
        "//src/v/transform:impl",
        "//src/v/wasm:api",
        "//src/v/wasm:transform_probe",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "transform_manager_test",
    timeout = "short",
    srcs = [
        "transform_manager_test.cc",
    ],
    cpu = 1,
    memory = "64MiB",
    deps = [
        ":test_fixture",
        "//src/v/base",
        "//src/v/model",
        "//src/v/ssx:semaphore",
        "//src/v/test_utils:gtest",
        "//src/v/transform:impl",
        "//src/v/transform:logger",
        "//src/v/utils:human",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@abseil-cpp//absl/strings",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "transform_processor_test",
    timeout = "short",
    srcs = [
        "transform_processor_test.cc",
    ],
    cpu = 1,
    memory = "64MiB",
    deps = [
        ":test_fixture",
        "//src/v/base",
        "//src/v/bytes:random",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/test_utils:gtest",
        "//src/v/transform:impl",
        "//src/v/transform:logger",
        "//src/v/utils:human",
        "@googletest//:gtest",
        "@seastar",
    ],
)

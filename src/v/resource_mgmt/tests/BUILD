load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_cc_gtest")

redpanda_cc_btest(
    name = "cpu_profiler_test",
    timeout = "short",
    srcs = [
        "cpu_profiler_test.cc",
    ],
    deps = [
        "//src/v/config",
        "//src/v/resource_mgmt:cpu_profiler",
        "//src/v/resource_mgmt:cpu_scheduling",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "available_memory_test",
    timeout = "short",
    srcs = [
        "available_memory_test.cc",
    ],
    deps = [
        "//src/v/resource_mgmt:available_memory",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "memory_sampling_tests",
    timeout = "short",
    srcs = [
        "memory_sampling_tests.cc",
    ],
    target_compatible_with = select({
        "@seastar//:use_system_allocator": ["@platforms//:incompatible"],
        "//conditions:default": [],
    }),
    deps = [
        "//src/v/config",
        "//src/v/crash_tracker",
        "//src/v/resource_mgmt:memory_sampling",
        "//src/v/storage:batch_cache",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "storage_test",
    timeout = "short",
    srcs = [
        "storage_test.cc",
    ],
    deps = [
        "//src/v/config",
        "//src/v/random:generators",
        "//src/v/resource_mgmt:storage",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "memory_groups_test",
    timeout = "short",
    srcs = [
        "memory_groups_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/resource_mgmt:memory_groups",
        "//src/v/test_utils:gtest",
        "//src/v/utils:human",
        "@googletest//:gtest",
        "@seastar//:testing",
    ],
)

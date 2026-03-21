load("//bazel:build.bzl", "redpanda_cc_library")
load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_library(
    name = "epoch_barrier_test_utils",
    hdrs = [
        "epoch_barrier_test_utils.h",
    ],
    deps = [
        "//src/v/cloud_topics:data_plane_api",
        "//src/v/cloud_topics:inflight_write_tracker",
        "//src/v/cloud_topics/level_zero/gc:epoch_barrier",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
    ],
)

redpanda_cc_gtest(
    name = "level_zero_gc_test",
    timeout = "short",
    srcs = [
        "level_zero_gc_tests.cc",
    ],
    deps = [
        "//src/v/cloud_topics:object_utils",
        "//src/v/cloud_topics/level_zero/gc:level_zero_gc",
        "//src/v/config",
        "//src/v/ssx:mutex",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "level_zero_gc_mt_test",
    timeout = "short",
    srcs = [
        "level_zero_gc_mt_test.cc",
    ],
    cpu = 3,
    deps = [
        "//src/v/cloud_topics:object_utils",
        "//src/v/cloud_topics/level_zero/gc:level_zero_gc",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "epoch_barrier_test",
    timeout = "short",
    srcs = [
        "epoch_barrier_test.cc",
    ],
    deps = [
        ":epoch_barrier_test_utils",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/gc:epoch_barrier",
        "//src/v/cluster",
        "//src/v/test_utils:async",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "epoch_barrier_mt_test",
    timeout = "short",
    srcs = [
        "epoch_barrier_mt_test.cc",
    ],
    cpu = 3,
    deps = [
        ":epoch_barrier_test_utils",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/gc:epoch_barrier",
        "//src/v/cluster",
        "//src/v/test_utils:async",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

load("//bazel:test.bzl", "redpanda_cc_gtest")

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

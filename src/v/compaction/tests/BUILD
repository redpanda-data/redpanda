load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "key_offset_map_test",
    timeout = "short",
    srcs = [
        "key_offset_map_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/compaction:key",
        "//src/v/compaction:key_offset_map",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

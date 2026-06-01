load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "simple_reducer",
    hdrs = [
        "simple_reducer.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes",
        "//src/v/compaction:filter",
        "//src/v/compaction:key",
        "//src/v/compaction:key_offset_map",
        "//src/v/compaction:reducer",
        "//src/v/compaction:utils",
        "//src/v/model",
        "@seastar",
    ],
)

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

redpanda_cc_gtest(
    name = "reducer_test",
    timeout = "short",
    srcs = [
        "reducer_test.cc",
    ],
    deps = [
        ":simple_reducer",
        "//src/v/bytes",
        "//src/v/compaction:filter",
        "//src/v/compaction:key",
        "//src/v/compaction:key_offset_map",
        "//src/v/compaction:reducer",
        "//src/v/compaction:utils",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/model",
        "//src/v/model:batch_compression",
        "//src/v/model/tests:random",
        "//src/v/reflection:adl",
        "//src/v/storage:record_batch_builder",
        "//src/v/storage/tests:batch_generators",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

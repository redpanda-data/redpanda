load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "readahead_file_reader_test",
    timeout = "short",
    srcs = [
        "readahead_file_reader_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/base",
        "//src/v/bytes:ioarray",
        "//src/v/lsm/io:persistence",
        "//src/v/lsm/io:readahead_file_reader",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "persistence_test",
    timeout = "short",
    srcs = [
        "persistence_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/base",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_io/tests:scoped_remote",
        "//src/v/cloud_storage_clients",
        "//src/v/lsm/core/internal:files",
        "//src/v/lsm/io:cloud_persistence",
        "//src/v/lsm/io:disk_persistence",
        "//src/v/lsm/io:memory_persistence",
        "//src/v/lsm/io:persistence",
        "//src/v/test_utils:gtest",
        "//src/v/utils:uuid",
        "@googletest//:gtest",
        "@seastar",
    ],
)

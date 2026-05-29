load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "object_test",
    timeout = "short",
    srcs = [
        "object_test.cc",
    ],
    deps = [
        "//src/v/bytes:iostream",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "object_utils_test",
    timeout = "short",
    srcs = [
        "object_utils_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/common:object_utils",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "file_io_test",
    timeout = "short",
    srcs = [
        "file_io_test.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/cloud_io/tests:cloud_io_cache_fixture",
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/common:file_io",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

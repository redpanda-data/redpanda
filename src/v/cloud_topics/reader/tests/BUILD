load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "placeholder_extent_fixture",
    srcs = [
        "placeholder_extent_fixture.cc",
    ],
    hdrs = [
        "mocks.h",
        "placeholder_extent_fixture.h",
    ],
    include_prefix = "cloud_topics/reader/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iostream",
        "//src/v/bytes:random",
        "//src/v/cloud_io:basic_cache_service_api",
        "//src/v/cloud_io:io_result",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_topics:placeholder",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/ssx:sformat",
        "//src/v/storage:record_batch_builder",
        "//src/v/storage:record_batch_utils",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "placeholder_extent_test",
    timeout = "short",
    srcs = [
        "placeholder_extent_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/reader:placeholder_extent",
        "//src/v/cloud_topics/reader/tests:placeholder_extent_fixture",
        "//src/v/model",
        "//src/v/ssx:sformat",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "placeholder_extent_reader_test",
    timeout = "short",
    srcs = [
        "placeholder_extent_reader_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics/reader:placeholder_extent_reader",
        "//src/v/cloud_topics/reader/tests:placeholder_extent_fixture",
        "//src/v/model",
        "//src/v/ssx:sformat",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

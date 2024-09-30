load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "write_request_test",
    timeout = "short",
    srcs = [
        "write_request_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/batcher:serializer",
        "//src/v/cloud_topics/batcher:write_request",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "serializer_test",
    timeout = "short",
    srcs = [
        "serializer_test.cc",
    ],
    deps = [
        "//src/v/bytes",
        "//src/v/cloud_topics/batcher:serializer",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "aggregator_test",
    timeout = "short",
    srcs = [
        "aggregator_test.cc",
    ],
    deps = [
        "//src/v/bytes",
        "//src/v/bytes:iostream",
        "//src/v/cloud_topics/batcher:aggregator",
        "//src/v/cloud_topics/batcher:serializer",
        "//src/v/cloud_topics/batcher:write_request",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "batcher_test",
    timeout = "short",
    srcs = [
        "batcher_test.cc",
        "remote_mock.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iostream",
        "//src/v/bytes:random",
        "//src/v/cloud_io:io_result",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_topics:placeholder",
        "//src/v/cloud_topics/batcher",
        "//src/v/cloud_topics/batcher:aggregator",
        "//src/v/cloud_topics/batcher:serializer",
        "//src/v/cloud_topics/batcher:write_request",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/storage:record_batch_builder",
        "//src/v/storage:record_batch_utils",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

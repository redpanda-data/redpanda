load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "active_segment_test",
    timeout = "short",
    srcs = [
        "active_segment_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/storage/mvlog",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "batch_collecting_stream_utils_test",
    timeout = "short",
    srcs = [
        "batch_collecting_stream_utils_test.cc",
    ],
    deps = [
        "//src/v/bytes:iostream",
        "//src/v/hashing:crc32",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/storage",
        "//src/v/storage:record_batch_utils",
        "//src/v/storage/mvlog",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "batch_collector_test",
    timeout = "short",
    srcs = [
        "batch_collector_test.cc",
    ],
    deps = [
        "//src/v/model",
        "//src/v/storage",
        "//src/v/storage/mvlog",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "entry_stream_utils_test",
    timeout = "short",
    srcs = [
        "entry_stream_utils_test.cc",
    ],
    deps = [
        "//src/v/bytes:iostream",
        "//src/v/random:generators",
        "//src/v/storage/mvlog",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random_bytes",
        "@googletest//:gtest",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "entry_stream_utils_death_test",
    timeout = "short",
    srcs = [
        "entry_stream_utils_death_test.cc",
    ],
    deps = [
        "//src/v/bytes:iostream",
        "//src/v/storage/mvlog",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "file_test",
    timeout = "short",
    srcs = [
        "file_test.cc",
    ],
    deps = [
        "//src/v/container:chunked_vector",
        "//src/v/storage/mvlog",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random_bytes",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "segment_appender_test",
    timeout = "short",
    srcs = [
        "segment_appender_test.cc",
    ],
    deps = [
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/storage:record_batch_utils",
        "//src/v/storage/mvlog",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "segment_io_test",
    timeout = "short",
    srcs = [
        "segment_io_test.cc",
    ],
    deps = [
        "//src/v/container:chunked_circular_buffer",
        "//src/v/model/tests:random",
        "//src/v/storage/mvlog",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "segment_reader_test",
    timeout = "short",
    srcs = [
        "segment_reader_test.cc",
    ],
    deps = [
        "//src/v/random:generators",
        "//src/v/storage/mvlog",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "skipping_data_source_test",
    timeout = "short",
    srcs = [
        "skipping_data_source_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iostream",
        "//src/v/random:generators",
        "//src/v/storage/mvlog",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random_bytes",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

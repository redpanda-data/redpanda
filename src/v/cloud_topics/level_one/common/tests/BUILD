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
    name = "single_flight_test",
    timeout = "short",
    srcs = [
        "single_flight_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/common:single_flight",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "file_io_stream_test",
    timeout = "short",
    srcs = [
        "file_io_stream_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/cloud_io:basic_cache_service_api",
        "//src/v/cloud_io:cache",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_io:remote_api",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_io/tests:scoped_remote",
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_topics/level_one/common:file_io",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/common:object_utils",
        "//src/v/config",
        "//src/v/storage:disk",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "//src/v/test_utils:tmp_dir",
        "//src/v/utils:retry_chain_node",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "chunk_data_source_test",
    timeout = "short",
    srcs = [
        "chunk_data_source_test.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/common:chunk_data_source",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "open_object_test",
    timeout = "short",
    srcs = [
        "open_object_test.cc",
    ],
    deps = [
        "//src/v/bytes:iostream",
        "//src/v/cloud_io:admission_control_types",
        "//src/v/cloud_topics/level_one/common:fake_io",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/cloud_topics/level_one/common:object_handle",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/common:open_object",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/storage:record_batch_utils",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "@abseil-cpp//absl/container:btree",
        "@googletest//:gtest",
        "@seastar",
    ],
)

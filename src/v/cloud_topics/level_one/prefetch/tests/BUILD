load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "chunk_reassembler_test",
    timeout = "short",
    srcs = [
        "chunk_reassembler_test.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/cloud_topics/level_one/prefetch:chunk_reassembler",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "l1_memory_broker_test",
    timeout = "short",
    srcs = [
        "l1_memory_broker_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/prefetch:l1_memory_broker",
        "//src/v/config",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "prefetch_pacer_test",
    timeout = "short",
    srcs = [
        "prefetch_pacer_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/prefetch:prefetch_pacer",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "chunk_downloader_test",
    timeout = "short",
    srcs = [
        "chunk_downloader_test.cc",
    ],
    deps = [
        "//src/v/bytes:iostream",
        "//src/v/cloud_io:admission_control_types",
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/common:fake_io",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/prefetch:chunk_downloader",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "fetch_stream_test",
    timeout = "short",
    srcs = [
        "fetch_stream_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iostream",
        "//src/v/cloud_io:admission_control_types",
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_topics/batch_cache",
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/common:fake_io",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/cloud_topics/level_one/metastore:simple",
        "//src/v/cloud_topics/level_one/prefetch:chunk_downloader",
        "//src/v/cloud_topics/level_one/prefetch:fetch_stream",
        "//src/v/cloud_topics/level_one/prefetch:prefetch_pacer",
        "//src/v/config",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/container:chunked_vector",
        "//src/v/features",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/storage",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:test_env",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "l1_fetch_service_test",
    timeout = "short",
    srcs = [
        "l1_fetch_service_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iostream",
        "//src/v/cloud_io:admission_control_types",
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_topics:log_reader_config",
        "//src/v/cloud_topics/batch_cache",
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/common:fake_io",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/cloud_topics/level_one/metastore:simple",
        "//src/v/cloud_topics/level_one/prefetch:l1_fetch_service",
        "//src/v/cloud_topics/level_one/prefetch:prefetch_pacer",
        "//src/v/config",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/features",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/storage",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:test_env",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "memory_first_reader_test",
    timeout = "short",
    srcs = [
        "memory_first_reader_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iostream",
        "//src/v/cloud_io:admission_control_types",
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_topics:log_reader_config",
        "//src/v/cloud_topics/batch_cache",
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/common:fake_io",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/cloud_topics/level_one/metastore:simple",
        "//src/v/cloud_topics/level_one/prefetch:fetch_stream",
        "//src/v/cloud_topics/level_one/prefetch:memory_first_reader",
        "//src/v/cloud_topics/level_one/prefetch:prefetch_pacer",
        "//src/v/config",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/features",
        "//src/v/model",
        "//src/v/model:batch_utils",
        "//src/v/model/tests:random",
        "//src/v/storage",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:test_env",
        "@googletest//:gtest",
        "@seastar",
    ],
)

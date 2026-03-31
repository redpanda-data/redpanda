load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "in_memory_sink",
    srcs = [
        "in_memory_sink.cc",
    ],
    hdrs = [
        "in_memory_sink.h",
    ],
    deps = [
        "//src/v/bytes",
        "//src/v/bytes:iostream",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/compaction:reducer",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "reducer_test",
    timeout = "short",
    srcs = [
        "reducer_test.cc",
    ],
    deps = [
        ":in_memory_sink",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/bytes:iostream",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/common:object_utils",
        "//src/v/cloud_topics/level_one/frontend_reader/tests:l1_reader_fixture",
        "//src/v/cloud_topics/level_one/maintenance:meta",
        "//src/v/cloud_topics/level_one/maintenance:worker_probe",
        "//src/v/cloud_topics/level_one/maintenance/compaction:compaction_filter",
        "//src/v/cloud_topics/level_one/maintenance/compaction:compaction_source_and_sink",
        "//src/v/cloud_topics/level_one/maintenance/tests:throwing_compaction_sink",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/cloud_topics/level_one/metastore:simple",
        "//src/v/compaction:key_offset_map",
        "//src/v/compaction:reducer",
        "//src/v/compaction/tests:simple_reducer",
        "//src/v/config",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/container:chunked_vector",
        "//src/v/kafka/server/tests:kafka_test_utils",
        "//src/v/model",
        "//src/v/model:batch_compression",
        "//src/v/model/tests:random",
        "//src/v/ssx:when_all",
        "//src/v/storage/tests:batch_generators",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

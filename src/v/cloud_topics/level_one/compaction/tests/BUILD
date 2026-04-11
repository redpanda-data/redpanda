load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "throwing_compaction_sink",
    hdrs = [
        "throwing_compaction_sink.h",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/compaction:source_and_sink",
        "//src/v/compaction:reducer",
        "//src/v/model",
        "@seastar",
    ],
)

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
        "//src/v/model:batch_compression",
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
        ":throwing_compaction_sink",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/bytes:iostream",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/common:object_utils",
        "//src/v/cloud_topics/level_one/compaction:filter",
        "//src/v/cloud_topics/level_one/compaction:meta",
        "//src/v/cloud_topics/level_one/compaction:source_and_sink",
        "//src/v/cloud_topics/level_one/compaction:worker_probe",
        "//src/v/cloud_topics/level_one/frontend_reader/tests:l1_reader_fixture",
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

redpanda_cc_gtest(
    name = "worker_manager_test",
    timeout = "short",
    srcs = [
        "worker_manager_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/compaction:meta",
        "//src/v/cloud_topics/level_one/compaction:scheduler_probe",
        "//src/v/cloud_topics/level_one/compaction:worker",
        "//src/v/model",
        "//src/v/ssx:semaphore",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "log_info_collector_test",
    timeout = "short",
    srcs = [
        "log_info_collector_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/compaction:log_info_collector",
        "//src/v/cloud_topics/level_one/compaction:meta",
        "//src/v/cloud_topics/level_one/frontend_reader/tests:l1_reader_fixture",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "log_collector_test",
    timeout = "short",
    srcs = [
        "log_collector_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/compaction:log_collector",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "scheduler_fixture",
    hdrs = [
        "scheduler_fixture.h",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/compaction:log_info_collector",
        "//src/v/cloud_topics/level_one/compaction:scheduler",
        "//src/v/cloud_topics/level_one/compaction:scheduling_policies",
        "//src/v/cloud_topics/level_one/compaction:worker",
        "//src/v/cloud_topics/level_one/frontend_reader/tests:l1_reader_fixture",
        "//src/v/cluster",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/storage/tests:batch_generators",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "scheduler_test",
    timeout = "short",
    srcs = [
        "scheduler_test.cc",
    ],
    cpu = 1,
    deps = [
        ":scheduler_fixture",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/storage/tests:batch_generators",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "scheduler_multithread_test",
    timeout = "short",
    srcs = [
        "scheduler_multithread_test.cc",
    ],
    cpu = 8,
    deps = [
        ":scheduler_fixture",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "compaction_e2e_test",
    timeout = "short",
    srcs = [
        "compaction_e2e_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_topics/level_zero/stm:ctp_stm",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/redpanda/tests:fixture",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

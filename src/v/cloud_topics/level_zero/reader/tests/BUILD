load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "materialized_extent_fixture",
    srcs = [
        "materialized_extent_fixture.cc",
    ],
    hdrs = [
        "materialized_extent_fixture.h",
        "mocks.h",
    ],
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
        "//src/v/cloud_topics:object_utils",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/cloud_topics/level_zero/reader:materialized_extent",
        "//src/v/cloud_topics/level_zero/stm:placeholder",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/ssx:sformat",
        "//src/v/storage:record_batch_builder",
        "//src/v/storage:record_batch_utils",
        "//src/v/test_utils:gtest",
        "//src/v/utils:retry_chain_node",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "materialized_extent_test",
    timeout = "short",
    srcs = [
        "materialized_extent_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/cloud_topics/level_zero/common:micro_probe",
        "//src/v/cloud_topics/level_zero/reader:materialized_extent",
        "//src/v/cloud_topics/level_zero/reader/tests:materialized_extent_fixture",
        "//src/v/cloud_topics/level_zero/stm:placeholder",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/ssx:sformat",
        "//src/v/test_utils:gtest",
        "//src/v/utils:retry_chain_node",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "materialized_extent_reader_test",
    timeout = "short",
    srcs = [
        "materialized_extent_reader_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/reader:materialized_extent_reader",
        "//src/v/cloud_topics/level_zero/reader/tests:materialized_extent_fixture",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/ssx:sformat",
        "//src/v/test_utils:gtest",
        "//src/v/utils:retry_chain_node",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "fetch_handler_test",
    timeout = "short",
    srcs = [
        "fetch_handler_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/pipeline:read_pipeline",
        "//src/v/cloud_topics/level_zero/reader:fetch_handler",
        "//src/v/cloud_topics/level_zero/reader/tests:materialized_extent_fixture",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/ssx:sformat",
        "//src/v/test_utils:gtest",
        "//src/v/utils:retry_chain_node",
        "@googletest//:gtest",
        "@seastar",
    ],
)
